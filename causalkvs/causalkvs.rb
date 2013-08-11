require 'rubygems'
require 'bud'

module ExternalKVS
  state do
    interface input, :put, [:key, :value]
    interface input, :get, [:key]
    interface output, :put_response, [:key, :version]
    interface output, :get_response, [:key, :value, :version]
  end
end

module InternalKVS
  include ExternalKVS
  state do
    # :version is set only when replicated; otherwise -1
    interface input, :put_internal, [:key, :value, :deps, :version]
    interface input, :get_internal, [:key, :deps]

    table :put_log, [:key, :value, :version, :deps]
    # using 'wholedep' as a partial key...
    table :get_log, [:key, :wholedep, :depkey, :depvers]
  
    scratch :this_version, [:key, :version]
    scratch :new_entry, put_log.schema
    scratch :all_entries, this_version.schema
    scratch :best_entry, this_version.schema
    scratch :plsiz, [:cnt]
    scratch :outstanding_deps, [:key, :wholedep, :cnt]
    scratch :satisfied_deps, outstanding_deps.schema
    scratch :sd1, get_log.schema
    scratch :yay, outstanding_deps.schema
    scratch :maxvers, put_log.schema
  end

  bootstrap do
    put_log << ["DUMMY", "DUMMY", 1, {}]
  end

  def get_version(version, cnt, id)
    if version == -1
      ((cnt + 1) << 8) + id
    else
      version
    end
  end

  bloom :puts do
    plsiz <= put_log.group([], count)
    new_entry <= (put_internal * plsiz * my_id).combos{|p, b, i| [p.key, p.value, get_version(p.version, b.cnt, i.id), p.deps]}
    # not well-reasoned, but join with put_internal to ensure we only deliver a response to the originator
    # and not a local replica...
    put_response <+ (new_entry * put_internal).pairs(:key => :key, :value => :value) do |n, i| 
      [n.key, n.version] if i.version == -1
    end
    localtick <~ put_internal{|p| [p.key]}
    put_log <+ new_entry
  end

  bloom :gets do
    localtick <~ yay{|p| [p.key]}
    get_log <= get_internal.flat_map do |g|
      g.deps.map do |k|
        [g.key, g.deps, k.first, k.last]
      end
    end

    outstanding_deps <= get_log.group([:key, :wholedep], count(:key))
    sd1 <= (put_log * get_log).pairs(:key => :depkey){|p,g| g if p.version >= g.depvers}
    satisfied_deps <= sd1.group([:key, :wholedep], count(:depkey))
    yay <= (outstanding_deps * satisfied_deps).lefts(:key => :key, :wholedep => :wholedep, :cnt => :cnt)
    # lots of recomputation, push down later
    maxvers <= put_log.argagg(:max, [:key], :version)
    get_response <+ (yay * maxvers).rights(:key => :key){|p| [p.key, p.value, p.version]}
  end

  bloom :debug do
    #stdio <~ put_internal{|p| ["(#{ip_port}@#{budtime}) PUT INTERNAL: #{p}"]}
    #stdio <~ new_entry{|n| ["NEWE: #{n}"]}
    #stdio <~ put_log{|p| ["PUTLOG: #{p}"]}
  end
end

module ClientSide
  include ExternalKVS
  state do
    table :context, [:key, :version]
    scratch :cxt_summary, [:deps]
  end
  bootstrap do
    context << ["DUMMY", 1]
  end
  
  bloom do
    context <= put_response
    context <= get_response{|g| [g.key, g.version]}
    cxt_summary <= context.group([], accum_pair(:key, :version)) 

    put_internal <= (put * cxt_summary).pairs do |p, c|
      [p.key, p.value, c.deps, -1]
    end
    get_internal <= (get * cxt_summary).pairs{|g, c| [g.key, c.deps]}
  end
end

module Rep
  # not finished...
  state do
    channel :put_channel, [:@tgt, :src, :key, :value, :deps, :version]
    channel :get_channel, [:@tgt, :src, :key, :deps]

    table :replicas, [:id] => [:node]
    scratch :my_id, [] => [:id]
    scratch :members, [:cnt]
    scratch :pc1, put_channel.schema

    channel :ack, [:@src, :tgt, :key, :version]
    table :acks, [:tgt, :key, :version]
  
    scratch :ackcnt, [:key, :version, :cnt]
    scratch :stable, [:key, :version]
    scratch :rep_candidate, put_log.schema
  end

  bloom :membership do
    my_id <= replicas{|r| [r.id] if r.node == ip_port}
    members <= replicas.group([], count)
  end

  bloom :network do
    rep_candidate <= put_log.notin(stable, :key => :key, :version => :version)
    put_channel <~ (rep_candidate * replicas).pairs{|p, n| [n.node, ip_port, p.key, p.value, p.deps, p.version] unless n.node == ip_port}
    get_channel <~ (get_internal * replicas).pairs{|g, n| [n.node, ip_port] + g.to_a}
    pc1 <= put_channel.notin(put_log, :key => :key, :version => :version)
    put_internal <= pc1{|p| [p.key, p.value, p.deps, p.version]}

    ack <~ put_channel{|p| [p.src, p.tgt, p.key, p.version]}
    acks <= ack{|a| [a.tgt, a.key, a.version]}

    ackcnt <= acks.group([:key, :version], count)
    stable <= (ackcnt * members).pairs do |a, m|
      if a.cnt == m.cnt
        [a.key, a.version]
      end
    end
    #stdio <~ stable{|s| ["STABLE: #{s}"]}
  end
end


