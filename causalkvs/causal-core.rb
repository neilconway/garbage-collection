require 'rubygems'
require 'bud'

module CausalCore
  state do
    # core collections
    # assumption: reqids are globally unique and externally generated.  
    # no ordering or other semantic assumptions.  Hence we can represent dependencies
    # as key, reqid pairs (but can't use <= for dominance)
    table :put_log, [:reqid, :key, :value, :deps]
    scratch :active_puts, put_log.schema

    # helper collections
    scratch :flat_deps, [:reqid, :key, :depkey, :depid]
    scratch :put_deps, [:reqid, :cnt]
    scratch :active_cnts, [:reqid, :cnt]
    table :satisfied, [:reqid]
    
    table :dominated, [:reqid]
    scratch :extent, [:key, :reqid, :siz]
    scratch :possible_dominance, [:key, :r1, :r2, :r2siz]
    scratch :intersections, [:r1, :r2, :dk, :di]
    scratch :icnt, [:r1, :r2, :cnt]
  end

  bloom do
    flat_deps <= put_log.flat_map do |p|
      p.deps.map do |d|
        [p.reqid, p.key, d[0], d[1]]
      end
    end

    # satisfied but not dominated.  both sets of reqids. 
    active_puts <= (put_log * satisfied).lefts(:reqid => :reqid).notin(dominated, :reqid => :reqid)
    stdio <~ active_puts{|p| ["ACTIVE(#{budtime}): #{p}"]}
  end

  bloom :satisfied do 
    satisfied <= put_log{|p| [p.reqid] if p.deps == {}}
    put_deps <= flat_deps.group([:reqid], count)
    active_cnts <= (flat_deps * satisfied).lefts(:depid => :reqid).group([:reqid], count)
    satisfied <+ (put_deps * active_cnts).lefts(:reqid => :reqid, :cnt => :cnt){|p| [p.reqid]}
  
    stdio <~ satisfied{|s| ["#{budtime} SAT: #{s}"]}
  end

  bloom :dominated do
    # dominance is dependency-containment.
    extent <= (flat_deps * satisfied).lefts(:reqid => :reqid).group([:key, :reqid], count(:depkey))
    dominated <= (extent * put_log).pairs do |e, p|
      if e.key == p.key and e.reqid != p.reqid and p.deps == {}
        [p.reqid]
      end
    end

    possible_dominance <= (extent * extent).pairs do |e1, e2|
      if e1.key == e2.key and e1.reqid != e2.reqid and e1.siz > e2.siz
        [e1.key, e1.reqid, e2.reqid, e2.siz]
      end
    end
    
    intersections <= (flat_deps * flat_deps).pairs do |d1, d2|
      if d1.key == d2.key and d1.reqid != d2.reqid and d1.depkey == d2.depkey and d1.depid == d2.depid
        [d1.reqid, d2.reqid, d1.depkey, d1.depid]
      end
    end
    icnt <= intersections.group([:r1, :r2], count)
    dominated <= (possible_dominance * icnt).lefts(:r1 => :r1, :r2 => :r2, :r2siz => :cnt){|p| [p.r2]}
  end
end


class CC
  include Bud
  include CausalCore
end


c = CC.new

c.put_log <+ [[1, "foo", "bar", {}]]
c.put_log <+ [[2, "foo", "baz", {"foo" => 1}]]
c.put_log <+ [[3, "foo", "qux", {"foo" => 1, "bar" => 4}]]



c.tick;c.tick

c.put_log <+ [[4, "bar", "eek", {}]]

c.tick;c.tick


c.put_log.each{|p| puts "PL: #{p}"}



c.put_log <+ [[5, "foo", "qux", {"foo" => 1, "bar" => 4, "fig" => 7}]]
c.tick
