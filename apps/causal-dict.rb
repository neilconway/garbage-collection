require 'rubygems'
require 'bud'

# Places where we would like to apply RCE:
#   (1) read_req => read_chn
#   (2) (node * log) => chn
#   (3) read_resp => resp_chn
#
# Places where we would like to apply RSE:
#   (1) read_req.notin(read_chn_approx) (post-RCE)
#   (2) (node * log).notin(chn_approx) (post-RCE)
#   (3) read_resp.notin(resp_chn_approx) (post-RCE)
#   (4) safe_log.notin(dominated)
#   (5) read_buf.notin(read_resp)
#
# Places where we would like to apply notin compression:
#   (1) read_chn_approx (post-RCE)
#   (2) chn_approx (post-RCE)
#   (3) resp_chn_approx (post-RCE)
#   (4) read_resp
#       => This is a little tricky: the intuition is that once a response has
#          been generated and sent to the client, we only need to keep around
#          enough information so that the negation against read_buf can be
#          performed. So this is like a combination of normal notin compression
#          and RSE.
#
# It would make sense to split the client code into a separate class and move
# the read channel state into a shared module. However, the current analysis is
# per-class, so this would prevent doing RCE/RSE on the read protocol.
class CausalDict
  include Bud

  state do
    # Replication state
    sealed :node, [:addr]
    table :log, [:id] => [:key, :val, :deps]
    channel :chn, [:@addr, :id] => [:key, :val, :deps]

    # Per-replica state for tracking causal dependencies, pending write
    # operations, and the current KVS view
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema
    table :safe_log, log.schema
    scratch :dominated, log.schema
    scratch :view, log.schema

    # State for handling read requests
    channel :req_chn, [:@addr, :id] => [:key, :deps]
    channel :resp_chn, [:@addr, :id, :key, :val]

    # Client-side read state
    table :read_req, req_chn.schema
    table :read_result, resp_chn.schema

    # Server-side read state
    table :read_buf, [:id] => [:key, :deps, :src_addr]
    scratch :read_pending, read_buf.schema
    scratch :read_dep, [:id, :dep]
    scratch :missing_read_dep, read_dep.schema
    scratch :safe_read, read_buf.schema
    table :read_resp, resp_chn.schema
  end

  bloom :replication do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads
  end

  bloom :check_deps do
    # Compute "safe" log entries; a log entry is safe if all of its dependencies
    # are safe. (This has a cycle through negation but it is harmless: as new
    # entries are moved into safe_log, missing_dep might shrink, which would
    # cause safe_log to increase, and so on.)
    #
    # When can we discard entries from the log? Intuitively, once a log entry
    # has been delivered to all nodes and its dependencies have been satisfied,
    # it isn't useful any more and can be reclaimed. That is, entries in "log"
    # are just buffered termporarily, until their dependencies have been
    # satisfied; once that has happened, they are no longer useful and can be
    # discarded.
    flat_dep <= log.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(safe_log, :dep => :id)
    safe_log <+ log.notin(missing_dep, :id => :id)
  end

  bloom :active_view do
    dominated <= (safe_log * safe_log).pairs(:key => :key) do |w1,w2|
      w2 if w1 != w2 and w1.deps.include? w2.id
    end
    view <= safe_log.notin(dominated)
  end

  bloom :read_server do
    # XXX: there should be a cleaner way to write this. We'd like to say that
    # "read_pending is the delta between read_resp and read_buf".
    read_buf <= req_chn {|r| [r.id, r.key, r.deps, r.source_addr]}
    read_pending <= read_buf.notin(read_resp, :id => :id)
    read_dep <= read_pending.flat_map {|r| r.deps.map {|d| [r.id, d]}}
    missing_read_dep <= read_dep.notin(safe_log, :dep => :id)
    safe_read <= read_pending.notin(missing_read_dep, :id => :id)
    read_resp <+ (safe_read * view).pairs(:key => :key) {|r,v| [r.src_addr, r.id, r.key, v.val]}
    resp_chn <~ read_resp
  end

  bloom :read_client do
    req_chn <~ read_req
    read_result <= resp_chn
  end

  def print_view
    puts "View @ #{port}:"
    puts view.map {|v| "\t#{v.key} => #{v.val}"}.sort.join("\n")
  end
end

opts = { :channel_stats => false }
ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| CausalDict.new(opts.merge(:ip => "localhost", :port => p))}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

# Writes:
#   (W1) foo -> bar, deps={}
#   (W2) baz -> qux, {W1}
#   (W3) baz -> kkk, {}
#   (W4) baz -> kkk2, {W5}
#
# Reads:
#   (1) foo, {W1}
first = rlist.first
first.log <+ [[[first.port, 1], 'foo', 'bar', []]]

last = rlist.last
last.log <+ [[[last.port, 1], 'baz', 'qux', [[first.port, 1]]]]
last.log <+ [[[last.port, 2], 'baz', 'kkk', []],
             [[last.port, 3], 'baz', 'kkk2', [[first.port, 2]]]]

c = CausalDict.new(opts)
c.tick
c.read_req <+ [[last.ip_port, [c.port, 1], 'foo', [[first.port, 1]]]]
c.tick

15.times { rlist.each(&:tick); sleep 0.1; c.tick }

first.print_view
last.print_view
puts "READ RESULT @ client: #{c.read_result.map {|r| "#{r.key} => #{r.val}"}.inspect}"

puts "# of stored requests @ client: #{c.read_req.to_a.size}"
puts "# of stored responses @ client: #{c.read_result.to_a.size}"
puts "# of buffered read requests @ server: #{last.read_buf.to_a.size}"
puts "# of stored read responses @ server: #{last.read_resp.to_a.size}"
puts "# of log entries @ server: #{last.log.to_a.size}"
puts "# of safe log entries @ server: #{last.safe_log.to_a.size}"

c.stop
rlist.each(&:stop)
