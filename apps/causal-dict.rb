require 'rubygems'
require 'bud'

# It would make sense to split the client code into a separate class and move
# the read channel state into a shared module. However, the current analysis is
# per-class, so this would prevent doing RCE/RSE on the read protocol.
class CausalDict
  include Bud

  state do
    # Replication state
    sealed :node, [:addr]
    channel :chn, [:@addr, :id] => [:key, :val, :deps]
    table :log, [:id] => [:key, :val, :deps]

    # State for tracking causal dependencies and pending write operations
    table :safe_log, log.schema
    table :safe, [:id]
    scratch :pending, log.schema
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema

    # State for computing the current KVS view
    table :dominated, [:id]
    scratch :dep, [:from, :to]
    scratch :dep_tc, [:from, :to]
    scratch :conflict_w, [:w1, :w2]
    scratch :view, log.schema

    # Protocol for read request/response
    channel :req_chn, [:@addr, :id] => [:key, :deps]
    channel :resp_chn, [:@addr, :id, :key, :val]

    # Server-side read state
    table :read_buf, [:id] => [:key, :deps, :src_addr]
    scratch :read_pending, read_buf.schema
    scratch :read_dep, [:id, :dep]
    scratch :missing_read_dep, read_dep.schema
    scratch :safe_read, read_buf.schema
    table :read_resp, resp_chn.schema
    table :done_read, [:id]

    # Client-side read state
    table :read_req, req_chn.schema
    table :read_result, resp_chn.schema
  end

  bloom :replication do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads
  end

  bloom :check_deps do
    # Compute "safe" log entries; a log entry is safe if all of its dependencies
    # are safe. (This has a cycle through negation but we use <+ to temporally
    # stratify it. If we added support for constraint stratification, we could
    # probably use the fact that dependencies are a partial order to constraint
    # stratify.)
    #
    # When can we discard entries from the log? Intuitively, once a log entry
    # has been delivered to all nodes and its dependencies have been satisfied,
    # it isn't useful any more and can be reclaimed. That is, entries in "log"
    # are just buffered termporarily, until their dependencies have been
    # satisfied; once that has happened, they are no longer useful and can be
    # discarded.
    pending <= log.notin(safe, :id => :id)
    flat_dep <= pending.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(safe, :dep => :id)
    safe_log <+ pending.notin(missing_dep, :id => :id)
    safe <= safe_log {|l| [l.id]}
  end

  bloom :active_view do
    # A safe_log entry e for key k is dominated if there is another safe_log
    # entry e' for k s.t. e happens-before e'. We only need to retain
    # non-dominated versions in the view. To compute the happens-before
    # relation, we take the transitive closure of the dependency graph.
    dep <= safe_log.flat_map {|l| l.deps.map {|d| [d, l.id]}}
    dep_tc <= dep
    dep_tc <= (dep * dep_tc).pairs(:to => :from) {|d,t| [d.from, t.to]}

    conflict_w <= (safe_log * safe_log).pairs(:key => :key) do |w1,w2|
      [w1.id, w2.id] if w1 != w2
    end
    dominated <= (conflict_w * dep_tc).lefts(:w1 => :from) {|c| [c.w1]}
    view <= safe_log.notin(dominated, :id => :id)
  end

  bloom :read_server do
    # XXX: there should be a cleaner way to write this. We'd like to say that
    # "read_pending is the delta between read_resp and read_buf".
    read_buf <= req_chn {|r| [r.id, r.key, r.deps, r.source_addr]}
    read_pending <= read_buf.notin(done_read, :id => :id)
    read_dep <= read_pending.flat_map {|r| r.deps.map {|d| [r.id, d]}}
    missing_read_dep <= read_dep.notin(safe, :dep => :id)
    safe_read <= read_pending.notin(missing_read_dep, :id => :id)
    read_resp <= (safe_read * view).pairs(:key => :key) {|r,v| [r.src_addr, r.id, r.key, v.val]}
    done_read <+ read_resp {|r| [r.id]}
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

  def id(i)
    [port, i]
  end
end

opts = { :channel_stats => false, :disable_rse => false }
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
#   (W4) baz -> kkk2, {W3}
#   (W5) baz -> kkk3, {W2,W4}
#   (W6) qux -> xxx, {W5}
#   (W7) baz -> kkk4, {W6}
#
# The final view should contain W1, W6, and W7. Note that W7 causally follows
# W5, even though W5 is not one of W7's (direct) dependencies.
#
# Reads:
#   (1) foo, {W1}
first = rlist.first
first.log <+ [[first.id(1), 'foo', 'bar', []]]

last = rlist.last
last.log <+ [[last.id(2), 'baz', 'qux', [first.id(1)]]]
last.log <+ [[last.id(3), 'baz', 'kkk', []],
             [last.id(4), 'baz', 'kkk2', [last.id(3)]],
             [last.id(5), 'baz', 'kkk3', [last.id(2), last.id(4)]],
             [last.id(6), 'qux', 'xxx', [last.id(5)]],
             [last.id(7), 'baz', 'kkk4', [last.id(6)]]]

c = CausalDict.new(opts)
c.tick
c.read_req <+ [[last.ip_port, c.id(1), 'foo', [first.id(1)]]]
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
