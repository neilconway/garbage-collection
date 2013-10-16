require 'rubygems'
require 'bud'

class BroadcastCausal
  include Bud

  state do
    sealed :node, [:addr]
    table :log, [:id] => [:val, :deps]
    channel :chn, [:@addr, :id] => [:val, :deps]

    table :safe_log, log.schema

    scratch :pending, log.schema
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema
  end

  bloom do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads

    # Note that we can safely reclaim from log once a matching entry appears in
    # safe_log. This is because (a) safe_log grows over time and is never
    # deleted from or GC'd (b) all paths downstream of log terminate in
    # safe_log. If log reached an output interface, we couldn't GC it; whereas
    # if safe_log was GC'd, we'd need to split safe_log into a set of IDs (range
    # compressed but not GC'd) and safe_log itself (GC'd), and then reference
    # the safe-ID collection in the rules below.
    pending <= log.notin(safe_log, :id => :id)
    flat_dep <= pending.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(safe_log, :dep => :id)
    safe_log <+ pending.notin(missing_dep, :id => :id)
  end
end

opts = {}
ports = (1..3).map {|i| i + 10001}
rlist = ports.map {|p| BroadcastCausal.new(opts.merge(:port => p))}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

first = rlist.first
first.log <+ [[first.id(1), 'foo',
               [first.id(2), first.id(10)]],
              [first.id(2), 'bar', []],
              [first.id(3), 'baz', [first.id(2)]]]
first.tick

10.times { rlist.each(&:tick); sleep(0.1) }

puts "# of safe_log: #{first.safe_log.length}"
puts "# of log: #{first.log.length}"

rlist.each(&:stop)
