require 'rubygems'
require 'bud'

class BroadcastCausal
  include Bud

  state do
    sealed :node, [:addr]
    table :log, [:id] => [:val, :deps]
    channel :chn, [:@addr, :id] => [:val, :deps]

    table :safe_log, log.schema
    table :done, [:id]

    scratch :in_progress, log.schema
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema
  end

  bloom do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads

    in_progress <= log.notin(done, :id => :id)
    flat_dep <= in_progress.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(done, :dep => :id)
    safe_log <+ in_progress.notin(missing_dep, :id => :id)
    done <= safe_log {|l| [l.id]}
  end
end

opts = {}
ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist = ports.map {|p| BroadcastCausal.new(opts.merge(:port => p))}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

first = rlist.first
first.log <+ [[[first.port, 1], 'foo',
               [[first.port, 2], [first.port, 10]]],
              [[first.port, 2], 'bar', []],
              [[first.port, 3], 'baz', [first.port, 2]]]
first.tick

30.times { rlist.each(&:tick); sleep(0.1) }

puts "Safe log: #{first.safe_log.to_a.sort}"
puts "# of log: #{first.log.to_a.size}"

rlist.each(&:stop)
