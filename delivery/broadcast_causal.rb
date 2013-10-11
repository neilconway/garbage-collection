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

  def id(i)
    "#{port}:#{i}"
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

puts "Safe log: #{first.safe_log.to_a.sort}"
puts "# of log: #{first.log.to_a.size}"

rlist.each(&:stop)
