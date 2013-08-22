require 'rubygems'
require 'bud'

class CausalDict
  include Bud

  state do
    sealed :node, [:addr]
    table :log, [:id] => [:key, :val, :deps]
    channel :chn, [:@addr, :id] => [:key, :val, :deps]

    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, [:id]
    table :safe_log, log.schema
    scratch :dominated, log.schema
    scratch :view, log.schema
  end

  bloom :replication do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads
  end

  bloom :check_deps do
    flat_dep <= log.flat_map do |l|
      l.deps.map {|d| [l.id, d]}
    end

    missing_dep <= flat_dep.notin(safe_log, :dep => :id).pro {|d| [d.id]}
    safe_log <+ log.notin(missing_dep, :id => :id)
  end

  bloom :active_view do
    dominated <= (safe_log * safe_log).pairs(:key => :key) do |w1,w2|
      w2 if w1 != w2 and w1.deps.include? w2.id
    end
    view <= safe_log.notin(dominated)
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist = ports.map {|p| CausalDict.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.run_bg
end

first = rlist.first
first.sync_do {
  first.log <+ [[[first.port, 1], 'foo', 'bar', []]]
}

last = rlist.last
last.sync_do {
  last.log <+ [[[last.port, 1], 'baz', 'qux', [[first.port, 1]]]]
}

last.sync_do {
  last.log <+ [[[last.port, 2], 'baz', 'kkk', []]]
}

sleep 1

first.sync_do {
  puts first.view.map {|v| "#{v.key} => #{v.val}"}.sort.inspect
}

rlist.each(&:stop)
