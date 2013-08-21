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
rlist = ports.map {|p| CausalDict.new(addrs, :ip => "localhost", :port => :p)}

d = CausalDict.new
