require 'rubygems'
require 'bud'

class PartitionedBroadcast
  include Bud

  state do
    sealed :node, [:addr] => [:part]
    channel :chn, [:@addr, :id] => [:val, :part]
    table :log, chn.payload_schema
  end

  bloom do
    chn <~ (node * log).pairs(:part => :part) {|n,l| [n.addr] + l}
    log <= chn.payloads

    stdio <~ chn {|c| ["Got msg @ #{port}: #{c.inspect}"]}
  end

  def do_insert(id, key)
    log <+ [[id, key, get_part(id)]]
  end
end

def get_part(k)
  k % 2
end

ports = (1..6).map {|i| i + 10000}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist = ports.map {|p| PartitionedBroadcast.new(:port => p)}
rlist.each do |r|
  r.node <+ addrs.each_with_index.map {|a,p| [a, get_part(p)]}
  r.tick
end

rlist.each_with_index do |r,i|
  r.do_insert(i, "foo#{i}")
  r.tick
end

10.times { rlist.each(&:tick); sleep 0.1 }

rlist.each do |r|
  puts "State @ #{r.port}: #{r.log.to_a.sort}"
end

rlist.each(&:stop)
