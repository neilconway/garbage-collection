require 'rubygems'
require 'bud'

class BroadcastEpochAll
  include Bud

  state do
    table :node, [:addr, :epoch]
    table :log, [:id] => [:epoch, :val]
    channel :chn, [:@addr, :id] => [:epoch, :val]
  end

  bloom do
    chn <~ (node * log).pairs(:epoch => :epoch) {|n,l| [n.addr] + l}
    log <= chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => false, :range_stats => true, :print_rules => true }
rlist = Array.new(3) { BroadcastEpochAll.new(opts) }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

rlist.each do |r|
  r.sync_do {
    r.node <+ r_addrs.map {|a| [a, "first"]}
  }
end

first = rlist.first
first.sync_do {
  first.log <+ [[first.id(1), "first", "foo"],
                [first.id(2), "first", "bar"]]
}

sleep 3

first.sync_do {
  first.seal_node_epoch <+ [["first"]]
}

3.times { rlist.each(&:tick) }

rlist.each do |r|
  r.sync_do {
    puts "log @ #{r.port}: #{r.log.to_a.size}"
  }
end

rlist.each(&:stop)
