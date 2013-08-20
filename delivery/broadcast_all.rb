require 'rubygems'
require 'bud'

# Reliable broadcast with a fixed set of nodes that act as both senders and
# receivers. That is, any node can send a message; when a node receives a
# message, it rebroadcasts the message to all other nodes. This ensures that we
# can tolerate the failure of nodes that have partially completed message
# sends. We assume that all nodes configured with the same set of values in
# "node". To ensure that log entries are globally unique, they are identified
# with a pair: <creator-addr, id> (note that "creator" is the node that
# originated the log entry, which is often different from the node that sent a
# message containing that entry.)
class BroadcastAll
  include Bud

  def initialize(addrs, opts={})
    @addr_list = addrs
    super(opts)
  end

  bootstrap do
    node <= @addr_list.map {|a| [a]}
  end

  state do
    immutable :node, [:addr]
    table :log, [:creator, :id] => [:val]
    channel :chn, [:@addr, :creator, :id] => [:val]
  end

  bloom do
    chn <~ (log * node).pairs {|m,n| n + m}
    log <= chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => false, :disable_rse => false }

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist = ports.map {|p| BroadcastAll.new(addrs, opts.merge(:port => p))}
rlist.each(&:run_bg)

s = rlist.first
s.sync_do {
  s.log <+ [[s.ip_port, 1, 'foo'],
            [s.ip_port, 2, 'bar']]
}

sleep 4

rlist.each do |r|
  r.sync_do {
    puts "#{r.port}: log size = #{r.log.to_a.size}"
  }
end

rlist.each(&:stop)
