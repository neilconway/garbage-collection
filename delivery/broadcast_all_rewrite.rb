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
class BroadcastAllRewrite
  include Bud

  def initialize(addrs, opts={})
    @addr_list = addrs
    super(opts)
  end

  bootstrap do
    node <= @addr_list.map {|a| [a]}
  end

  state do
    table :node, [:addr]        # XXX: s/table/immutable/
    table :log, [:id, :creator] => [:val]
    channel :chn, [:@addr, :id, :creator] => [:val]
    table :chn_approx, chn.schema
    channel :ack_chn, [:@sender, :addr, :id, :creator] => [:val]
  end

  bloom do
    chn <~ ((log * node).pairs {|m,n| [n.addr] + m}).notin(chn_approx)
    log <= chn.payloads

    ack_chn <~ chn {|c| [c.source_address] + c}
    chn_approx <= ack_chn.payloads

    stdio <~ ack_chn {|c| ["Got ack @ #{port}, t = #{budtime}: #{c.inspect}"]}
    stdio <~ chn {|c| ["Got msg @ #{port}, t = #{budtime}: #{c.inspect}"]}
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist = ports.map {|p| BroadcastAllRewrite.new(addrs, :port => p,
                                               :channel_stats => true)}
rlist.each(&:run_bg)

s = rlist.first
s.sync_do {
  s.log <+ [[1, s.ip_port, 'foo']]
            [2, s.ip_port, 'bar']]
}

sleep 5
rlist.each(&:stop)
