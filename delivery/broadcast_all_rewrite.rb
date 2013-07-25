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
    channel :chn, [:id, :creator, :@addr] => [:val]
    table :chn_approx, chn.schema
    channel :ack_chn, [:@sender, :id, :creator, :addr] => [:val]
  end

  bloom do
    chn <~ ((log * node).pairs {|m,n| [m.id, m.creator, n.addr, m.val]}).notin(chn_approx)
    log <= chn {|c| [c.id, c.creator, c.val]}

    ack_chn <~ chn {|c| [remote_addr(c)] + c}
    chn_approx <= ack_chn {|c| [c.id, c.creator, c.addr, c.val]}

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| BroadcastAllRewrite.new(addrs, :ip => "localhost", :port => p)}
rlist.each(&:run_bg)

# NB: as a hack to test that we tolerate sender failures, have the original
# sender only send to one of the receivers.
s = BroadcastAllRewrite.new([addrs.first])
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, s.ip_port, 'foo'],
             [2, s.ip_port, 'bar']]
}

sleep 2

s.stop
rlist.each(&:stop)
