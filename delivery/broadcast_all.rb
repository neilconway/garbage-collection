require 'rubygems'
require 'bud'

# Reliable broadcast with a fixed set of nodes that act as both senders and
# receivers. That is, any node can send a message; when a node receives a
# message, it rebroadcasts the message to all other nodes. This ensures that we
# can tolerate the failure of nodes that have partially completed message
# sends. We assume that all nodes configured with the same set of values in
# "node". We also assume that message IDs are globally unique.
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
    table :node, [:addr]        # XXX: s/table/immutable/
    table :sbuf, [:id] => [:val, :sender]
    channel :chn, [:id, :@addr] => [:val, :sender]
    table :rbuf, chn.schema
  end

  bloom do
    chn   <~ (sbuf * node).pairs {|m,n| [m.id, n.addr, m.val, m.sender]}
    sbuf  <= chn {|c| [c.id, c.val, c.sender]}
    rbuf  <= chn

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| BroadcastAll.new(addrs, :ip => "localhost", :port => p)}
rlist.each(&:run_bg)

# NB: as a hack to test that we tolerate sender failures, have the original
# sender only send to one of the receivers.
s = BroadcastAll.new([addrs.first])
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, 'foo', s.ip_port],
             [2, 'bar', s.ip_port]]
}

sleep 3

s.stop
rlist.each(&:stop)
