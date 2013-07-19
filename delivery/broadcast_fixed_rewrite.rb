require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a fixed set of receivers. Note
# that we don't tolerate sender failure.
class BroadcastFixedRewrite
  include Bud

  def initialize(addrs=[])
    @addr_list = addrs
    super()
  end

  bootstrap do
    node <= @addr_list.map {|a| [a]}
  end

  state do
    table :node, [:addr]        # XXX: s/table/immutable/
    table :sbuf, [:id] => [:val, :sender]
    channel :chn, [:id, :@addr] => [:val, :sender]
    scratch :sbuf_out, chn.schema
    table :rbuf, chn.schema
    table :rbuf_approx, rbuf.schema
    channel :ack_chn, [:id, :addr] => [:val, :@sender]
    periodic :tik, 0.5
  end

  bloom do
    sbuf_out <= (sbuf * node).pairs {|m,n| [m.id, n.addr, m.val, m.sender]}
    chn  <~ sbuf_out.notin(rbuf_approx)
    rbuf <= chn

    ack_chn <~ chn
    rbuf_approx <= ack_chn

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
    stdio <~ ack_chn {|c| ["Got ack: #{c.inspect}"]}
  end
end

rlist = Array.new(2) { BroadcastFixedRewrite.new }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

s = BroadcastFixedRewrite.new(r_addrs)
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, 'foo', s.ip_port],
             [2, 'bar', s.ip_port]]
}

sleep 3

s.stop
rlist.each(&:stop)
