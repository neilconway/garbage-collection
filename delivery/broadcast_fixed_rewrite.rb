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
    table :sbuf, [:id] => [:val]
    channel :chn, [:@addr, :id] => [:val]
    table :rbuf, sbuf.schema
    table :chn_approx, chn.schema
    channel :ack_chn, [:@sender, :addr, :id] => [:val]
  end

  bloom do
    chn  <~ ((sbuf * node).pairs {|m,n| [n.addr] + m}).notin(chn_approx)
    rbuf <= chn.payloads

    ack_chn <~ chn {|c| [c.source_address] + c}
    chn_approx <= ack_chn.payloads

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
  s.sbuf <+ [[1, 'foo'],
             [2, 'bar']]
}

sleep 2

s.stop
rlist.each(&:stop)
