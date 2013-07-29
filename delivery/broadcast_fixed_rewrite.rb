require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a fixed set of receivers. Note
# that we don't tolerate sender failure.
class BroadcastFixedRewrite
  include Bud

  def initialize(addrs=[], opts={})
    @addr_list = addrs
    super(opts)
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
    channel :chn_ack, [:@sender, :addr, :id] => [:val]
  end

  bloom do
    chn  <~ ((sbuf * node).pairs {|m,n| [n.addr] + m}).notin(chn_approx)
    rbuf <= chn.payloads

    chn_ack <~ chn {|c| [c.source_address] + c}
    chn_approx <= chn_ack.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => true }

rlist = Array.new(2) { BroadcastFixedRewrite.new([], opts) }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

s = BroadcastFixedRewrite.new(r_addrs, opts)
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, 'foo'],
             [2, 'bar']]
}

sleep 2

s.stop
rlist.each(&:stop)
