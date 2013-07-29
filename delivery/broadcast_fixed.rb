require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a fixed set of receivers. Note
# that we don't tolerate sender failure.
class BroadcastFixed
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
  end

  bloom do
    chn  <~ (sbuf * node).pairs {|m,n| [n.addr] + m}
    rbuf <= chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => false }

rlist = Array.new(2) { BroadcastFixed.new([], opts) }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

s = BroadcastFixed.new(r_addrs, opts)
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, 'foo'],
             [2, 'bar']]
}

sleep 2

s.stop
rlist.each(&:stop)
