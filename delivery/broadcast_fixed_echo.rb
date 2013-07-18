require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a fixed set of receivers. In order
# to tolerate sender failure, each receiver echoes every message it observes to
# every receiver. We assume that all the receivers are configured with the same
# set of values in "node".
class BroadcastFixedEcho
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
    table :sbuf, [:id] => [:val]
    table :rbuf, [:id] => [:addr, :val]
    channel :chn, [:id, :@addr] => [:val]
  end

  bloom do
    chn   <~ (sbuf * node).pairs {|m,n| [m.id, n.addr, m.val]}
    sbuf  <= chn {|c| [c.id, c.val]}
    rbuf  <= chn

    stdio <~ chn {|c| ["Sending: #{c.inspect}"]}
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}

rlist = ports.map {|p| BroadcastFixedEcho.new(addrs, :ip => "localhost", :port => p)}
rlist.each(&:run_bg)

# NB: as a hack to test that we tolerate sender failures, have the original
# sender only send to one of the receivers.
s = BroadcastFixedEcho.new([addrs.first])
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, 'foo'],
             [2, 'bar']]
}

sleep 3

s.stop
rlist.each(&:stop)
