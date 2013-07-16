require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a static set of receivers. Note
# that we don't tolerate sender failure.
class MultiRecvStatic
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
    table :rbuf, [:id] => [:addr, :val]
    channel :chn, [:id, :@addr] => [:val]
  end

  bloom do
    chn   <~ (sbuf * node).pairs {|m,n| [m.id, n.addr, m.val]}
    rbuf  <= chn
    stdio <~ chn.inspected
  end
end

rlist = Array.new(2) { MultiRecvStatic.new }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

s = MultiRecvStatic.new(r_addrs)
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, 'foo'],
             [2, 'bar']]
}

sleep 3

s.stop
rlist.each(&:stop)
