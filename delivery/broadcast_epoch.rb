require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a dynamic set of receivers. We
# assume that receiver groups are "sealed" on epoch number -- that is, we learn
# about all the receivers in a given epoch simultaneously. That means that while
# new receivers can be added (in new epochs), the membership list of a given
# epoch is fixed. Moreover, each outbound message is tagged with the epoch to
# which it should be delivered. Outbound messages for not-yet-known epochs will
# be buffered until the epoch has been learned; also, new outbound messages for
# prior epochs can always be learned (and will be delivered correctly), since we
# don't try to reclaim node information for prior epochs.
#
# Note that we don't tolerate sender failure.
class BroadcastEpoch
  include Bud

  state do
    table :node, [:addr, :epoch]        # XXX: s/table/sealed-on-epoch/
    table :sbuf, [:id] => [:epoch, :val]
    table :rbuf, sbuf.schema
    channel :chn, [:@addr, :id] => [:epoch, :val]
  end

  bloom do
    chn <~ (sbuf * node).pairs(:epoch => :epoch) {|m,n| [n.addr] + m}
    rbuf <= chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

rlist = Array.new(2) { BroadcastEpoch.new }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

s = BroadcastEpoch.new
s.run_bg
s.sync_do {
  s.node <+ [[r_addrs.first, "first"]]
  s.node <+ r_addrs.map {|a| [a, "second"]}
  s.sbuf <+ [[1, "first", 'foo'],
             [2, "second", 'bar']]
}

sleep 2

s.stop
rlist.each(&:stop)
