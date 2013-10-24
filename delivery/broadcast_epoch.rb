require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a dynamic set of receivers. Both
# receivers and messages are divided into epochs; the messages in an epoch
# cannot be reclaimed until we have a "seal" for the nodes in that epoch. That
# is, we assume some outside mechanism (typically based on consensus) that
# determines when a new epoch should begin and seals the new epoch. Similarly,
# tuples in "node" can be reclaimed if we a given epoch of messages has been
# sealed.
#
# Note that all nodes don't need to learn about seals or new epochs at the same
# time; outbound messages for as-yet-unknown epochs will be buffered until the
# epoch has been learned.
#
# Note that we don't tolerate sender failure.
class BroadcastEpoch
  include Bud

  state do
    table :node, [:addr, :epoch]
    table :sbuf, [:id] => [:epoch, :val]
    table :rbuf, sbuf.schema
    channel :chn, [:@addr, :id] => [:epoch, :val]
  end

  bloom do
    chn <~ (node * sbuf).pairs(:epoch => :epoch) {|n,m| [n.addr] + m}
    rbuf <= chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => false }

rlist = Array.new(2) { BroadcastEpoch.new(opts) }
rlist.each(&:run_bg)
r_addrs = rlist.map(&:ip_port)

s = BroadcastEpoch.new(opts)
s.run_bg
s.sync_do {
  s.node <+ [[r_addrs.first, "first"]]
  s.node <+ r_addrs.map {|a| [a, "second"]}
  s.sbuf <+ [[1, "first", 'foo'],
             [2, "second", 'bar']]
}

sleep 2

s.sync_do {
  puts "Buffered messages: #{s.sbuf.to_a.size}"
  puts "Missing: #{s.node_sbuf_missing.to_a.inspect}"
  puts "Joinbuf: #{s.node_sbuf_joinbuf.to_a.inspect}"
  s.seal_node_epoch <+ [["first"]]
}

s.sync_do

sleep 1

s.sync_do {
  puts "Buffered messages: #{s.sbuf.to_a.size}"
}

s.stop
rlist.each(&:stop)
