require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a fixed set of receivers. Note
# that we don't tolerate sender failure.
class BroadcastFixed
  include Bud

  state do
    sealed :node, [:addr]
    table :sbuf, [:id] => [:val]
    channel :chn, [:@addr, :id] => [:val]
    table :rbuf, sbuf.schema
  end

  bloom do
    chn  <~ (sbuf * node).pairs {|m,n| n + m}
    rbuf <= chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => false, :disable_rse => false }

rlist = Array.new(2) { BroadcastFixed.new(opts) }
rlist.each(&:run_bg)

s = BroadcastFixed.new(opts)
s.node <+ rlist.map {|r| [r.ip_port]}
s.run_bg

s.sync_do {
  s.sbuf <+ [[1, 'foo'],
             [2, 'bar']]
}

sleep 2

s.sync_do {
  puts "#{s.port}: log size = #{s.sbuf.to_a.size}"
}

s.stop
rlist.each(&:stop)
