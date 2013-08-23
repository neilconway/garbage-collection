require 'rubygems'
require 'bud'

# Reliable broadcast with a single sender and a fixed set of receivers. Note
# that we don't tolerate sender failure.
class BroadcastFixedRewrite
  include Bud

  state do
    sealed :node, [:addr]
    table :sbuf, [:id] => [:val]
    channel :chn, [:@addr, :id] => [:val]
    table :rbuf, sbuf.schema
    table :chn_approx, chn.schema
    channel :chn_ack, [:@sender, :addr, :id] => [:val]
  end

  bloom do
    chn  <~ ((sbuf * node).pairs {|m,n| n + m}).notin(chn_approx)
    rbuf <= chn.payloads

    chn_ack <~ chn {|c| [c.source_addr] + c}
    chn_approx <= chn_ack.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

opts = { :channel_stats => true, :disable_rce => true, :disable_rse => false }

rlist = Array.new(2) { BroadcastFixedRewrite.new(opts) }
rlist.each(&:run_bg)

s = BroadcastFixedRewrite.new(opts)
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
