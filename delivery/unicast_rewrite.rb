require 'rubygems'
require 'bud'

class UnicastRewrite
  include Bud

  state do
    table :sbuf, [:id] => [:addr, :val, :sender]
    table :rbuf, sbuf.schema
    table :rbuf_approx, rbuf.schema
    channel :chn, [:id] => [:@addr, :val, :sender]
    channel :ack_chn, [:id] => [:addr, :val, :@sender]
  end

  bloom do
    chn   <~ sbuf.notin(rbuf_approx)
    rbuf  <= chn
    stdio <~ chn {|c| ["Sending: #{c.inspect}"]}

    ack_chn <~ chn
    rbuf_approx <= ack_chn
    stdio <~ ack_chn {|c| ["Got ack: #{c.inspect}"]}
  end
end

r = UnicastRewrite.new
r.run_bg

s = UnicastRewrite.new
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, r.ip_port, 'foo', s.ip_port],
             [2, r.ip_port, 'bar', s.ip_port]]
}

sleep 3

s.stop
r.stop
