require 'rubygems'
require 'bud'

class UnicastRewrite
  include Bud

  state do
    channel :chn, [:id] => [:@addr, :val, :sender]
    table :sbuf, chn.schema
    table :rbuf, chn.schema
    table :rbuf_approx, rbuf.schema
    channel :ack_chn, [:id] => [:addr, :val, :@sender]
  end

  bloom do
    chn   <~ sbuf.notin(rbuf_approx)
    rbuf  <= chn

    ack_chn <~ chn
    rbuf_approx <= ack_chn

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
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
