require 'rubygems'
require 'bud'

class UnicastRewrite
  include Bud

  state do
    channel :chn, [:id] => [:@addr, :val]
    table :sbuf, chn.schema
    table :rbuf, chn.schema
    table :rbuf_approx, rbuf.schema
    channel :ack_chn, [:@sender, :id] => [:addr, :val]
  end

  bloom do
    chn   <~ sbuf.notin(rbuf_approx)
    rbuf  <= chn

    ack_chn <~ chn {|c| [c.source_address] + c}
    rbuf_approx <= ack_chn.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
    stdio <~ ack_chn {|c| ["Got ack: #{c.inspect}"]}
  end
end

r = UnicastRewrite.new
r.run_bg

s = UnicastRewrite.new
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, r.ip_port, 'foo'],
             [2, r.ip_port, 'bar']]
}

sleep 2

s.stop
r.stop
