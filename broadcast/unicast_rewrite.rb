require 'rubygems'
require 'bud'

class SingleRecvRewrite
  include Bud

  state do
    table :sbuf, [:id] => [:addr, :val]
    scratch :sbuf_to_send, sbuf.schema
    table :rbuf, [:id] => [:addr, :val, :sender]
    table :rbuf_approx, rbuf.schema
    channel :chn, [:id] => [:@addr, :val, :sender]
    channel :ack_chn, [:id] => [:@addr, :val, :sender]
  end

  bloom do
    sbuf_to_send <= sbuf.notin(rbuf_approx)
    chn <~ sbuf_to_send {|s| s.to_a + [ip_port]}
    rbuf  <= chn
    stdio <~ chn.inspected

    ack_chn <~ chn {|c| [c.id, c.sender, c.val, c.addr]}
    rbuf_approx <= ack_chn {|a| [a.id, a.sender, a.val]}
  end
end

r = SingleRecvRewrite.new
r.run_bg

s = SingleRecvRewrite.new
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, r.ip_port, 'foo'],
             [2, r.ip_port, 'bar']]
}

sleep 3

s.stop
r.stop
