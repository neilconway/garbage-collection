require 'rubygems'
require 'bud'

class UnicastRewrite
  include Bud

  state do
    channel :chn, [:id] => [:@addr, :val]
    table :sbuf, chn.schema
    table :rbuf, chn.schema
    table :chn_approx, chn.schema
    channel :chn_ack, [:@sender, :id] => [:addr, :val]
  end

  bloom do
    chn  <~ sbuf.notin(chn_approx)
    rbuf <= chn

    chn_ack <~ chn {|c| [c.source_addr] + c}
    chn_approx <= chn_ack.payloads

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
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

s.sync_do {
  puts "#{s.port}: sbuf size = #{s.sbuf.to_a.size}"
}

s.stop
r.stop
