require 'rubygems'
require 'bud'

class UnicastRewrite
  include Bud

  state do
    channel :chn, [:id] => [:@addr, :val]
    table :sbuf, chn.schema
    table :rbuf, chn.schema
    table :chn_approx, chn.key_cols
    channel :chn_ack, [:@sender] + chn.key_cols
  end

  bloom do
    chn  <~ sbuf.notin(chn_approx, 0 => :id)
    rbuf <= chn

    chn_ack <~ chn {|c| [c.source_addr, c.id]}
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
