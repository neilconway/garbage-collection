require 'rubygems'
require 'bud'

class Unicast
  include Bud

  state do
    channel :chn, [:id] => [:@addr, :val]
    table :sbuf, chn.schema
    table :rbuf, chn.schema
  end

  bloom do
    chn  <~ sbuf
    rbuf <= chn

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

r = Unicast.new
r.run_bg

s = Unicast.new
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
