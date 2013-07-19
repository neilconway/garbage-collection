require 'rubygems'
require 'bud'

class Unicast
  include Bud

  state do
    channel :chn, [:id] => [:@addr, :val, :sender]
    table :sbuf, chn.schema
    table :rbuf, chn.schema
  end

  bloom do
    chn   <~ sbuf
    rbuf  <= chn

    stdio <~ chn {|c| ["Got msg: #{c.inspect}"]}
  end
end

r = Unicast.new
r.run_bg

s = Unicast.new
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, r.ip_port, 'foo', s.ip_port],
             [2, r.ip_port, 'bar', s.ip_port]]
}

sleep 3

s.stop
r.stop
