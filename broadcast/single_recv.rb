require 'rubygems'
require 'bud'

class SingleRecv
  include Bud

  state do
    table :sbuf, [:id] => [:addr, :val]
    table :rbuf, sbuf.schema
    channel :chn, [:id] => [:@addr, :val]
  end

  bloom do
    chn   <~ sbuf
    rbuf  <= chn
    stdio <~ chn.inspected
  end
end

r = SingleRecv.new
r.run_bg

s = SingleRecv.new
s.run_bg
s.sync_do {
  s.sbuf <+ [[1, r.ip_port, 'foo'],
             [2, r.ip_port, 'bar']]
}

sleep 3

s.stop
r.stop
