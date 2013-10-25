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
  end
end

r = Unicast.new(:channel_stats => true)
r.run_bg

s = Unicast.new(:channel_stats => true)
s.run_bg
s.sync_do {
  100.times do |i|
    s.sbuf <+ [[i*2, r.ip_port, "foo#{i}"],
               [(i*2)+1, r.ip_port, "bar#{i}"]]
  end
}

sleep 2

s.tick  # Needed so that the pending @delete for sbuf is applied
s.sync_do {
  puts "#{s.port}: sbuf size = #{s.sbuf.to_a.size}"
}

s.stop
r.stop
