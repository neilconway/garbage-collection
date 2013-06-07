require "rubygems"
require "bud"

class RDelivery
  include Bud

  state do
    table :sbuf, [:id] => [:addr, :payload]
    table :ack, [:id]
    scratch :chn, [:id] => [:addr, :payload]
  end

  bloom do
    chn <= sbuf.notin(ack, :id => :id)
  end
end

r = RDelivery.new
r.sbuf <+ [[5, "localhost:123", "baz"], [6, "localhost:123", "bar"]]
r.tick
puts r.chn.to_a.inspect
r.ack <+ [[5]]
r.tick
puts r.chn.to_a.inspect
