require 'rubygems'
require 'bud'

class NegationTest
  include Bud

  state do
    table :sbuf, [:id] => [:epoch]
    table :node, [:addr, :epoch]
    scratch :to_send, [:id, :addr]
    table :send_ack, to_send.schema

    scratch :got_ack, [:id, :msg_epoch, :addr, :addr_epoch]
    scratch :missing_val, got_ack.schema
    scratch :to_reclaim, sbuf.schema
  end

  bloom do
    to_send <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| [s.id, n.addr]}).notin(send_ack)

    got_ack <= (send_ack * sbuf * node).combos(send_ack.id => sbuf.id, send_ack.addr => node.addr, sbuf.epoch => node.epoch) {|a,s,n| s + n}
    missing_val <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| s + n}).notin(got_ack)
    to_reclaim <= sbuf.notin(missing_val, :id => :id, :epoch => :msg_epoch)
  end
end

n = NegationTest.new
n.sbuf <+ [[5, 1], [6, 1], [7, 2]]
n.node <+ [["foo", 1], ["bar", 1], ["baz", 2]]
n.tick

puts "MISSING: #{n.missing_val.to_a.inspect}"
puts "TO RECLAIM: #{n.to_reclaim.to_a.inspect}"

n.send_ack <+ [[5, "foo"], [5, "bar"], [6, "foo"]]
n.tick

puts "MISSING: #{n.missing_val.to_a.inspect}"
puts "TO RECLAIM: #{n.to_reclaim.to_a.inspect}"

n.send_ack <+ [[7, "baz"]]
n.tick

puts "MISSING: #{n.missing_val.to_a.inspect}"
puts "TO RECLAIM: #{n.to_reclaim.to_a.inspect}"
