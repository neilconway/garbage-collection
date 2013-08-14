require 'rubygems'
require 'bud'

# Additional unspecified safety conditions:
#  (1) 'sbuf' must not appear on the RHS of any other (user) rules
#  (2) 'send_ack' must not appear on the LHS of any deletion rules
class NegationTest
  include Bud

  state do
    table :sbuf, [:id] => [:epoch]
    table :node, [:addr, :epoch]
    scratch :to_send, [:id, :addr]
    table :send_ack, to_send.schema

    scratch :got_ack, [:sbuf, :node]
    scratch :missing_val, sbuf.schema
    scratch :to_reclaim, sbuf.schema

    # Sealing metadata: a tuple in this table asserts that no more 'node' tuples
    # will be delivered for the given epoch.
    table :node_seal_epoch, [:epoch]
  end

  bloom do
    # User program, except the notin clause will typically be inferred by RCE
    to_send <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| [s.id, n.addr]}).notin(send_ack)

    # Find the set of join input tuples (i.e., pairs of sbuf, node tuples) that
    # have been acknowledged
    got_ack <= (send_ack * sbuf * node).combos(send_ack.id => sbuf.id, send_ack.addr => node.addr, sbuf.epoch => node.epoch) {|_,s,n| [s,n]}

    # An sbuf that doesn't have an acknowledgment for an address in the sbuf's
    # epoch cannot (yet) be reclaimed
    missing_val <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| [s,n]}).notin(got_ack).pro {|x| x.first}

    # We can reclaim whatever isn't unsafe to reclaim, provided the epoch to
    # which the message belongs has been sealed
    to_reclaim <= (sbuf * node_seal_epoch).lefts(:epoch => :epoch).notin(missing_val)
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

n.node_seal_epoch <+ [[1]]
n.tick

puts "MISSING: #{n.missing_val.to_a.inspect}"
puts "TO RECLAIM: #{n.to_reclaim.to_a.inspect}"

n.send_ack <+ [[7, "baz"]]
n.tick

puts "MISSING: #{n.missing_val.to_a.inspect}"
puts "TO RECLAIM: #{n.to_reclaim.to_a.inspect}"

n.node_seal_epoch <+ [[2]]
n.tick

puts "MISSING: #{n.missing_val.to_a.inspect}"
puts "TO RECLAIM: #{n.to_reclaim.to_a.inspect}"
