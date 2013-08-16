require 'rubygems'
require 'bud'

# Additional unspecified safety conditions:
#  (1) 'sbuf' must not appear on the RHS of any other (user) rules
#  (2) 'send_ack' must not appear on the LHS of any deletion rules
class RseJoinTest
  include Bud

  state do
    table :sbuf, [:id] => [:epoch]
    table :node, [:addr, :epoch]
    scratch :to_send, [:id, :addr]
    table :send_ack, to_send.schema

    scratch :sbuf_node_ack, [:sbuf_id, :sbuf_epoch, :node_addr, :node_epoch]
    scratch :sbuf_node_missing, sbuf_node_ack.schema
    scratch :sbuf_reclaim, sbuf.schema
    scratch :node_reclaim, node.schema

    # Sealing metadata: a tuple in this table asserts that no more 'node' tuples
    # will be delivered for the given epoch; this allows sbuf messages that have
    # been acknowledged by all nodes in the sbuf's epoch to be reclaimed.
    table :node_seal_epoch, [:epoch]

    # A tuple in this table asserts that no more 'sbuf' tuples will be delivered
    # for a given epoch; this allows 'node' tuples that have acknowledged all
    # the messages in a given epoch to be reclaimed.
    table :sbuf_seal_epoch, [:epoch]
  end

  bloom do
    # User program (the notin clause will typically be inferred by RCE)
    to_send <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| [s.id, n.addr]}).notin(send_ack)

    # Find the set of join input tuples (i.e., pairs of sbuf, node tuples) that
    # have been acknowledged
    sbuf_node_ack <= (send_ack * sbuf * node).combos(send_ack.id => sbuf.id, send_ack.addr => node.addr, sbuf.epoch => node.epoch) {|_,s,n| s + n}

    # Find (sbuf, node) pairs that have not yet been acknowledged
    sbuf_node_missing <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| s + n}).notin(sbuf_node_ack)

    # We can reclaim an sbuf s when (a) the list of node addresses in s.epoch is
    # sealed (b) s has been acknowledged by all the addresses in the epoch
    # (i.e., (s, n) does not exist in sbuf_node_missing for any n).
    sbuf_reclaim <= (sbuf * node_seal_epoch).lefts(:epoch => :epoch).notin(sbuf_node_missing, :id => :sbuf_id, :epoch => :sbuf_epoch)
    sbuf <- sbuf_reclaim

    # We can reclaim a node n when (a) the list of messages (sbuf) in n.epoch is
    # sealed (b) n has acknowledged all the messages in the epoch (i.e., (s, n)
    # does not exist in sbuf_node_missing for any s).
    node_reclaim <= (node * sbuf_seal_epoch).lefts(:epoch => :epoch).notin(sbuf_node_missing, :addr => :node_addr, :epoch => :node_epoch)
    node <- node_reclaim
  end
end

n = RseJoinTest.new
n.sbuf <+ [[5, 1], [6, 1], [7, 2]]
n.node <+ [["foo", 1], ["bar", 1], ["baz", 2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.send_ack <+ [[5, "foo"], [5, "bar"], [6, "foo"]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.node_seal_epoch <+ [[1]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.send_ack <+ [[7, "baz"]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.node_seal_epoch <+ [[2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.sbuf_seal_epoch <+ [[1]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.sbuf_seal_epoch <+ [[2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.tick

puts "========"
puts "FINAL STATE: sbuf = #{n.sbuf.to_a.inspect}, node = #{n.node.to_a.inspect}"
