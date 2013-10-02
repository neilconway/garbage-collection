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
    scratch :to_send, [:id, :addr, :epoch]
    table :send_ack, to_send.schema
  end

  bloom do
    # User program (the notin clause will typically be inferred by RCE)
    to_send <= ((sbuf * node).pairs(:epoch => :epoch) {|s,n| [s.id] + n}).notin(send_ack)
  end
end

n = RseJoinTest.new
puts n.t_rules.map{|r| r.orig_src}.join("\n")
n.sbuf <+ [[5, 1], [6, 1], [7, 2]]
n.node <+ [["foo", 1], ["bar", 1], ["baz", 2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.send_ack <+ [[5, "foo", 1], [5, "bar", 1], [6, "foo", 1]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.seal_node_epoch <+ [[1]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.send_ack <+ [[7, "baz", 2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.seal_node_epoch <+ [[2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.seal_sbuf_epoch <+ [[1]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.seal_sbuf_epoch <+ [[2]]
n.tick

puts "MISSING: #{n.sbuf_node_missing.to_a.inspect}"
#puts "RECLAIM: sbuf = #{n.sbuf_reclaim.to_a.inspect}; node = #{n.node_reclaim.to_a.inspect}"

n.tick

puts "========"
puts "FINAL STATE: sbuf = #{n.sbuf.to_a.inspect}, node = #{n.node.to_a.inspect}"
