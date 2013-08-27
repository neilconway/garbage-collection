require 'rubygems'
require 'bud'

INSERT_OP = 1
DELETE_OP = 2

# An implementation of a replicated dictionary that uses reliable broadcast,
# similar to Wuu & Bernstein ("Efficient Solutions to the Replicated Log and
# Dictionary Problems", PODC'84).
#
# The system consists of a (fixed) set of nodes; each node has a complete copy
# of the log. Any node can add a new entry to the log, which will then be
# replicated to all the other nodes. Log entries are uniquely identified and
# consist of an operation (create/delete), a key, and an optional value (for
# creations). Log entries are used to construct a dictionary. Each node also
# keeps track of the knowledge at every other node; this information is used to
# reclaim log entries when we know that they have been delivered to all sites.
#
# Our goal is to (a) implement the positive dictionary logic (log broadcast +
# dictionary construction) (b) automatically infer the logic for both
# propagating knowledge about node state and reclaiming log entries.
#
# Differences from Wuu & Bernstein:
# (1) We don't assume that messages from A -> B are delivered in-order
# (2) We don't explicitly depend on stamping messages with the sender's clock;
#     i.e., we just assume that each message has a unique ID
# (3) (Possible) we might exchange common knowledge (what W&B call "2DTT") in a
#     different manner
#
# TODO:
# * how to handle/prevent/allow concurrent insertions of the same key?
# * look at different schemes for propagating common knowledge
class ReplDict
  include Bud

  state do
    sealed :node, [:addr]
    channel :chn, [:@addr, :id] => [:op_type, :key, :val]
    table :log, [:id] => [:op_type, :key, :val]
    table :ins_ops, [:key] => [:val]
    table :del_ops, [:key]
    scratch :view, [:key] => [:val]
  end

  bloom do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads

    ins_ops <= log {|l| [l.key, l.val] if l.op_type == INSERT_OP}
    del_ops <= log {|l| [l.key] if l.op_type == DELETE_OP}
    view <= ins_ops.notin(del_ops, :key => :key)
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| ReplDict.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

rlist.each_with_index do |r,i|
  t = [[r.ip_port, 1], INSERT_OP, "foo#{i}", 'bar']
  r.log <+ [t]
  r.tick
end

r = rlist.first
t = [[r.ip_port, 3], DELETE_OP, 'foo2']
r.log <+ [t]
r.tick

8.times { sleep 0.2; rlist.each(&:tick) }

puts r.view.to_a.inspect
puts "# of log records: #{r.log.to_a.size}"
puts "# of ins_ops: #{r.ins_ops.to_a.size}"
puts "# of del_ops: #{r.del_ops.to_a.size}"

rlist.each(&:stop)
