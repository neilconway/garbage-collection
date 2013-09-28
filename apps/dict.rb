require 'rubygems'
require 'bud'

# An implementation of a replicated dictionary that uses reliable broadcast,
# similar to Wuu & Bernstein ("Efficient Solutions to the Replicated Log and
# Dictionary Problems", PODC'84).
#
# The system consists of a (fixed) set of nodes; each node has a complete copy
# of the log. Any node can add a new entry to the log, which will then be
# replicated to all the other nodes. Log entries are uniquely identified and
# consist of an operation (insert/delete), a key, and an optional value (for
# insertions). Log entries are used to construct a dictionary. Each node also
# keeps track of the knowledge at every other node; this information is used to
# reclaim log entries when we know that they have been delivered to all sites.
#
# We assume that there is at most one insert for a given key; hence, once an
# element has been deleted it cannot be reinstated. These semantics are similar
# to the "2P-Set" CRDT.
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
# (4) We maintain insert and delete logs separately, rather than a single
#     unified log; this also means the ID sequences used by inserts and deletes
#     are not shared
#
# TODO:
# * try to prevent/handle multiple insertions of the same key?
# * look at different schemes for propagating common knowledge
#   (=> more efficient ACK'ing protocol, gossip, etc.)
class ReplDict
  include Bud

  state do
    sealed :node, [:addr]
    channel :ins_chn, [:@addr, :id] => [:key, :val]
    channel :del_chn, [:@addr, :id] => [:key]
    table :ins_log, [:id] => [:key, :val]
    table :del_log, [:id] => [:key]
    scratch :view, ins_log.schema
  end

  bloom do
    ins_chn <~ (node * ins_log).pairs {|n,l| n + l}
    del_chn <~ (node * del_log).pairs {|n,l| n + l}

    ins_log <= ins_chn.payloads
    del_log <= del_chn.payloads

    view <= ins_log.notin(del_log, :key => :key)
  end

  def print_view
    puts "View @ #{port}:"
    puts view.map {|v| "\t#{v.key} => #{v.val}"}.sort.join("\n")
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
  r.ins_log <+ [[[r.port, 1], "foo#{i}", 'bar']]
  r.tick
end

first = rlist.first
first.del_log <+ [[[first.port, 1], 'foo2'], [[first.port, 2], 'foo1']]
first.tick

10.times { sleep 0.1; rlist.each(&:tick) }

puts first.print_view
puts "# of insert log records: #{first.ins_log.to_a.size}"
puts "# of delete log records: #{first.del_log.to_a.size}"

rlist.each(&:stop)
