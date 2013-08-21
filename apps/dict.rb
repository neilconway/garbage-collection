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

  def initialize(addrs, opts={})
    @addr_list = addrs
    super(opts)
  end

  bootstrap do
    node <= @addr_list.map {|a| [a]}
  end

  state do
    sealed :node, [:addr]
    table :log, [:creator, :id] => [:op_type, :key, :val]
    channel :chn, [:@addr, :creator, :id] => [:op_type, :key, :val]
    scratch :view, [:key] => [:val]
    scratch :ins_ops, [:key] => [:val]
    scratch :del_ops, [:key]
  end

  bloom do
    ins_ops <= log {|l| [l.key, l.val] if l.op_type == INSERT_OP}
    del_ops <= log {|l| [l.key] if l.op_type == DELETE_OP}
    view <= ins_ops.notin(del_ops, :key => :key)

    chn <~ (log * node).pairs {|l,n| n + l}
    log <= chn.payloads
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| ReplDict.new(addrs, :ip => "localhost", :port => p)}
rlist.each(&:run_bg)

rlist.each_with_index do |r,i|
  r.sync_do {
    r.log <+ [[r.ip_port, 1, INSERT_OP, "foo#{i}", 'bar']]
  }
end

r = rlist.first
r.sync_do {
  r.log <+ [[r.ip_port, 2, DELETE_OP, 'foo2']]
}

sleep 2

r.sync_do {
  puts r.view.to_a.inspect
}

rlist.each(&:stop)
