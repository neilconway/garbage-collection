require 'rubygems'
require 'bud'

# An implementation of a key-value store (a.k.a., "replicated dictionary") that
# uses reliable broadcast, in the style of Wuu & Bernstein ("Efficient Solutions
# to the Replicated Log and Dictionary Problems", PODC'84).
#
# The system consists of a (fixed) set of nodes; each node has a complete copy
# of the log. Any node can add a new entry to the log, which will then be
# replicated to all the other nodes. Each log entry has a unique ID. Insert log
# entries contain a <key, value> pair; delete log entries contain the unique ID
# of the insert to be removed. Log entries are used to construct the set of live
# key-value mappings.  Each node also keeps track of the knowledge at every
# other node; this info is used to reclaim log entries when they have been
# delivered to all nodes.
#
# Wuu & Bernstein assume that there will be at most one insert for a given key;
# hence, a delete references a _key_, and deleted elements can never be
# reinstated. Instead, we allow multiple inserts of the same key (the live view
# consists of all such non-deleted inserts). Deletes reference an insertion ID;
# once a given _ID_ has been deleted, it cannot be reinstated -- but another
# insert with the same key is allowed.
#
# Our goal is to (a) implement the positive program logic (log broadcast +
# construction of the live view) (b) automatically infer the logic for both
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
# (5) As noted above, multiple inserts of the same key are allowed, and deletes
#     reference IDs, not key names.
#
# TODO:
# * look at different schemes for propagating common knowledge
#   (=> more efficient ACK'ing protocol, gossip, etc.)
class KvsReplica
  include Bud

  state do
    sealed :node, [:addr]
    channel :ins_chn, [:@addr, :id] => [:key, :val]
    channel :del_chn, [:@addr, :id] => [:del_id]
    table :ins_log, [:id] => [:key, :val]
    table :del_log, [:id] => [:del_id]
    scratch :view, ins_log.schema
  end

  bloom do
    ins_chn <~ (node * ins_log).pairs {|n,l| n + l}
    del_chn <~ (node * del_log).pairs {|n,l| n + l}

    ins_log <= ins_chn.payloads
    del_log <= del_chn.payloads

    view <= ins_log.notin(del_log, :id => :del_id)
  end

  def print_view
    puts "View @ #{port}:"
    puts view.map {|v| "\t#{v.key} => #{v.val}"}.sort.join("\n")
  end
end
