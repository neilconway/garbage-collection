require 'rubygems'
require 'bud'

# An implementation of a replicated dictionary that uses reliable broadcast,
# based on Wuu & Bernstein ("Efficient Solutions to the Replicated Log and
# Dictionary Problems", PODC'84).
#
# The system consists of a (fixed) set of nodes; each node has a copy of the
# log. Any node can add a new entry to the log, which will then be replicated to
# all the other nodes. Log entries are uniquely identified, and consist of an
# operation (create/delete), a key, and an optional value (for creations). Log
# entries are used to construct a dictionary. Each node also keeps track of the
# knowledge at every other node; this information is used to reclaim log entries
# when we know that they have been delivered to all sites.
#
# Our goal is to (a) implement the positive dictionary logic (log broadcast +
# dictionary construction) (b) automatically infer the logic for both
# propagating knowledge about node state and reclaiming log entries.
#
# Differences from Wuu & Bernstein:
# (1) We don't assume that messages from A -> B are delivered in-order
# (2) We don't explicitly depend on stamping messages with the sender's clock
# (3) (Possible) we might communicate common knowledge (what W&B call "2DTT") in
#     a different manner
#
# TODO:
# * how to handle/prevent/allow concurrent insertions of the same key?
# * look at different schemes for propagating common knowledge
class ReplDict
  include Bud

  state do
    table :node, [:addr]        # XXX: immutable
    table :log, [:creator, :id] => [:op_type, :key, :val]
    scratch :view, [:key] => [:val]
  end

  bloom do
  end
end
