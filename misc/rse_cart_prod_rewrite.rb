require 'rubygems'
require 'bud'

# Additional unspecified safety conditions:
#  (1) 'sbuf' must not appear on the RHS of any other (user) rules
#  (2) 'send_ack' must not appear on the LHS of any deletion rules
class RseCartesianProdTest
  include Bud

  state do
    table :sbuf, [:id]
    table :node, [:addr]
    scratch :to_send, [:id, :addr]
    table :send_ack, to_send.schema

    scratch :sbuf_node_joinbuf, [:sbuf_id, :node_addr]
    scratch :sbuf_node_missing, sbuf_node_joinbuf.schema

    # Sealing metadata: a tuple in this table asserts that no more 'node' tuples
    # will be delivered.
    table :seal_node, [:ignored]

    # A tuple in this table asserts that no more 'sbuf' tuples will be
    # delivered.
    table :seal_sbuf, [:ignored]
  end

  bloom do
    # User program (the notin clause will typically be inferred by RCE)
    to_send <= ((sbuf * node).pairs {|s,n| s + n}).notin(send_ack)

    # Find the set of join input tuples (i.e., pairs of sbuf, node tuples) that
    # have been acknowledged
    sbuf_node_joinbuf <= (send_ack * sbuf * node).combos(send_ack.id => sbuf.id, send_ack.addr => node.addr) {|_,s,n| s + n}

    # Find (sbuf, node) pairs that have not yet been acknowledged
    sbuf_node_missing <= ((sbuf * node).pairs {|s,n| s + n}).notin(sbuf_node_joinbuf)

    # We can reclaim an sbuf s when (a) there is a collection-level seal for
    # node (b) s has been acknowledged by all the nodes. (i.e., (s, n) does not
    # exist in sbuf_node_missing for any n).
    sbuf <- (sbuf * seal_node).lefts.notin(sbuf_node_missing, :id => :sbuf_id)

    # We can reclaim a node n when (a) there is a collection-level seal for sbuf
    # (b) n acknowledged all the messages in sbuf. (i.e., (s, n) does not exist
    # in sbuf_node_missing for any s).
    node <- (node * seal_sbuf).lefts.notin(sbuf_node_missing, :addr => :node_addr)
  end
end
