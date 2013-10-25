require 'rubygems'
require 'bud'

class ReliableLog
  include Bud

  state do
    sealed :node
    table :log, [:id] => [:val]
    channel :chn, [:@addr, :id] => [:val]

    # A tuple here means that x knows that y knows about message "id"
    table :chn_approx, [:x, :y, :id]
    # Tell node "addr" that node x knows that node y knows about message "id"
    channel :chn_ack, [:@addr] + chn_approx.schema
  end

  bloom do
    # Don't send a message to node X if ANY node knows that X has already seen
    # the message
    chn <~ ((node * log).pairs {|n,l| n + l}).notin(chn_approx, 0 => :y, 1 => :id)
    log <= chn.payloads

    # Update our local knowledge based on learning a new log message. When we
    # receive message m from X, we know that (a) X knows about m (b) we know
    # about m.
    chn_approx <= chn {|c| [ip_port, c.source_addr, c.id]}
    chn_approx <= chn {|c| [c.source_add, c.source_addr, c.id]}

    # When we get a message from X, tell X what we know about the knowledge of
    # all the nodes
    chn_ack <~ (chn * chn_approx).pairs {|c,l| [c.source_address] + l}

    # Update our local knowledge based on receiving common knowledge from
    # another node
    chn_approx <= chn_ack.payloads
  end
end
