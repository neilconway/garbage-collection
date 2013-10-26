require 'rubygems'
require 'bud'

class WuuLog
  include Bud

  state do
    sealed :node, [:addr]
    table :log, [:id] => [:val]
    channel :chn, [:@addr, :id] => [:val]

    # A tuple here means that x knows that y knows about message "id". This is
    # essentially a set-theoretic representation of a matrix clock. Range
    # compression means this will use O(n^2) space (for n nodes) as we'd hope,
    # rather than O(mn^2) (m messages), as a naive representation would.
    range :chn_approx, [:x, :y, :id]

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
    chn_approx <= chn {|c| [c.source_addr, c.source_addr, c.id]}

    # When we get a message from X, tell X what we know about the knowledge of
    # all the nodes. Note that the timing of this message is irrelevant -- i.e.,
    # we could trigger it based on inbound chn messages, as we do, or else just
    # send it periodically.
    chn_ack <~ (chn * chn_approx).pairs {|c,l| [c.source_addr] + l}

    # Update our local knowledge based on receiving common knowledge from
    # another node
    chn_approx <= chn_ack.payloads
  end
end

opts = { :channel_stats => true, :disable_rce => true, :disable_rse => false }

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "127.0.0.1:#{p}"}
rlist = ports.map {|p| WuuLog.new(opts.merge(:port => p))}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.run_bg
end

s = rlist.first
s.sync_do {
  s.log <+ [[s.id(1), 'foo'], [s.id(2), 'bar'], [s.id(3), 'baz'],
            [s.id(4), 'qux'], [s.id(5), 'qux2'], [s.id(6), 'qux3'],
            [s.id(7), 'qux4'], [s.id(8), 'qux5'], [s.id(9), 'qux6'],
            [s.id(10), 'qux7']]
}

sleep 4

rlist.each do |r|
  r.tick
  r.sync_do {
    puts "#{r.port}: log size = #{r.log.to_a.size}"
  }
end

rlist.each(&:stop)
