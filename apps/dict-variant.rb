require 'rubygems'
require 'bud'

# Slight variant of the ReplicatedDict; here, we assume that deletions identify
# the ID of their corresponding insert, not the key; hence, if we see two
# inserts for a given key with different IDs but only one deletion, the key is
# considered to still be inserted.
class ReplDictVariant
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

opts = { :channel_stats => false }
ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| ReplDictVariant.new(opts.merge(:ip => "localhost", :port => p))}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

first = rlist.first
first.ins_log <+ [[first.id(1), 'foo', 'bar'],
                  [first.id(2), 'foo', 'bar2'],
                  [first.id(3), 'baz', 'qux']]

last = rlist.last
last.del_log <+ [[last.id(1), first.id(1)],
                 [last.id(2), first.id(2)]]

10.times { rlist.each(&:tick); sleep 0.1 }

first.print_view
last.print_view
puts "# of insert log records: #{first.ins_log.to_a.size}"
puts "# of delete log records: #{first.del_log.to_a.size}"

rlist.each(&:stop)
