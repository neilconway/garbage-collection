require 'rubygems'
require 'bud'

# Slight variant of ReplDict; here, we assume that deletions identify the key to
# be removed, not the ID. Because the "key" column is not a (Bloom) key of the
# ins_log collection (i.e., we can have multiple inserts with different IDs and
# the same key), this means we can never discard deletion log entries; moreover,
# it means that once a key has been deleted it can never be reinstated (since a
# single deletion is taken to dominate all insertions). This behavior is similar
# to the 2P-Set CRDT.
class ReplDictDeleteByKey
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
rlist = ports.map {|p| ReplDictDeleteByKey.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

rlist.each_with_index do |r,i|
  r.ins_log <+ [[r.id(1), "foo#{i}", 'bar']]
  r.tick
end

first = rlist.first
first.del_log <+ [[first.id(1), 'foo2'], [first.id(2), 'foo1']]
first.tick

10.times { sleep 0.1; rlist.each(&:tick) }

puts first.print_view
puts "# of insert log records: #{first.ins_log.to_a.size}"
puts "# of delete log records: #{first.del_log.to_a.size}"

rlist.each(&:stop)
