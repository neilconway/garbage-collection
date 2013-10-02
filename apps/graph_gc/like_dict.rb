require 'rubygems'
require 'bud'

class GraphGC
  include Bud

  state do
    sealed :node, [:addr]
    channel :ins_chn, [:@addr, :id] => [:obj]
    channel :del_chn, [:@addr, :id] => [:obj]
    table :ins_obj, [:id] => [:obj]
    table :del_obj, [:id] => [:obj]
    scratch :active_objects, ins_obj.schema
  end

  bloom do
    ins_chn <~ (node * ins_obj).pairs {|n,l| n + l}
    del_chn <~ (node * del_obj).pairs {|n,l| n + l}

    ins_obj <= ins_chn.payloads
    del_obj <= del_chn.payloads

    active_objects <= ins_obj.notin(del_obj, :obj => :obj)
  end

  def print_active_objects
    puts "active_objects @ #{port}:"
    puts active_objects.map {|v| "\t#{v.obj}"}.sort.join("\n")
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| GraphGC.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

rlist.each_with_index do |r,i|
  r.ins_obj <+ [[[r.port, 1], "object#{i}"]]
  r.tick
end

first = rlist.first
first.del_obj <+ [[[first.port, 1], 'object2'], [[first.port, 2], 'object1']]
first.tick

10.times { sleep 0.1; rlist.each(&:tick) }

puts first.print_active_objects
puts "# of insert log records: #{first.ins_obj.to_a.size}"
puts "# of delete log records: #{first.del_obj.to_a.size}"

rlist.each(&:stop)
