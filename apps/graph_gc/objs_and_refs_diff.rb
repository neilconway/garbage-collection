require 'rubygems'
require 'bud'

class GraphGC
  include Bud

  state do
    sealed :node, [:addr]
    table :objects, [:obj]
    
    channel :ins_chn, [:@addr, :ref] => [:obj]
    channel :del_chn, [:@addr, :ref] 

    table :ins_ref, [:ref] => [:obj]
    table :del_ref, [:ref]

    scratch :active_objects, [:obj]
    scratch :active_refs, ins_ref.schema
    
  end

  bloom do
    ins_chn <~ (node * ins_ref).pairs {|n,l| n + l}
    del_chn <~ (node * del_ref).pairs {|n,l| n + l}

    ins_ref <= ins_chn { |i| [i.ref, i.obj] }
    del_ref <= del_chn { |d| [d.ref] }

    active_refs <= ins_ref.notin(del_ref, :ref => :ref)
    active_objects <= active_refs { |a| [a.obj] }
  end

  def print_active_objects
    puts "active_objects @ #{port}:"
    puts active_objects.map {|v| "\t#{v.obj}"}.sort.join("\n")
  end

  def print_active_refs
    puts "active references @ #{port}:"
    puts active_refs.map { |v| "\t#{v.ref} => #{v.obj}"}.sort.join("\n")
  end

  def print_objects
    puts "objects @ #{port}"
    puts objects.map {|v| "\t#{v.obj}"}.sort.join("\n")
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| GraphGC.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  #r.objects <+ [['object0'], ['object1'], ['object2']]
  r.tick
end

rlist.each_with_index do |r,i|
  r.ins_ref <+ [["object_ref#{i}", "object#{i}"]]
  r.ins_ref <+ [["object_ref#{i+10}", "object#{i}"]]
  r.tick
end

first = rlist.first
first.del_ref <+ [['object_ref2'], ['object_ref1'], ['object_ref11']]
first.tick

10.times { sleep 0.1; rlist.each(&:tick) }

puts first.print_active_objects
puts first.print_active_refs
puts first.print_objects
puts "# of insert log records: #{first.ins_ref.to_a.size}"
puts "# of delete log records: #{first.del_ref.to_a.size}"

rlist.each(&:stop)
