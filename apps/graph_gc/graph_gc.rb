require 'rubygems'
require 'bud'

# Assumes no more references will be created. And no more 
# nodes will be added.

class GraphGC
  include Bud

  state do
    sealed :node, [:addr]
    scratch :objects, [:id] => [:obj_val]
    scratch :references, [:ref] => [:id]
    scratch :tombstones, objects.schema 
    
    table :create_obj, [:id] => [:obj_val]
    table :create_ref, [:ref] => [:id]
    table :delete_ref, [:ref]
    
    channel :create_obj_chn, [:@addr, :id] => [:obj_val]
    channel :create_ref_chn, [:@addr, :ref] => [:id]
    channel :delete_ref_chn, [:@addr, :ref] 
  end

  bloom do
    create_obj_chn <~ (node * create_obj).pairs {|n, c| [n.addr, c.id, c.obj_val] }
    create_obj <= create_obj_chn { |c| [c.id, c.obj_val] }

    create_ref_chn <~ (node * create_ref).pairs {|n, c| [n.addr, c.ref, c.id] }
    create_ref <= create_ref_chn { |c| [c.ref, c.id] }

    delete_ref_chn <~ (node * delete_ref).pairs {|n, d| [n.addr, d.ref] }
    delete_ref <= delete_ref_chn { |d| [d.ref] }

    references <= create_ref.notin(delete_ref, :ref => :ref)
    tombstones <= create_obj.notin(references, :id => :id)
    # Should garbage collect objects in tombstones from create_obj, but doesn't.
    objects <= create_obj.notin(tombstones, :id => :id)
  end

  def print_references
    puts "references @ #{port}:"
    puts references.map { |v| "\t#{v.ref} => #{v.id}"}.sort.join("\n")
  end

  def print_objects
    puts "objects @ #{port}"
    puts objects.map {|v| "\t#{v.id} => #{v.obj_val}"}.sort.join("\n")
  end

  def print_tombstones
    puts "Tombstones @ #{port}"
    puts tombstones.map {|v| "\t#{v.id} => #{v.obj_val}"}.sort.join("\n")
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
  r.create_obj <+ [["#{i}", "object#{i}"]]
  r.create_ref <+ [["reference#{i}", "#{i}"]]
  r.tick
end

first = rlist.first
first.create_ref <+ [["reference5", "1"]]
first.tick
first.tick
first.delete_ref <+ [["reference1"], ["reference5"]]
first.tick

10.times { sleep 0.1; rlist.each(&:tick) }

puts first.print_tombstones
puts first.print_references
puts first.print_objects
puts "# of created object records: #{first.create_obj.to_a.size}"
puts "# of created reference records: #{first.create_ref.to_a.size}"
puts "# of deleted reference records: #{first.delete_ref.to_a.size}"

rlist.each(&:stop)
