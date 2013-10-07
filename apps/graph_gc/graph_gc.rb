require 'rubygems'
require 'bud'

# Assumes no more references will be created. And no more 
# nodes will be added.

class GraphGC
  include Bud

  state do
    sealed :node, [:addr]
    scratch :objects, [:id] => [:obj_val]
    scratch :references, [:ref] => [:id, :other_ref]
    scratch :tombstones, objects.schema 
    
    table :create_obj, [:id] => [:obj_val]
    table :create_ref, [:ref] => [:id, :other_ref]
    table :duplicate_ref, [:ref] => [:id, :other_ref]
    table :all_refs, create_ref.schema
    table :delete_ref, [:ref]
    
    channel :create_obj_chn, [:@addr, :id] => [:obj_val]
    channel :create_ref_chn, [:@addr, :ref] => [:id, :other_ref]
    channel :duplicate_ref_chn, [:@addr, :ref] => [:id, :other_ref]
    channel :delete_ref_chn, [:@addr, :ref] 
  end

  bloom do
    create_obj_chn <~ (node * create_obj).pairs {|n, c| n + c }
    create_obj <= create_obj_chn.payloads
    create_ref <= create_obj {|c| ["orig_ref#{c.id}", c.id, 'original_reference']}
    create_ref_chn <~ (node * create_ref).pairs {|n, c| n + c }
    all_refs <= create_ref { |c| c }
    
    duplicate_ref_chn <~ (node * duplicate_ref).pairs {|n, d| n + d }

    # Only add a reference if the ref we are duplicating from is active
    # This means that we can only add references when other references exist
    # So once there are no longer active references for an object, we can't create new references.
    all_refs <= (duplicate_ref_chn * references).pairs(:other_ref => :ref) { |c, r| [c.ref, c.id, r.ref] }

    delete_ref_chn <~ (node * delete_ref).pairs {|n, d| n + d }
    delete_ref <= delete_ref_chn.payloads

    references <= all_refs.notin(delete_ref, :ref => :ref)
    tombstones <= create_obj.notin(references, :id => :id)
    objects <= create_obj.notin(tombstones, :id => :id)
  end

  def print_references
    puts "references @ #{port}:"
    puts references.map { |v| "\t#{v.ref} => #{v.id}, #{v.other_ref}"}.sort.join("\n")
  end

  def print_objects
    puts "objects @ #{port}"
    puts objects.map {|v| "\t#{v.id} => #{v.obj_val}"}.sort.join("\n")
  end

  def print_tombstones
    puts "tombstones @ #{port}"
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

first = rlist.first
first.create_obj <+ [["1", "object1"], ["2", "object2"]]
# reference5 should not be added.
first.duplicate_ref <+ [["reference5", "1", "ref0"]]
first.duplicate_ref <+ [["reference6", "1", "orig_ref1"]]
# reference7 should not be added.
first.duplicate_ref <+ [["reference7", "1"]]
2.times { first.tick }
first.delete_ref <+ [["reference6"], ["orig_ref1"]]
# reference8 should not be added.
first.duplicate_ref <+ [["reference8", "1", "orig_ref1"]]
first.tick


10.times { sleep 0.1; rlist.each(&:tick) }

puts first.print_tombstones
puts first.print_references
puts first.print_objects
puts "# of created object records: #{first.create_obj.to_a.size}"
puts "# of created reference records: #{first.all_refs.to_a.size}"
puts "# of deleted reference records: #{first.delete_ref.to_a.size}"

rlist.each(&:stop)
