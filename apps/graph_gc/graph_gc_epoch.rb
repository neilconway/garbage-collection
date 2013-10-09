require 'rubygems'
require 'bud'

class EpochGc
  include Bud

  state do
    sealed :node, [:addr]

    table :obj, [:id] => [:val, :epoch]
    table :ref, [:id] => [:name, :obj_id, :epoch]
    table :del_ref, [:id] => [:del_id]

    table :ins_obj, obj.schema
    table :ins_ref, ref.schema

    channel :obj_chn, [:@addr, :id] => [:val, :epoch]
    channel :ref_chn, [:@addr, :id] => [:name, :obj_id, :epoch]
    channel :del_ref_chn, [:@addr, :id] => [:del_id] 

    scratch :view, [:ref_id, :name, :val]
  end

  bloom do
    obj_chn <~ (node * obj).pairs {|n, c| n + c }
    obj <= obj_chn.payloads
    obj <= ins_obj
    
    ref_chn <~ (node * ref).pairs {|n, c| n + c }
    ref <= ref_chn.payloads
    ref <= ins_ref

    del_ref_chn <~ (node * del_ref).pairs {|n, d| n + d }
    del_ref <= del_ref_chn.payloads

    view <= ((ref * obj).pairs(:epoch => :epoch, :obj_id => :id) {|r,o| [r.id, r.name, o.val]}).notin(del_ref, 0 => :del_id)
  end

  def print_view
    puts "View:"
    puts view.map {|v| "\t#{v.name} => #{v.val}"}
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| EpochGc.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end

s = rlist.first
s.ins_obj <+ [[1, 'foo', 'e1'], [2, 'bar', 'e2'], [3, 'baz', 'e2']]
s.ins_ref <+ [[10, 'k1', 1, 'e1'], [11, 'k1b', 1, 'e1'], [12, 'k2', 2, 'e2']]
s.tick
s.print_view

s.del_ref <+ [[1, 10], [2, 12]]
2.times { s.tick }
s.seal_ins_obj_epoch <+ [["e1"]]
s.seal_ins_ref_epoch <+ [["e1"]]
2.times { s.tick }
s.print_view
s.seal_ins_obj_epoch <+ [["e2"]]
s.seal_ins_ref_epoch <+ [["e2"]]
2.times {s.tick}
s.print_view


puts "Size of obj: #{s.obj.to_a.size}"
puts "Size of ref: #{s.ref.to_a.size}"
puts "Size of del_ref: #{s.del_ref.to_a.size}"

s.stop
