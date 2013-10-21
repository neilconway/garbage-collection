require 'rubygems'
require 'bud'

CURRENT_EPOCH = 1

class SealedRefGc
  include Bud

  state do
    scratch :ins_obj, [:id] => [:val]
    scratch :ins_ref, [:id] => [:name, :obj_id]

    table :obj, [:id, :epoch] => [:val]
    table :ref, [:id, :epoch] => [:name, :obj_id]
    table :del_ref, [:id] => [:del_id]

    scratch :view, [:ref_id, :obj_id, :name, :val]
  end

  bloom :gc do
    obj <= ins_obj { |i| [i.id, CURRENT_EPOCH, i.val] }
    ref <= ins_ref { |i| [i.id, CURRENT_EPOCH, i.name, i.obj_id] }

    view <= ((ref * obj).pairs(:epoch => :epoch, :obj_id => :id) {|r,o| [r.id, o.id, r.name, o.val]}).notin(del_ref, 0 => :del_id)

    ins_obj <+ view {|v| [v.obj_id, v.val]}
    ins_ref <+ view {|v| [v.ref_id, v.name, v.obj_id]}
  end

  bloom :move_to_new do

  end

  def print_view
    puts "View:"
    puts view.map {|v| "\t#{v.name} => #{v.val}"}
  end

  def print_obj
    puts "Objs"
    puts obj.map {|v| "\t#{v.id} => #{v.epoch}, #{v.val}"}
  end

end

s = SealedRefGc.new

s.ins_obj <+ [[1, 'foo'], [2, 'bar'], [3, 'baz']]
s.ins_ref <+ [[10, 'k1', 1], [11, 'k1b', 1], [12, 'k2', 2]]
s.tick
s.print_view
s.print_obj

s.del_ref <+ [[1, 10], [2, 12]]
2.times { s.tick }
s.seal_obj_epoch <+ [[1]]
s.seal_ref_epoch <+ [[1]]
CURRENT_EPOCH += 1
2.times { s.tick }
s.print_view
s.print_obj
s.seal_obj_epoch <+ [[2]]
s.seal_ref_epoch <+ [[2]]
CURRENT_EPOCH += 1
2.times {s.tick}
s.print_view
s.print_obj


puts "Size of obj: #{s.obj.to_a.size}"
puts "Size of ref: #{s.ref.to_a.size}"
puts "Size of del_ref: #{s.del_ref.to_a.size}"

s.stop
