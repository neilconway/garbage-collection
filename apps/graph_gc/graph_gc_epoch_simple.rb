require 'rubygems'
require 'bud'

class SealedRefGc
  include Bud

  state do
    table :obj, [:id] => [:val, :epoch]
    table :ref, [:id] => [:name, :obj_id, :epoch]
    table :del_ref, [:id] => [:del_id]

    scratch :view, [:ref_id, :name, :val]
  end

  bloom do
    view <= ((ref * obj).pairs(:epoch => :epoch, :obj_id => :id) {|r,o| [r.id, r.name, o.val]}).notin(del_ref, 0 => :del_id)
  end

  def print_view
    puts "View:"
    puts view.map {|v| "\t#{v.name} => #{v.val}"}
  end
end

s = SealedRefGc.new
s.obj <+ [[1, 'foo', 'e1'], [2, 'bar', 'e2'], [3, 'baz', 'e2']]
s.ref <+ [[10, 'k1', 1, 'e1'], [11, 'k1b', 1, 'e1'], [12, 'k2', 2, 'e2']]
s.tick
s.print_view

s.del_ref <+ [[1, 10], [2, 12]]
2.times { s.tick }
s.seal_obj_epoch <+ [["e1"]]
s.seal_ref_epoch <+ [["e1"]]
2.times { s.tick }
s.print_view
s.seal_obj_epoch <+ [["e2"]]
s.seal_ref_epoch <+ [["e2"]]
2.times {s.tick}
s.print_view


puts "Size of obj: #{s.obj.to_a.size}"
puts "Size of ref: #{s.ref.to_a.size}"
puts "Size of del_ref: #{s.del_ref.to_a.size}"

s.stop
