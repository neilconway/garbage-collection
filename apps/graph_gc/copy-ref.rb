require 'rubygems'
require 'bud'

class SealedRefGc
  include Bud

  state do
    table :obj, [:id] => [:val]
    sealed :ref, [:id] => [:name, :obj_id]
    table :del_ref, [:id] => [:del_id]

    scratch :view, [:ref_id, :name, :obj_id, :val]
    scratch :copy_ref, [:id, :name, :src_ref]
  end

  bloom do
    view <= ((ref * obj).pairs(:obj_id => :id) {|r,o| [r.id, r.name] + o}).notin(del_ref, 0 => :del_id)

    ref <= (copy_ref * view).pairs(:src_ref => :name) {|cr,v| [cr.id, cr.name, v.obj_id]}
  end

  def print_view
    puts "View:"
    puts view.map {|v| "\t#{v.name} => #{v.val}"}
  end
end

s = SealedRefGc.new
s.obj <+ [[1, 'foo'], [2, 'bar']]
s.ref <+ [[10, 'k1', 1], [11, 'k1b', 1], [12, 'k2', 2]]
s.tick
s.print_view

s.del_ref <+ [[1, 10], [2, 12]]
2.times { s.tick }
s.print_view

puts "Size of obj: #{s.obj.to_a.size}"
puts "Size of ref: #{s.ref.to_a.size}"
puts "Size of del_ref: #{s.del_ref.to_a.size}"

s.stop
