require 'rubygems'
require 'bud'

class SealedRefGc
  include Bud

  state do
    sealed :obj, [:id] => [:val]
    sealed :ref, [:id] => [:name, :obj_id]
    table :del_ref, [:id] => [:del_id]

    scratch :view, [:ref_id, :name, :val]
  end

  bloom do
    view <= ((ref * obj).pairs(:obj_id => :id) {|r,o| [r.id, r.name, o.val]}).notin(del_ref, 0 => :del_id)
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
