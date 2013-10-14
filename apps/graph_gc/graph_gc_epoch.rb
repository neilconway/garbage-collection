require 'rubygems'
require 'bud'

class EpochGc
  include Bud

  state do
    sealed :node, [:addr]

    table :obj, [:id] => [:val, :epoch]
    table :ref, [:id] => [:name, :obj_id, :epoch]
    table :del_ref, [:id] => [:del_id]

    channel :obj_chn, [:@addr, :id] => [:val, :epoch]
    channel :ref_chn, [:@addr, :id] => [:name, :obj_id, :epoch]
    channel :del_ref_chn, [:@addr, :id] => [:del_id] 

    scratch :view, [:ref_id, :name, :val]
  end

  bloom do
    obj_chn <~ (node * obj).pairs {|n, c| n + c }
    obj <= obj_chn.payloads

    ref_chn <~ (node * ref).pairs {|n, c| n + c }
    ref <= ref_chn.payloads

    del_ref_chn <~ (node * del_ref).pairs {|n, d| n + d }
    del_ref <= del_ref_chn.payloads

    view <= ((ref * obj).pairs(:epoch => :epoch, :obj_id => :id) {|r,o| [r.id, r.name, o.val]}).notin(del_ref, 0 => :del_id)
  end

  def print_view
    puts "View:"
    puts view.map {|v| "\t#{v.name} => #{v.val}"}
  end

  def print_obj
    puts "Objs:"
    puts obj.map {|v| "\t#{v.id} => #{v.val}"}
  end
end

ports = (1..3).map {|i| i + 10001}
addrs = ports.map {|p| "localhost:#{p}"}
rlist = ports.map {|p| EpochGc.new(:ip => "localhost", :port => p)}
rlist.each do |r|
  r.node <+ addrs.map {|a| [a]}
  r.tick
end
rlist.each(&:run_bg)

s = rlist.first
s.obj <+ [[1, 'foo', 'e1'], [2, 'bar', 'e1'], [3, 'xxx', 'e2']]
s.ref <+ [[10, 'k1', 1, 'e1'], [11, 'k1b', 1, 'e1'], [12, 'k2', 2, 'e1'], [13, 'k3', 3, 'e2']]
s.tick
s.print_view

s.del_ref <+ [[1, 10], [2, 12]]
2.times { s.tick }
s.seal_ref_epoch <+ [["e1"]]
2.times { s.tick }
s.print_view
s.del_ref <+ [[3,13]]
2.times { s.tick }
s.seal_ref_epoch <+ [["e2"]]
2.times {s.tick}
s.print_view
s.print_obj



puts "Size of obj: #{s.obj.to_a.size}"
puts "Size of ref: #{s.ref.to_a.size}"
puts "Size of del_ref: #{s.del_ref.to_a.size}"

s.stop
