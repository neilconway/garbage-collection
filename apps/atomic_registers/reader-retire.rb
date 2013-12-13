require 'rubygems'
require 'bud'

class ReaderRetire
  include Bud

  state do
    sealed :obj, [:id] => [:val]
    sealed :reader, [:id]
    table :reader_done, reader.schema
    scratch :visible, [:reader, :obj] => [:val]
  end

  bloom do
    visible <= ((reader * obj).pairs {|r,o| r + o}).notin(reader_done, 0 => :id)
  end
end

r = ReaderRetire.new
r.obj <+ [[1, "foo"], [2, "bar"]]
r.reader <+ [[100], [101]]
r.tick
puts r.visible.to_a.sort.inspect

r.reader_done <+ [[100]]
r.tick
puts r.visible.to_a.sort.inspect

r.reader_done <+ [[101]]
2.times { r.tick }
puts r.visible.to_a.sort.inspect

puts "# of objects: #{r.obj.to_a.size}"
puts "# of readers: #{r.reader.to_a.size}"
