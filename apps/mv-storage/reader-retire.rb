require 'rubygems'
require 'bud'

class ReaderRetire
  include Bud

  state do
    sealed :object, [:id] => [:val]
    table :reader, [:id]
    table :reader_done, reader.schema
    scratch :live_r, reader.schema
    scratch :visible, [:reader, :obj] => [:val]
  end

  bloom do
    live_r <= reader.notin(reader_done)
    visible <= (live_r * object).pairs {|r,o| r + o}
  end
end

r = ReaderRetire.new
r.object <+ [[1, "foo"], [2, "bar"]]
r.reader <+ [[100], [101]]
r.tick
puts r.visible.to_a.sort.inspect

r.reader_done <+ [[100]]
r.tick
puts r.visible.to_a.sort.inspect

r.reader_done <+ [[101]]
r.tick
puts r.visible.to_a.sort.inspect

puts "# of objects: #{r.object.to_a.size}"
puts "# of readers: #{r.reader.to_a.size}"
