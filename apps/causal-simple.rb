require 'rubygems'
require 'bud'

# A simplified version of CausalDict; we accept a sequence of write requests
# that are used to compute the current view, but a write is not applied until
# its (single) dependency has also been observed. We also ignore the read path
# and don't do replication.
class CausalStore
  include Bud

  state do
    table :log, [:id] => [:key, :dep]
    table :safe_log, log.schema
    scratch :live, log.schema
  end

  bootstrap do
    safe_log <+ [[0, "foo", nil]]
  end

  bloom do
    safe_log <= (log * safe_log).lefts(:dep => :id)
    live <= safe_log.notin(safe_log, :id => :dep)
  end

  def print_live
    puts "Live objects:"
    puts live.map {|l| "\t#{l.id} => #{l.key}"}.sort
  end
end

s = CausalStore.new
s.tick
s.print_live

s.log <+ [[1, "bar", 0]]
s.tick
s.print_live

puts "# of log entries: #{s.log.to_a.size}"
puts "# of safe_log entries: #{s.safe_log.to_a.size}"

s.stop
