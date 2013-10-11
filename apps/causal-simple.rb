require 'rubygems'
require 'bud'

# A simplified version of CausalDict; we accept a sequence of write requests,
# but don't mark a write as "safe" until its dependencies are safe.
class CausalStore
  include Bud

  state do
    table :log, [:id] => [:key, :deps]
    table :safe_log, log.schema
    table :done, [:id]
    scratch :pending, log.schema
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema
  end

  bloom do
    pending <= log.notin(done, :id => :id)
    flat_dep <= pending.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(done, :dep => :id)
    safe_log <+ pending.notin(missing_dep, :id => :id)
    done <= safe_log {|l| [l.id]}
  end
end

s = CausalStore.new(:print_rules => true)
s.tick
s.print_live

s.log <+ [[1, "bar", [5]], [2, "foo", []]]
3.times { s.tick }

s.log <+ [[5, "baz", [2]]]
4.times { s.tick }

puts "# of log entries: #{s.log.to_a.size}"
puts "# of safe_log entries: #{s.safe_log.to_a.size}"

s.stop
