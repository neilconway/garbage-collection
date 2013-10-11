require 'rubygems'
require 'bud'

# A simplified version of CausalDict; we accept a sequence of write requests,
# but don't mark a write as "safe" until its dependencies are safe.
class CausalStore
  include Bud

  state do
    table :log, [:id] => [:key, :deps]
    table :safe_log, log.schema
    table :safe, [:id]
    table :dominated, [:id]
    scratch :pending, log.schema
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema
    scratch :view, log.schema
  end

  bloom do
    pending <= log.notin(safe, :id => :id)
    flat_dep <= pending.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(safe, :dep => :id)
    safe_log <+ pending.notin(missing_dep, :id => :id)
    safe <= safe_log {|l| [l.id]}

    dominated <= (safe_log * safe_log).pairs(:key => :key) do |w1,w2|
      [w2.id] if w1 != w2 and w1.deps.include? w2.id
    end
    view <= safe_log.notin(dominated, :id => :id)
  end

  def print_view
    puts "View:"
    puts view.map {|v| [v.id, v.key]}.sort.inspect
  end
end

s = CausalStore.new(:print_rules => true)
s.tick

s.log <+ [[1, "bar", [5]], [2, "foo", []]]
3.times { s.tick }

s.log <+ [[5, "bar", [2]]]
4.times { s.tick }
s.print_view

puts "# of log entries: #{s.log.to_a.size}"
puts "# of safe_log entries: #{s.safe_log.to_a.size}"

s.stop
