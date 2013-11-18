require 'rubygems'
require 'bud'

class NormalCausalKvsReplica
  include Bud

  state do
    # Write operations
    table :log, [:id] => [:key, :val]
    table :safe_log, log.schema

    # Write dependencies; each write depends on zero or more other writes
    table :dep, [:id, :target]

    # Seals for dependency lists
    range :log_commit, [:id]

    scratch :missing_dep, dep.schema

    scratch :same_key, [:w1, :w2]
    scratch :dominated, [:id]
    scratch :view, log.schema
  end

  bloom :safe do
    # Check write dependencies. We can declare that a write is "safe" when all
    # of the write's dependencies are safe and the dependency list has been
    # sealed.
    missing_dep <= dep.notin(safe_log, 1 => :id)
    safe_log <+ (log * log_commit).lefts(:id => :id).notin(missing_dep, 0 => :id)
  end

  bloom :view do
    same_key <= (safe_log * safe_log).pairs(:key => :key) {|w1,w2| [w1.id, w2.id] if w1 != w2}
    dominated <= (same_key * dep).lefts(:w1 => :id, :w2 => :target) {|w| [w.w2]}
    view <= safe_log.notin(dominated, :id => :id)
  end

  def print_state
    puts "safe_log: #{safe_log.to_a.inspect}"
    puts "view: #{view.to_a.inspect}"
    puts "same_key: #{same_key.to_a.inspect}"
    puts "dominated: #{dominated.to_a.inspect}"
    puts "=========="
  end
end

n = NormalCausalKvsReplica.new(:disable_rse => true)
n.log <+ [[5, "foo", "bar"]]
2.times { n.tick }
n.print_state

n.log_commit <+ [[5]]
2.times { n.tick }
n.print_state

n.log <+ [[6, "foo", "baz"]]
n.dep <+ [[6, 5]]
2.times { n.tick }
n.print_state

n.log_commit <+ [[6]]
2.times { n.tick }
n.print_state

n.log <+ [[7, "foo", "qux"], [8, "foo", "qux2"]]
n.dep <+ [[8, 7]]
n.log_commit <+ [[8]]
2.times { n.tick }
n.print_state
