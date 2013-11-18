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
  end

  bloom do
    # Check write dependencies. We can declare that a write is "safe" when all
    # of the write's dependencies are safe and the dependency list has been
    # sealed.
    missing_dep <= dep.notin(safe_log, 1 => :id)
    safe_log <+ (log * log_commit).lefts(:id => :id).notin(missing_dep, 0 => :id)
  end
end

n = NormalCausalKvsReplica.new
n.log <+ [[5, "foo", "bar"]]
2.times { n.tick }
puts n.safe_log.to_a.inspect

n.log_commit <+ [[5]]
2.times { n.tick }
puts n.safe_log.to_a.inspect

n.log <+ [[6, "foo", "baz"]]
n.dep <+ [[6, 5]]
2.times { n.tick }
puts n.safe_log.to_a.inspect

n.log_commit <+ [[6]]
2.times { n.tick }
puts n.safe_log.to_a.inspect

n.log <+ [[7, "foo", "qux"], [8, "foo", "qux2"]]
n.dep <+ [[8, 7]]
n.log_commit <+ [[8]]
2.times { n.tick }
puts n.safe_log.to_a.inspect
