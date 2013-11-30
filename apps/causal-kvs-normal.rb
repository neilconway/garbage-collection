require 'rubygems'
require 'bud'

class NormalCausalKvsReplica
  include Bud

  state do
    # Replication
    sealed :node, [:addr]
    channel :log_chn, [:@addr, :id] => [:key, :val]
    channel :dep_chn, [:@addr, :id, :target]
    channel :log_commit_chn, [:@addr, :id]

    # Write operations
    table :log, [:id] => [:key, :val]
    table :safe_log, log.schema

    # Write dependencies; each write depends on zero or more other writes
    table :dep, [:id, :target]

    # Seals for dependency lists
    range :log_commit, [:id]

    # The set of dominated write IDs; we use DR- to reclaim from this.
    table :dominated, [:id]

    scratch :missing_dep, dep.schema
    scratch :pending_log, log.schema

    scratch :same_key, [:w1, :w2]
    scratch :view, log.schema
  end

  bloom :replication do
    log_chn <~ (node * log).pairs {|n,l| n + l}
    dep_chn <~ (node * dep).pairs {|n,d| n + d}
    log_commit_chn <~ (node * log_commit).pairs {|n,c| n + c}

    log <= log_chn.payloads
    dep <= dep_chn.payloads
    log_commit <= log_commit_chn.payloads
  end

  bloom :safe do
    # Check write dependencies. We can declare that a write is "safe" when all
    # of the write's dependencies are safe and the dependency list has been
    # sealed.
    # XXX: Would be nice to avoid the explicit negation against safe_log when
    # defining pending_log.
    pending_log <= log.notin(safe_log)
    missing_dep <= dep.notin(safe_log, :target => :id)
    safe_log <+ (pending_log * log_commit).lefts(:id => :id).notin(missing_dep, 0 => :id)
  end

  bloom :view do
    same_key <= (safe_log * safe_log).pairs(:key => :key) {|w1,w2| [w1.id, w2.id] if w1 != w2}
    dominated <= (same_key * dep).rights(:w1 => :id, :w2 => :target) {|d| [d.target]}
    view <= safe_log.notin(dominated, :id => :id)
  end

  def print_state
    puts "safe_log: #{safe_log.to_a.inspect}"
    puts "view: #{view.to_a.inspect}"
    puts "same_key: #{same_key.to_a.inspect}"
    puts "dominated: #{dominated.to_a.inspect}"
    puts "log: #{log.to_a.inspect}"
    puts "dep: #{dep.to_a.inspect}"
    puts "=========="
  end
end

n = NormalCausalKvsReplica.new(:disable_rse => false)
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
