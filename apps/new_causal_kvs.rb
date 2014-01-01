require 'rubygems'
require 'bud'

class CausalHack
  include Bud

  state do
    # Replication
    sealed :node, [:addr]
    channel :log_chn, [:@addr, :id] => [:key, :val]
    channel :dep_chn, [:@addr, :id, :target] => [:src_key, :target_key]
    channel :seal_dep_id_chn, [:@addr, :id]

    table :log, [:id] => [:key]
    table :safe, log.schema
    table :dep, [:id, :target] => [:src_key, :target_key]
    table :safe_dep, dep.schema
    table :dom, [:id]

    range :seal_dep_id, [:id]
    range :safe_keys, [:id]

    scratch :pending, log.schema
    scratch :missing_dep, dep.schema

    scratch :view, safe.schema
  end

  bloom :replication do
    log_chn <~ (node * log).pairs {|n,l| n + l}
    dep_chn <~ (node * dep).pairs {|n,d| n + d}
    seal_dep_id_chn <~ (node * seal_dep_id).pairs {|n,c| n + c}

    log <= log_chn.payloads
    dep <= dep_chn.payloads
    seal_dep_id <= seal_dep_id_chn.payloads
  end

  bloom :safe do
    pending <= log.notin(safe_keys, :id => :id)
    missing_dep <= dep.notin(safe_keys, :target => :id)
    safe <+ (pending * seal_dep_id).lefts(:id => :id).notin(missing_dep, 0 => :id)
    safe_keys <= safe {|s| [s.id]}
  end

  bloom :view do
    safe_dep <= (dep * safe).lefts(:id => :id)
    dom <+ (safe_dep * safe).lefts(:target => :id, :src_key => :key, :target_key => :key) {|d| [d.target]}.notin(dom, 0 => :id)
    view <= safe.notin(dom, :id => :id)
  end

  def print_view
    c = self
    puts "VIEW: #{c.view.to_set.inspect}"
    puts "log: #{c.log.to_a.size}"
    puts "dep: #{c.dep.to_a.size}"
    puts "safe_dep: #{c.safe_dep.to_a.size}"
    puts "dom: #{c.dom.to_a.size}"
    puts "safe: #{c.safe.to_a.size}"
    puts "************"
  end
end

c = CausalHack.new(:print_rules => true)

# Test cases. First, check basic behavior. Writes w/o seal_dep_id shouldn't be
# applied.
c.log <+ [[5, "foo"], [6, "bar"]]
5.times { c.tick }
c.print_view
raise unless c.view.to_a.empty?
raise unless c.safe.to_a.empty?

# Both writes have no deps => applied to view.
c.seal_dep_id <+ [[5], [6]]
5.times { c.tick }
c.print_view
raise unless c.safe.to_a.size == 2
raise unless c.view.to_a.size == 2
raise unless c.log.to_a.empty?

# Write 8 dominates write 5, but no seal_dep => 5 remains in view.
c.log <+ [[8, "foo"]]
c.dep <+ [[8, 5, "foo", "foo"]]
5.times { c.tick }
c.print_view
raise unless c.safe.to_a.size == 2
raise unless c.view.to_a.size == 2
raise unless c.log.to_a.size == 1
raise unless c.dep.to_a.size == 1
raise unless c.safe_dep.to_a.empty?

# Seal deps for 8 => 8 now dominates 5.
c.seal_dep_id <+ [[8]]
5.times { c.tick }
c.print_view
raise unless c.safe.to_a.size == 2
raise unless c.view.to_a.size == 2
raise unless c.log.to_a.empty?
raise unless c.dep.to_a.empty?
raise unless c.safe_dep.to_a.empty?
raise unless c.dom.to_a.empty?

# Check that dependencies that do not result in dominating a write are still
# reclaimed.
c.dep <+ [[9, 8, "baz", "foo"], [9, 6, "baz", "bar"]]
c.seal_dep_id <+ [[9]]
5.times { c.tick }
c.print_view
raise unless c.safe.to_a.size == 2
raise unless c.view.to_a.size == 2
raise unless c.log.to_a.empty?
raise unless c.dom.to_a.empty?
raise unless c.safe_dep.to_a.empty?
raise unless c.dep.to_a.size == 2

c.log <+ [[9, "baz"]]
5.times { c.tick }
c.print_view
puts "SAFE DEP: #{c.safe_dep.to_a.sort.inspect}"
raise unless c.safe.to_a.size == 3
raise unless c.view.to_a.size == 3
raise unless c.log.to_a.empty?
raise unless c.dep.to_a.empty?
raise unless c.safe_dep.to_a.empty?
raise unless c.dom.to_a.empty?

# Concurrent writes to the same key
c.log <+ [[10, "baz"]]
c.seal_dep_id <+ [[10]]
5.times { c.tick }
c.print_view
raise unless c.safe.to_a.size == 4
raise unless c.view.to_a.size == 4
raise unless c.log.to_a.empty?
raise unless c.dep.to_a.empty?
raise unless c.safe_dep.to_a.empty?
raise unless c.dom.to_a.empty?

# Other test cases: concurrent writes.

# c.log <+ [[8, "foo"]]
# c.dep <+ [[8, 7, "foo", "foo"], [8, 6, "foo", "bar"]]
# c.seal_dep_id <+ [[8]]

# 6.times { c.tick }
# c.print_view
