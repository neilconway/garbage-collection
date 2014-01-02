require 'rubygems'
require 'bud'

class CausalHack
  include Bud

  state do
    # Replication state
    sealed :node, [:addr]
    channel :log_chn, [:@addr, :id] => [:key, :val]
    channel :dep_chn, [:@addr, :id, :target] => [:src_key, :target_key]
    channel :seal_dep_id_chn, [:@addr, :id]

    # Representation of write operations
    table :log, [:id] => [:key, :val]
    table :dep, [:id, :target]
    range :seal_dep_id, [:id]

    table :safe, log.schema
    range :safe_keys, [:id]
    table :safe_dep, [:id, :target] => [:src_key]
    table :dom, [:id]

    scratch :pending, log.schema
    scratch :missing_dep, dep.schema
    scratch :view, safe.schema

    # Read request/response protocol
    channel :req_chn, [:@addr, :id] => [:key]
    channel :req_dep_chn, [:@addr, :id, :target]
    channel :req_seal_dep_id_chn, [:@addr, :id]
    channel :resp_chn, [:@addr, :id, :key, :val]

    # Server-side read state
    table :read_buf, [:id] => [:key, :deps, :src_addr]
    table :read_dep, [:id, :target]
    table :seal_read_dep_id, [:id]
    scratch :read_pending, read_buf.schema
    scratch :missing_read_dep, read_dep.schema
    scratch :safe_read, read_buf.schema
    table :read_resp, resp_chn.schema

    # Client-side read state
    table :read_req, req_chn.schema
    table :read_req_dep, req_dep_chn.schema
    table :read_req_seal_dep_id, req_seal_dep_id_chn.schema
    table :read_result, resp_chn.schema
  end

  bloom :replication do
    log_chn <~ (node * log).pairs {|n,l| n + l}
    dep_chn <~ (node * dep).pairs {|n,d| n + d}
    seal_dep_id_chn <~ (node * seal_dep_id).pairs {|n,c| n + c}

    log <= log_chn.payloads
    dep <= dep_chn.payloads
    seal_dep_id <= seal_dep_id_chn.payloads
  end

  bloom :check_safe do
    pending <= log.notin(safe_keys, :id => :id)
    missing_dep <= dep.notin(safe_keys, :target => :id)
    safe <+ (pending * seal_dep_id).lefts(:id => :id).notin(missing_dep, 0 => :id)
    safe_keys <= safe {|s| [s.id]}
  end

  bloom :live_view do
    # A safe_log entry e for key k is dominated if there is another safe_log
    # entry e' for k s.t. e happens-before e'. However, implementing this
    # correctly (w/o any further assumptions) would mean we couldn't safely
    # garbage collect any dependency metadata, because we might always see a
    # subsequent write that depends on a earlier part of the dependency
    # graph. Hence, we make a simplifying assumption: a safe_log entry e for key
    # k includes a dependency on e', the most recent previous version of k that
    # the client was aware of.
    safe_dep <= (dep * safe).pairs(:id => :id) {|d,s| d + [s.key]}
    dom <+ (safe_dep * safe).lefts(:target => :id, :src_key => :key) {|d| [d.target]}.notin(dom, 0 => :id)
    view <= safe.notin(dom, :id => :id)
  end

  bloom :read_server do
    read_buf <= req_chn {|r| [r.id, r.key, r.source_addr]}
    read_dep <= req_dep_chn {|d| [d.id, d.target]}
    seal_read_dep_id <= req_seal_dep_id_chn {|s| [s.id]}

    read_pending <= read_buf.notin(read_resp, :id => :id)
    missing_read_dep <= read_dep.notin(safe_keys, :target => :id)
    safe_read <= (read_pending * seal_read_dep_id).lefts(:id => :id).notin(missing_read_dep, 0 => :id)
    read_resp <+ (safe_read * view).pairs(:key => :key) {|r,v| [r.src_addr, r.id, r.key, r.val]}
    resp_chn <~ read_resp
  end

  bloom :read_client do
    req_chn <~ read_req
    req_dep_chn <~ read_req_dep
    req_seal_dep_id_chn <~ read_req_seal_dep_id

    read_result <= resp_chn
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
c.dep <+ [[8, 5]]
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
c.dep <+ [[9, 8], [9, 6]]
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

# c.log <+ [[8, "foo"]]
# c.dep <+ [[8, 7, "foo", "foo"], [8, 6, "foo", "bar"]]
# c.seal_dep_id <+ [[8]]

# 6.times { c.tick }
# c.print_view
