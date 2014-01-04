require 'rubygems'
require 'bud'

class CausalKvsReplica
  include Bud

  state do
    # Replication state
    sealed :node, [:addr]
    channel :log_chn, [:@addr, :id] => [:key, :val, :deps]

    # Representation of write operations
    table :log, [:id] => [:key, :val, :deps]
    table :safe, [:id] => [:key, :val]
    table :dep, [:id, :target]
    range :seal_dep_id, [:id]
    range :safe_keys, [:id]
    table :safe_dep, [:target, :src_key]
    table :dom, [:id]

    scratch :pending, log.schema
    scratch :missing_dep, dep.schema
    scratch :view, safe.schema

    # Read request/response protocol
    channel :req_chn, [:@addr, :id] => [:key, :deps]
    channel :resp_chn, [:@addr, :id, :key, :val]

    # Server-side read state
    table :read_buf, [:id] => [:key, :deps, :src_addr]
    scratch :read_pending, read_buf.schema
    scratch :read_dep, [:id, :target]
    scratch :missing_read_dep, read_dep.schema
    scratch :safe_read, read_buf.schema
    table :read_resp, resp_chn.schema

    # Client-side read state
    table :read_req, req_chn.schema
    table :read_result, resp_chn.schema
  end

  bloom :replication do
    log_chn <~ (node * log).pairs {|n,l| n + l}
    log <= log_chn.payloads
  end

  bloom :check_safe do
    dep <= log.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    seal_dep_id <= log {|l| [l.id]}
    pending <= log.notin(safe_keys, :id => :id)
    missing_dep <= dep.notin(safe_keys, :target => :id)
    safe <+ pending.notin(missing_dep, 0 => :id).pro {|p| [p.id, p.key, p.val]}
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
    safe_dep <= (dep * safe).pairs(:id => :id) {|d,s| [d.target, s.key]}
    dom <+ (safe_dep * safe).lefts(:target => :id, :src_key => :key) {|d| [d.target]}.notin(dom, 0 => :id)
    view <= safe.notin(dom, :id => :id)
  end

  bloom :read_server do
    read_buf <= req_chn {|r| [r.id, r.key, r.deps, r.source_addr]}
    read_pending <= read_buf.notin(read_resp, :id => :id)
    read_dep <= read_pending.flat_map {|r| r.deps.map {|d| [r.id, d]}}
    missing_read_dep <= read_dep.notin(safe_keys, :target => :id)
    safe_read <+ read_pending.notin(missing_read_dep, 0 => :id)
    read_resp <= (safe_read * view).pairs(:key => :key) {|r,v| [r.src_addr, r.id, r.key, v.val]}
    resp_chn <~ read_resp
  end

  bloom :read_client do
    req_chn <~ read_req
    read_result <= resp_chn
  end

  def get_safe
    self.safe.map {|s| [s.id, s.key, s.val]}.to_set
  end

  def get_view
    self.view.map {|v| [v.id, v.key, v.val]}.to_set
  end

  def do_write(id, key, val, write_deps=[])
    self.log <+ [[id, key, val, write_deps]]
  end

  def do_read(addr, id, key, read_deps=[])
    self.read_req <+ [[addr, id, key, read_deps]]
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

c = CausalKvsReplica.new(:print_state => true, :print_rules => true, :disable_rse_opt => true)
