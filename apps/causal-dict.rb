require 'rubygems'
require 'bud'

# It would make sense to split the client code into a separate class and move
# the read channel state into a shared module. However, the current analysis is
# per-class, so this would prevent doing RCE/RSE on the read protocol.
class CausalDict
  include Bud

  state do
    # Replication state
    sealed :node, [:addr]
    channel :chn, [:@addr, :id] => [:key, :val, :deps]
    table :log, [:id] => [:key, :val, :deps]

    # State for tracking causal dependencies and pending write operations
    table :safe_log, log.schema
    range :safe, [:id]
    scratch :pending, log.schema
    scratch :flat_dep, [:id, :dep]
    scratch :missing_dep, flat_dep.schema

    # State for computing the current KVS view
    table :dominated, [:id]
    scratch :view, log.schema

    # Protocol for read request/response
    channel :req_chn, [:@addr, :id] => [:key, :deps]
    channel :resp_chn, [:@addr, :id, :key, :val]

    # Server-side read state
    table :read_buf, [:id] => [:key, :deps, :src_addr]
    scratch :read_pending, read_buf.schema
    scratch :read_dep, [:id, :dep]
    scratch :missing_read_dep, read_dep.schema
    scratch :safe_read, read_buf.schema
    table :read_resp, resp_chn.schema
    range :done_read, [:id]

    # Client-side read state
    table :read_req, req_chn.schema
    table :read_result, resp_chn.schema
  end

  bloom :replication do
    chn <~ (node * log).pairs {|n,l| n + l}
    log <= chn.payloads
  end

  bloom :check_deps do
    # Compute "safe" log entries; a log entry is safe if all of its dependencies
    # are safe. (This has a cycle through negation but we use <+ to temporally
    # stratify it. If we added support for constraint stratification, we could
    # probably use the fact that dependencies are a partial order to constraint
    # stratify.)
    #
    # We can discard an entry from "log" when it has been delivered to all nodes
    # and its dependencies have been satisfied.
    #
    # XXX: Tracking the set of "safe" IDs as well as "safe_log" is unfortunate.
    pending <= log.notin(safe, :id => :id)
    flat_dep <= pending.flat_map {|l| l.deps.map {|d| [l.id, d]}}
    missing_dep <= flat_dep.notin(safe, :dep => :id)
    safe_log <+ pending.notin(missing_dep, :id => :id)
    safe <= safe_log {|l| [l.id]}
  end

  bloom :active_view do
    # A safe_log entry e for key k is dominated if there is another safe_log
    # entry e' for k s.t. e happens-before e'. However, implementing this
    # correctly (w/o any further assumptions) would mean we couldn't safely
    # garbage collect any dependency metadata, because we might always see a
    # subsequent write that depends on a earlier part of the dependency
    # graph. Hence, we make a simplifying assumption: a safe_log entry e for key
    # k includes a dependency on e', the most recent previous version of k that
    # the client was aware of. Hence, we can say that a safe_log entry is
    # dominated if there is another entry for the same key that includes this
    # entry in its list of dependencies.
    dominated <= (safe_log * safe_log).pairs(:key => :key) do |w1,w2|
      [w2.id] if w1 != w2 and w1.deps.include? w2.id
    end
    view <= safe_log.notin(dominated, :id => :id)
  end

  bloom :read_server do
    # XXX: there should be a cleaner way to write this. We'd like to say that
    # "read_pending is the delta between read_resp and read_buf".
    read_buf <= req_chn {|r| [r.id, r.key, r.deps, r.source_addr]}
    read_pending <= read_buf.notin(done_read, :id => :id)
    read_dep <= read_pending.flat_map {|r| r.deps.map {|d| [r.id, d]}}
    missing_read_dep <= read_dep.notin(safe, :dep => :id)
    safe_read <= read_pending.notin(missing_read_dep, :id => :id)
    read_resp <= (safe_read * view).pairs(:key => :key) {|r,v| [r.src_addr, r.id, r.key, v.val]}
    done_read <+ read_resp {|r| [r.id]}
    resp_chn <~ read_resp
  end

  bloom :read_client do
    req_chn <~ read_req
    read_result <= resp_chn
  end

  def print_view
    puts "View @ #{port}:"
    puts view.map {|v| "\t#{v.key} => #{v.val}"}.sort.join("\n")
  end
end
