require 'rubygems'
require 'bud'

module Dependencies
  state do
    range :write_done, [:key, :prev_xid]
  end
  bloom do
    write_done <= write_log{|l| [l.key, l.prev_xid]}
    live <= write_log.notin(write_done, :xid => :prev_xid, :key => :key)
  end
end

module SimpleMV
  # the simplest possible multivalued store
  include Dependencies
  state do
    # inputs
    table :write, [:xid] => [:key, :val]
    # internal state
    table :write_log, [:xid] => [:key, :val, :prev_xid]
    # views
    scratch :write_event, write.schema
    scratch :live, write_log.schema
  end

  bloom do
    write_event <= write.notin(write_log, :xid => :xid)
    write_log <+ (write_event * live).pairs(:key => :key){|e, l| [e.xid, e.key, e.val, l.xid]}
  end
end

module SerialWriteConstraint
  # enforce the constraint that at any time, at most one transaction (performing all its writes at once)
  # writes to any given key.
  state do
    scratch :write_commit_constraint, [:key] => [:xid]
  end
  bloom :constraint do
    write_commit_constraint <= write_commit_event{|e| [e.key, e.xid]}
  end
end

module MultiKeyWrites
  include Dependencies
  include SerialWriteConstraint
  state do
    # inputs
    table :write, [:xid, :key] => [:val]
    table :commit, [:xid]
    # internal state
    table :write_log, [:xid, :key] => [:val, :prev_xid]
    # views
    scratch :live, write_log.schema
    scratch :write_commit_event, write.schema
  end

  bloom do
    write_commit_event <= (write * commit).pairs(:xid => :xid){|w,s| w}.notin(write_log, 0 => :xid)
    write_log <+ (write_commit_event * live).pairs(:key => :key){|e, l| [e.xid, e.key, e.val, l.xid]}
  end
end

module ReadTabs
  # separated because reused in dev.rb
  state do
    # input
    table :read, [:xid, :key]
    table :read_commit, [:xid]
  
    # internal
    table :pinned_writes, [:effective, :xid, :key, :val, :prev_xid]
    range :read_commit_log, [:xid]
  
    scratch :read_event, read.schema
    scratch :read_commit_event, read.schema
    scratch :read_view, pinned_writes.schema
  end
end

module SimplerMultiKeyReads
  include MultiKeyWrites
  include ReadTabs
  state do
    range :ever_pinned, [:xid]
  end
  bloom do
    ever_pinned <= pinned_writes{|w| [w.effective]}
    read_event <= read.notin(ever_pinned, :xid => :xid)
    pinned_writes <+ (read_event * live).pairs{|r, l| [r.xid] + l.to_a}
    read_view <= pinned_writes.notin(read_commit_log, :effective => :xid)
    read_commit_log <+ read_commit{|c| [c.xid]}
  end
end
