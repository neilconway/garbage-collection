require 'rubygems'
require 'bud'

module Dependencies
  bloom do
    foo <= write_log
    live <= write_log.notin(foo, :xid => :prev_xid, :key => :key)
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
    # workaround
    scratch :foo, write_log.schema
  end

  bloom do
    write_event <= write.notin(write_log, :xid => :xid)
    # N.B. there is only ever one row per key in live(); hence the new version
    # can always be simply the successor of the parent version's dependencies.
    # assume: keys are already initialized
    write_log <+ (write_event * live).pairs(:key => :key){|e, l| [e.xid, e.key, e.val, l.xid]}
  end
end

module MultiKeyWrites
  include Dependencies
  state do
    # inputs
    table :write, [:xid, :key] => [:val]
    table :commit, [:xid]
    # internal state
    table :write_log, [:xid, :key] => [:val, :prev_xid]
    table :commit_log, [:xid] => [:prev_xid]
    # views
    scratch :last_commit, commit_log.schema
    scratch :live, write_log.schema
    scratch :write_commit_event, write.schema
    # constraints
    scratch :write_commit_constraint, [:key] => [:xid]
    # workarounds
    scratch :foo, write_log.schema
    scratch :commit_foo, commit_log.schema
  end

  bloom do
    # no worky; workaround below
    # write_seal_event <= (write * write_seal).lefts(:xid => :xid).notin(write_log, :xid => :xid)
    # test
    write_commit_event <= (write * commit).pairs(:xid => :xid){|w,s| w}.notin(write_log, 0 => :xid)
    commit_foo <= commit_log
    last_commit <= commit_log.notin(commit_foo, :xid => :prev_xid)
    write_log <+ (write_commit_event * live).pairs(:key => :key){|e, l| [e.xid, e.key, e.val, l.xid]}
    commit_log <+ (write_commit_event * last_commit).pairs{|e, l| [e.xid, l.xid]}
  end

  bloom :constraint do
    write_commit_constraint <= write_commit_event{|e| [e.key, e.xid]}
  end
end

module SimplerMultiKeyReads
  include MultiKeyWrites
  state do
    # input
    table :read, [:xid, :key]
    # internal
    table :pinned_writes, [:reader_xid, :writer_xid, :key, :val, :prev_xid]
  
    scratch :read_event, read.schema
    scratch :read_commit_event, read.schema
    scratch :read_view, pinned_writes.schema

  end
  bloom do
    read_event <= read.notin(commit_log, :xid => :xid)
    pinned_writes <+ (read_event * live).pairs{|r, l| [r.xid] + l.to_a}
    read_view <= pinned_writes.notin(commit_log, :reader_xid => :xid)
    read_commit_event <= (read * commit).pairs(:xid => :xid){|w,s| s}.notin(commit_log, 0 => :xid)
    commit_log <+ (read_commit_event * last_commit).pairs{|e, l| [e.xid, l.xid]}
  end
end
