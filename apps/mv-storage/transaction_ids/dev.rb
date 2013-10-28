module AnotherApproach
  include MultiKeyWrites
  # in the implementation above, we made a copy of the whole live() view
  # for every read,  since we didn't know which keys it would access.
  # we don't need to do this, but we do need to recognize and ignore write_log
  # values *more* recent than our read's effective time.

  state do
    table :read, [:xid, :key]
    scratch :read_event, read.schema
    table :effective, [:xid, :anchor]

    scratch :nogood, [:reader, :writer]
    scratch :candidates, [:reader] + write_log.key_cols => write_log.val_cols
    scratch :read_view, candidates.schema
    scratch :pre_candidates, candidates.schema

    scratch :relevant_writes, write_log.schema

  end

  bloom do
    read_event <= read.notin(effective, :xid => :xid)
    effective <+ (read_event * last_commit).pairs{|r, c| [r.xid, c.xid]}

    nogood <= (effective * commit_log).pairs(:anchor => :prev_xid){|e, c| [e.xid, c.xid]}
    nogood <= (nogood * commit_log).pairs(:writer => :prev_xid){|n, c| [n.reader, c.xid]}

   # candidates <= (write_log * read).pairs(:key => :key){|w, r| [r.xid] + w.to_a}.notin(nogood, 1 => :writer)

    pre_candidates <= (write_log * read).pairs(:key => :key){|w, r| [r.xid] + w.to_a}
    candidates <= pre_candidates.notin(nogood, 1 => :writer)


    #read_view <+ write_log.notin(candidates, :xid => :prev_xid, :key => :key)
    read_view <+ candidates.notin(candidates, :xid => :prev_xid, :key => :key)


    #stdio <~ nogood.inspected
    stdio <~ read_view{|r| ["#{budtime} READVIEW #{r}"]}
    stdio <~ nogood{|r| ["#{budtime} NOGOOD #{r}"]}
    stdio <~ candidates{|r| ["#{budtime} CAND #{r}"]}
  end

end

module SimpleReadOptimized
  include MultiKeyWrites
  include ReadTabs

  state do
    table :anchor, [:read_xid] => [:write_xid]
    scratch :my_read_event, [:effective_write, :read_xid, :key]

    scratch :loose_head, last_commit.schema

  end

  bloom do
    loose_head <= last_commit.notin(anchor, :xid => :write_xid)
    my_read_event <= (read * loose_head).pairs{|r, c| [c.xid] + r}.notin(anchor, 1 => :read_xid)
    anchor <+ my_read_event{|r| [r.read_xid, r.effective_write]}
    pinned_writes <+ (my_read_event * live).pairs{|r, l| [r.effective_write] + l}

    read_view <= (anchor * pinned_writes).pairs(:write_xid => :effective){|a, p| [a.read_xid, p.xid, p.key, p.val, p.prev_xid]}.notin(read_commit_log, 0 => :xid)

    read_commit_event <= read_commit.notin(read_commit_log, 0 => :xid)
    read_commit_log <+ read_commit_event{|e| [e.xid]}
  end
end


module SimpleReadOptimized2
  include MultiKeyWrites
  include ReadTabs

  state do
    table :anchor, [:read_xid] => [:write_xid]
    scratch :my_read_event, [:effective_write, :read_xid, :key]
  end

  bloom do
    my_read_event <= (read * last_commit).pairs{|r, c| [c.xid] + r}.notin(anchor, 1 => :read_xid)
    anchor <+ my_read_event{|r| [r.read_xid, r.effective_write]}

    write_log <+ (write_commit_event * live).pairs(:key => :key){|e, l| [e.xid, e.key, e.val, l.xid]}

    pinned_writes <+ (write_commit_event * live).pairs{|r, l| [r.effective_write] + l}

    read_view <= (anchor * pinned_writes).pairs(:write_xid => :effective){|a, p| [a.read_xid, p.xid, p.key, p.val, p.prev_xid]}.notin(read_commit_log, 0 => :xid)

    read_commit_event <= read_commit.notin(read_commit_log, 0 => :xid)
    read_commit_log <+ read_commit_event{|e| [e.xid]}
  end
end

module HWM
  # cool stuff, but unnecessary for the naive 'snapshotting' implementation
  state do
    table :commit_log, [:xid] => [:prev_xid]
    scratch :last_commit, commit_log.schema
    range :previous_commits, [:xid]
  end

  bloom do
    previous_commits <= commit_log{|l| [l.prev_xid]}
    last_commit <= commit_log.notin(previous_commits, :xid => :xid)
    commit_log <+ (write_commit_event * last_commit).pairs{|e, l| [e.xid, l.xid]}
  end
end

