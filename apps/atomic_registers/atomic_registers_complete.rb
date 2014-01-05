require 'rubygems'
require 'bud'

module AtomicRegister
  state do
    table :write, [:wid] => [:batch, :name, :val]
    table :write_log, [:wid] => [:batch, :name, :val, :prev_wid]
    table :dom, [:wid]
    scratch :write_event, write.schema
    scratch :live, write_log.schema
  end

  bloom do
    write_event <= write.notin(write_log, :wid => :wid)
    write_log <+ (write_event * live).outer(:name => :name) do |e,l|
      e + [l.wid.nil? ? 0 : l.wid]
    end
    dom <= write_log {|l| [l.prev_wid]}
    live <= write_log.notin(dom, :wid => :wid)
  end
end

module SerialWriteConstraint
  # enforce the constraint that at any time, at most one transaction (performing
  # all its writes at once) writes to any given name.
  state do
    scratch :write_commit_constraint, [:name] => [:wid]
  end
  bloom :constraint do
    write_commit_constraint <= write_commit_event{|e| [e.name, e.wid]}
  end
end

module AtomicBatchWrites
  state do
    # inputs
    table :write, [:wid] => [:batch, :name, :val]
    table :commit, [:batch]
    # internal state
    table :write_log, [:wid] => [:batch, :name, :val, :prev_wid]
    table :dom, [:wid]
    # views
    scratch :live, write_log.schema
    scratch :write_commit_event, write.schema
  end

  bloom do
    write_commit_event <= (write * commit).lefts(:batch => :batch).notin(write_log, 0 => :wid)
    write_log <+ (write_commit_event * live).outer(:name => :name){|e, l| e + [l.wid.nil? ? 0 : l.wid]}
    dom <= write_log {|l| [l.prev_wid]}
    live <= write_log.notin(dom, :wid => :wid)
  end
end

module AtomicReads
  include AtomicBatchWrites

  state do
    table :read, [:batch, :name]
    range :read_commit, [:batch]

    table :snapshot, [:effective, :wid, :batch, :name, :val, :prev_wid]
    range :snapshot_exists, [:batch]

    scratch :read_event, read.schema
    scratch :read_view, snapshot.schema
  end

  bloom do
    snapshot_exists <= snapshot {|r| [r.effective]}
    read_event <= read.notin(snapshot_exists, :batch => :batch)
    snapshot <+ (read_event * live).pairs {|r,l| [r.batch] + l}
    read_view <= snapshot.notin(read_commit, :effective => :batch)
  end
end

module AtomicReads2
  include AtomicBatchWrites
  state do
    table :read, [:batch, :name]
    range :read_commit, [:batch]

    #table :snapshot, [:effective, :wid, :batch, :name, :val, :prev_wid]
    scratch :snapshot, [:effective, :wid, :batch, :name, :val, :prev_wid]
    range :snapshot_exists, [:batch]

    scratch :read_event, read.schema
    scratch :read_commit_event, read.schema
    scratch :read_view, snapshot.schema
  end

  bloom do
    snapshot_exists <= snapshot{|r| [r.effective]}
    read_event <= read.notin(snapshot_exists, :batch => :batch)
    snapshot <+ (read_event * live).pairs{|r, l| [r.batch] + l}
    read_view <= snapshot.notin(read_commit, :effective => :batch)
  end
end



module AtomicReadView2
  include AtomicBatchWrites
  state do
    table :read, [:batch, :name]
    table :post_writes, [:read_batch, :write_batch, :prev_batch]
    range :read_commit, [:batch]
    
    scratch :active_read, read.schema
    scratch :read_live, [:read_batch] + live.key_cols => live.val_cols
    scratch :active_post_writes, post_writes.schema
    scratch :relevant_wl, read_live.schema

    scratch :extra, read_live.schema
    scratch :wonder, read_live.schema
  end
  
  bloom do
    active_read <= read.notin(read_commit, :batch => :batch)
    post_writes <= (write_commit_event * active_read).pairs{|e, r| [r.batch, e.batch]}
    active_post_writes <= post_writes.notin(read_commit, :read_batch => :batch)

    # read_live contains, for each active read, those rows in live not written by a more recent write
    read_live <= (active_read * live).pairs(:name => :name){|r, l| [r.batch] + l}.notin(active_post_writes, 0 => :read_batch, 2 => :write_batch)
    # as well as those rows in write_log superceded by a "future" write.
    # BUG! 
    extra <= (active_post_writes * live).pairs(:write_batch => :batch){|p, l| [p.read_batch] + l}
    read_live <= (write_log * extra).pairs(:wid => :prev_wid){|l, w| [w.batch] + l}.notin(read_commit, 0 => :batch)
  end
end


module MinimalCopy
  include AtomicBatchWrites
  state do
    table :read, [:batch, :name]
    scratch :post_writes, [:read_batch, :write_batch]
    range :read_commit, [:batch]
    table :snapshot, [:read_batch, :name] => [:wid]

    scratch :active_read, read.schema
    scratch :read_live, [:read_batch] + live.key_cols => live.val_cols
  end

  bloom do
    active_read <= read.notin(read_commit, :batch => :batch)
    post_writes <= (write_commit_event * active_read).pairs{|e, r| [r.batch, e.batch]}
    snapshot <+ (live * post_writes).pairs(:batch => :write_batch){|l, p| [r.read_batch, l.name, l.wid]}.notin(snapshot, 0 => :read_batch, 1 => :name)

    read_live <= (active_read * live).pairs(:name => :name){|r, l| [r.batch] + l}.notin(post_writes, 0 => :read_batch, 2 => :write_batch)
    read_live <= (snapshot * write_log).pairs(:name => :name, :wid => :wid){|s, l| [s.read_batch] + l}.notin(read_commit, 0 => :batch)  
  end
end

