require 'rubygems'
require 'bud'

class AtomicRegister
  include Bud

  state do
    table :write, [:wid] => [:name, :val]
    table :write_log, [:wid] => [:name, :val, :prev_wid]
    scratch :write_event, write.schema
    scratch :live, write_log.schema
    range :write_done, [:prev_wid]
  end

  bloom do
    write_event <= write.notin(write_log, :wid => :wid)
    write_log <+ (write_event * live).pairs(:name => :name) do |e,l|
      e + [l.wid]
    end
    write_done <= write_log {|l| [l.prev_wid]}
    live <= write_log.notin(write_done, :wid => :prev_wid)
  end
end

class AtomicBatchWrites
  include Bud

  state do
    table :write, [:wid] => [:batch, :name, :val]
    table :commit, [:batch]
    table :write_log, [:wid] => [:batch, :name, :val, :prev_wid]
    scratch :live, write_log.schema
    scratch :commit_event, write.schema
    range :write_done, [:prev_wid]
  end

  bloom do
    commit_event <= (write * commit).lefts(:batch => :batch).notin(write_log, 0 => :wid)
    write_log <+ (commit_event * live).pairs(:name => :name) do |e,l| 
      [e.wid, e.batch, e.name, e.val, l.wid]
    end
    write_done <= write_log {|l| [l.prev_wid]}
    live <= write_log.notin(write_done, :wid => :prev_wid)
  end
end

class AtomicReads
  include Bud

  state do
    table :read, [:batch, :name]
    range :read_commit, [:batch]
    table :snapshot, [:effective, :wid, :batch, :name, :val, :prev_wid]
    range :snapshot_exists, [:batch]
    scratch :read_event, read.schema
    scratch :read_view, snapshot.schema
  end

  bloom do
    snapshot_exists <= snapshot {|w| [w.effective]}
    read_event <= read.notin(snapshot_exists, :batch => :batch)
    snapshot <+ (read_event * live).pairs {|r, l| [r.batch] + l} 
    read_view <= snapshot.notin(read_commit, :effective => :batch)
  end
end
