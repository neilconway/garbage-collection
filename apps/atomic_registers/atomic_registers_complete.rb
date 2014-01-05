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
    write_commit_constraint <= commit_event{|e| [e.name, e.wid]}
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
    scratch :commit_event, write.schema
  end

  bloom do
    commit_event <= (write * commit).lefts(:batch => :batch).notin(write_log, 0 => :wid)
    write_log <+ (commit_event * live).outer(:name => :name) {|e, l| e + [l.wid.nil? ? 0 : l.wid]}
    dom <= write_log {|l| [l.prev_wid]}
    live <= write_log.notin(dom, :wid => :wid)
  end
end

module AtomicReads
  include AtomicBatchWrites

  state do
    table :read, [:batch]
    range :read_commit, [:batch]

    table :snapshot, [:effective, :wid, :batch, :name, :val, :prev_wid]
    range :snapshot_exists, [:batch]

    scratch :read_event, read.schema
    scratch :read_view, snapshot.schema
  end

  bloom do
    snapshot_exists <= snapshot {|r| [r.effective]}
    read_event <= read.notin(snapshot_exists, :batch => :batch)
    snapshot <+ (read_event * live).pairs {|r,l| r + l}
    read_view <= snapshot.notin(read_commit, :effective => :batch)
  end
end
