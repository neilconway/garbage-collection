require 'rubygems'
require 'bud'

module Dependencies
  state do
    range :write_done, [:prev_wid]
  end
  bloom do
    write_done <= write_log{|l| [l.prev_wid]}
    live <= write_log.notin(write_done, :wid => :prev_wid)
  end
end

module SimpleMV
  include Dependencies
  state do
    table :write, [:wid] => [:xact, :key, :val]
    table :write_log, [:wid] => [:xact, :key, :val, :prev_wid]
    scratch :write_event, write.schema
    scratch :live, write_log.schema
  end

  bloom do
    write_event <= write.notin(write_log, :wid => :wid)
    write_log <+ (write_event * live).pairs(:key => :key) do |e, l|
      e + [l.wid]
    end
  end
end

module SerialWriteConstraint
  # enforce the constraint that at any time, at most one transaction (performing all its writes at once)
  # writes to any given key.
  state do
    scratch :write_commit_constraint, [:key] => [:wid]
  end
  bloom :constraint do
    write_commit_constraint <= write_commit_event{|e| [e.key, e.wid]}
  end
end

module MultiKeyWrites
  include Dependencies
  include SerialWriteConstraint
  state do
    # inputs
    table :write, [:wid] => [:xact, :key, :val]
    table :commit, [:xact]
    # internal state
    table :write_log, [:wid] => [:xact, :key, :val, :prev_wid]
    # views
    scratch :live, write_log.schema
    scratch :write_commit_event, write.schema
  end

  bloom do
    write_commit_event <= (write * commit).pairs(:xact => :xact){|w,s| w}.notin(write_log, 0 => :wid)
    write_log <+ (write_commit_event * live).pairs(:key => :key){|e, l| [e.wid, e.xact, e.key, e.val, l.wid]}
  end
end

module ReadTabs
  # separated because reused in dev.rb
  state do
    # input
    table :read, [:xact, :key]
    range :read_commit, [:xact]
  
    # internal
    table :pinned, [:effective, :wid, :xact, :key, :val, :prev_wid]
  
    scratch :read_event, read.schema
    scratch :read_commit_event, read.schema
    scratch :read_view, pinned.schema
  end
end

module SimplerMultiKeyReads
  include MultiKeyWrites
  include ReadTabs
  state do
    table :ever_pinned, [:xact]
  end
  bloom do
    ever_pinned <= pinned.notin(read_commit, :effective => :xact).pro{|r| [r.effective]}
    read_event <= read.notin(read_commit, :xact => :xact).notin(ever_pinned, :xact => :xact)
    pinned <+ (read_event * live).pairs{|r, l| [r.xact] + l}
    read_view <= pinned.notin(read_commit, :effective => :xact)
  end
end
