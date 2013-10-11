require 'rubygems'
require 'bud'

module MVCC
  # Intuitions: we can reclaim any versions (entries in log())
  # that are not currently used to:
  # 1) derive a tuple in current(), or
  # 2) derive a tuple in hot_versions()
  
  # Further, we can delete inquiry_result() records when their corresponding
  # log entries appear.

  state do
    # both reads and writes to keys are logged at commit time, 
    # along with their 'version'
    # a version is a set of dependent operation ids.
    table :log, [:id, :key] => [:val, :deps, :kind]
    scratch :flat_dep, [:id, :kind, :dep]
    scratch :current, log.schema

    # inquiries are made, and logged, regarding the current versions of objects
    # for read-only queries.
    table :inquiry_result, [:id, :key] => [:dep]
    scratch :live_reads, inquiry_result.schema
    scratch :hot_versions, log.schema
  end
  bloom do
    flat_dep <= log.flat_map{|l| l.deps.map{|d| [l.id, l.kind, d] unless l.id == d}}

    # Updates operate on the most current written version.  We assume that updates
    # follow 2PL, and check their "version" at commit time -- that is, we
    # consult current() while holding locks.
    # The "current" version for a key is the union of all the read or write
    # action_ids that have referenced it.  The calling process would need to
    # take the union of the two rows returned for reads and writes to this key,
    # along with any other keys in their read/write set (see the test code).
    # A (read,write) log entry is "current" if no other (read,write) entries
    # depend on it.
    current <= log.notin(flat_dep, :id => :dep, :kind => :kind)

    # a read first does a 'reconnaisance' query to determine the versions
    # of keys it references.  Between this time and when the read completes,
    # we need to retain the logged version even if it is no longer "current."
    live_reads <= inquiry_result.notin(log, :id => :id)
    hot_versions <= (log * live_reads).lefts(:key => :key, :deps => :dep)
  end

  bloom :debug do
    #stdio <~ current{|c| ["CURRENT: #{c}"]}
    #stdio <~ inquiry_result{|c| ["INQ-RESULT(#{budtime}): #{c}"]}
    #stdio <~ live_reads{|c| ["LIVE: #{c}"]}
    #stdio <~ hot_versions{|h| ["HOT: #{h}"]}
  end
end

class M
  include Bud
  include MVCC
end
