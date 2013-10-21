require 'rubygems'
require 'bud'

module Dependencies
  state do
    scratch :flat_dep, [:aid, :key, :dep]  
  end

  bloom do
    flat_dep <= write_log.flat_map{|l| l.dep.map{|d| [l.aid, l.key, d]}}
    live <= write_log.notin(flat_dep, :aid => :dep, :key => :key){|l, d| d.aid != d.dep}
  end
end

module SimpleMV
  # the simplest possible multivalued store
  include Dependencies
  state do
    # inputs
    table :write, [:aid] => [:key, :val]
    # internal state
    table :write_log, [:aid] => [:key, :val, :dep]
    # views
    scratch :write_event, write.schema
    scratch :live, write_log.schema
  end

  bloom do
    write_event <= write.notin(write_log, :aid => :aid)
    # N.B. there is only ever one row per key in live(); hence the new version
    # can always be simply the successor of the parent version's dependencies.
    write_log <+ (write_event * live).pairs(:key => :key){|e, l| [e.aid, e.key, e.val, l.dep | [e.aid]]}
  end
end

module MultiKeyWrites
  include Dependencies
  state do
    # inputs
    table :write, [:aid, :key] => [:val]
    table :write_seal, [:aid]
    # internal state
    table :write_log, [:aid, :key] => [:val, :dep]
    # views
    scratch :live, write_log.schema
    scratch :commit_set, [:aid, :key] => [:val, :dep]
    scratch :write_version, [:aid] => [:dep]
    scratch :write_version_flat, [:aid, :dep]
    scratch :write_seal_event, write.schema
  end

  bloom do
    write_seal_event <= (write * write_seal).lefts(:aid => :aid).notin(write_log, :aid => :aid)
    commit_set <= (live * write_seal_event).pairs(:key => :key){|l, e| [e.aid, e.key, e.val, l.dep | [e.aid]]}
    # LUB is union
    # the version # associated with a write contains the versions of any dependencies.
    write_version_flat <= (commit_set * flat_dep).pairs(:key => :key){|c,d| [c.aid, d.dep]}
    write_version <= write_version_flat.group([:aid], accum(:dep))
    write_log <+ (commit_set * write_version).pairs(:aid => :aid){|c, v| [c.aid, c.key, c.val, v.dep.to_a | [c.aid]]}
  end
end

module MultiKeyReads
  include MultiKeyWrites
  state do
    # inputs 
    table :prepare, [:aid, :key]
    table :prepare_seal, [:aid]
    table :read_commit, [:aid]
    # internal state
    table :prepare_effective, [:aid, :key] => [:dep]
    # views 
    scratch :prepare_seal_event, [:aid, :key]
    scratch :active, prepare_effective.schema
    scratch :read_response, [:aid, :key, :value, :dep]
  end

  bloom do
    prepare_seal_event <= (prepare * prepare_seal).lefts(:aid => :aid).notin(prepare_effective, :aid => :aid)
    prepare_effective <+ (prepare_seal_event * live).pairs(:key => :key){|e, l| [e.aid, e.key, l.dep]}
    active <= prepare_effective.notin(read_commit, :aid => :aid)
    read_response <= (write_log * active).pairs(:key => :key, :dep => :dep){|l, p| [p.aid, l.key, l.val, l.dep]}
  end
end