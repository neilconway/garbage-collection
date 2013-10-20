require 'rubygems'
require 'bud'

module Dependencies
  def successor(x)
    x + 1
  end

  bloom do
    # here, > is just integer inequality
    live <= write_log.notin(write_log, :key => :key){|r, l| l.dep > r.dep}
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
    write_log <+ (write_event * live).pairs(:key => :key){|e, l| [e.aid, e.key, e.val, successor(l.dep)]}
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
    scratch :write_seal_event, write.schema
  end

  bloom do
    write_seal_event <= (write * write_seal).lefts(:aid => :aid).notin(write_log, :aid => :aid)
    commit_set <= (live * write_seal_event).pairs(:key => :key){|l, e| [e.aid, e.key, e.val, successor(l.dep)]}
    # LUB is max
    write_version <= commit_set.group([:aid], max(:dep))
    write_log <+ (commit_set * write_version).pairs(:aid => :aid){|c, v| [c.aid, c.key, c.val, v.dep]}
  end
end

module MultiKeyReads
  include MultiKeyWrites
  state do
    # inputs 
    table :prepare, [:aid, :key]
    table :prepare_seal, [:aid]
    table :read, [:aid, :key]
    # internal state
    table :prepare_effective, [:aid, :key] => [:val, :dep]
    # views 
    scratch :prepare_seal_event, [:aid, :key]
    scratch :read_response, [:aid, :key, :value, :dep]
  end

  bloom do
    prepare_seal_event <= (prepare * prepare_seal).lefts(:aid => :aid).notin(prepare_effective, :aid => :aid)
    prepare_effective <+ (prepare_seal_event * live).pairs(:key => :key){|e, l| [e.aid, e.key, l.val, l.dep]}
    read_response <= (read * prepare_effective).pairs(:aid => :aid, :key => :key){|r, p| [r.aid, r.key, p.val, p.dep]}
  end
end
