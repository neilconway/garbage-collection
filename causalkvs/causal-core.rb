require 'rubygems'
require 'bud'

module CausalCore
  state do
    # core collections
    # assumption: reqids are globally unique and externally generated.  
    # no ordering or other semantic assumptions.  Hence we can represent dependencies
    # as key, reqid pairs (but can't use <= for dominance)
    table :put_log, [:reqid, :key, :value, :deps]
    scratch :active_puts, put_log.schema

    # helper collections
    scratch :flat_deps, [:reqid, :key, :depid]
    table :satisfied, [:reqid]
    scratch :good_put, put_log.schema
    scratch :missing_deps, flat_deps.schema
    table :dominated, [:reqid]
    scratch :contains, [:r1, :r2]
  end

  bloom do
    flat_deps <= put_log.flat_map do |p|
      p.deps.map{|d| [p.reqid, p.key, d]}
    end

    # satisfied but not dominated.  both sets of reqids. 
    active_puts <= (put_log * satisfied).lefts(:reqid => :reqid).notin(dominated, :reqid => :reqid)
    stdio <~ active_puts{|p| ["ACTIVE(#{budtime}): #{p}"]}
  end

  bloom :satisfied do 
    missing_deps <= flat_deps.notin(put_log, :depid => :reqid)
    good_put <= put_log.notin(missing_deps, :reqid => :reqid)
    satisfied <= good_put{|p| [p.reqid]}
  end

  bloom :dominated do
    contains <= (put_log * put_log).pairs(:key => :key) do |l1, l2|
      [l1.reqid, l2.reqid] if l1.reqid != l2.reqid and l2.deps.to_set.subset? l1.deps.to_set
    end
    dominated <= (contains * satisfied).lefts(:r1 => :reqid){|c| [c.r2]}
  end
end


class CC
  include Bud
  include CausalCore
end


c = CC.new

c.put_log <+ [[1, "foo", "bar", []]]
c.put_log <+ [[2, "foo", "baz", [1]]]
c.put_log <+ [[3, "foo", "qux", [1,4]]]



c.tick;c.tick

c.put_log <+ [[4, "bar", "eek", []]]

c.tick;c.tick


c.put_log.each{|p| puts "PL: #{p}"}



c.put_log <+ [[5, "foo", "qux", [1, 4, 7]]]
c.tick
