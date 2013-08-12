require 'rubygems'
require 'bud'

module CausalCore
  state do
    # core collections
    # assumption: reqids are globally unique and externally generated.  
    # no ordering or other semantic assumptions.  Hence we can represent dependencies
    # as key, reqid pairs (but can't use integer comparison for dominance)
    table :put_log, [:reqid] => [:key, :value, :deps]
    scratch :active_puts, put_log.schema

    scratch :flat_deps, [:reqid, :depid]
    scratch :missing_deps, flat_deps.schema
    table :good_put, put_log.schema
    table :dominated, [:reqid]
    scratch :contains, [:r1, :r2]
  end

  bloom :satisfied do 
    flat_deps <= put_log.flat_map do |p|
      p.deps.map{|d| [p.reqid, d]}
    end

    missing_deps <= flat_deps.notin(good_put, :depid => :reqid)
    good_put <+ put_log.notin(missing_deps, :reqid => :reqid)
    active_puts <= (put_log * good_put).lefts(:reqid => :reqid).notin(dominated, :reqid => :reqid)
  end

  bloom :dominated do
    contains <= (put_log * put_log).pairs(:key => :key) do |l1, l2|
      [l1.reqid, l2.reqid] if l1.reqid != l2.reqid and l2.deps.to_set.subset? l1.deps.to_set
    end
    dominated <= (contains * good_put).lefts(:r1 => :reqid){|c| [c.r2]}
  end
end


class CC
  include Bud
  include CausalCore
end


c = CC.new
c.put_log <+ [[1, "foo", "bar", []],
              [2, "foo", "baz", [1]],
              [3, "foo", "qux", [1,4]]]
c.tick; c.tick; c.tick

puts c.active_puts.to_a.sort.inspect

c.put_log <+ [[4, "bar", "eek", []]]
c.tick; c.tick; c.tick

puts c.active_puts.to_a.sort.inspect

c.put_log <+ [[5, "foo", "quux", [1, 4, 8]],
              [6, "trick", "treat", [1, 5]],
              [7, "k", "v", [2]]]
c.tick; c.tick

puts c.active_puts.to_a.sort.inspect
