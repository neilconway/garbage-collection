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
    scratch :dominated, good_put.schema
  end

  bloom :satisfied do 
    flat_deps <= put_log.flat_map do |p|
      p.deps.map{|d| [p.reqid, d]}
    end

    missing_deps <= flat_deps.notin(good_put, :depid => :reqid)
    good_put <+ put_log.notin(missing_deps, :reqid => :reqid)
    active_puts <= good_put.notin(dominated, :reqid => :reqid)
  end

  bloom :dominated do
    dominated <= (good_put * good_put).pairs(:key => :key) do |p1, p2|
      p2 if p1 != p2 and p1.deps.include? p2.reqid
    end
  end

  def print_view
    puts active_puts.map {|p| "#{p.key} => #{p.value}"}.sort.inspect
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
c.print_view

c.put_log <+ [[4, "bar", "eek", []]]
c.tick; c.tick; c.tick
c.print_view

c.put_log <+ [[5, "foo", "quux", [1, 4, 99]],
              [6, "trick", "treat", [1, 5]],
              [7, "k", "v", [2]]]
c.tick; c.tick
c.print_view

c.put_log <+ [[8, "foo", "FINAL", [3]]]
c.tick; c.tick
c.print_view

