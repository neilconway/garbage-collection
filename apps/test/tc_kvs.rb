require_relative '../kvs'
gem 'minitest'  
require 'minitest/autorun'

class TestKvs < MiniTest::Unit::TestCase
  def test_dict_basic
    opts = { :quiet => true, :channel_stats => false }
    ports = (1..3).map {|i| i + 10001}
    addrs = ports.map {|p| "localhost:#{p}"}
    rlist = ports.map {|p| KvsReplica.new(opts.merge(:ip => "localhost", :port => p))}
    rlist.each {|r| r.node <+ addrs.map {|a| [a]}}

    first = rlist.first
    first.ins_log <+ [[first.id(1), 'foo', 'bar'],
                      [first.id(2), 'foo', 'bar2'],
                      [first.id(3), 'baz', 'qux']]

    last = rlist.last
    last.del_log <+ [[last.id(1), first.id(1)],
                     [last.id(2), first.id(2)]]

    10.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    rlist.each do |r|
      assert_equal([[first.id(3), 'baz', 'qux']].to_set, r.ins_log.to_set)
      assert_equal([].to_set, r.del_log.to_set)
    end

    rlist.each(&:stop)
  end

  def check_convergence(rlist)
    first = rlist.first
    state = [:ins_log, :del_log, :view]
    rlist.each do |r|
      next if r == first

      state.each do |t|
        r_t = r.tables[t]
        first_t = first.tables[t]
        assert_equal(first_t.to_set, r_t.to_set)
      end
    end
  end
end
