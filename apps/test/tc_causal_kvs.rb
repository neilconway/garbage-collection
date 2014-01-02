require_relative '../causal-kvs'
gem 'minitest'  
require 'minitest/autorun'

class TestCausalKvs < MiniTest::Unit::TestCase
  @@opts = { :quiet => true, :channel_stats => false, :range_stats => false }

  def make_cluster
    ports = (1..3).map {|i| i + 10001}
    addrs = ports.map {|p| "localhost:#{p}"}
    rlist = ports.map {|p| CausalKvsReplica.new(@@opts.merge(:ip => "localhost", :port => p, :print_rules => (p == ports.first ? true : false), :print_state => (p == ports.first ? true : false)))}
    rlist.each {|r| r.node <+ addrs.map {|a| [a]}}
    rlist
  end

  def test_causal_basic
    rlist = make_cluster

    # Writes:
    #   (W1) foo -> bar, deps={} (origin = server 1)
    #   (W2) baz -> qux, {W1}    (origin = server 3, same for the following)
    #   (W3) baz -> kkk, {}
    #   (W4) baz -> kkk2, {W3}
    #   (W5) baz -> kkk3, {W2,W4}
    #   (W6) qux -> xxx, {W5}
    #   (W7) baz -> kkk4, {W6,W5}
    #
    # The final view should contain W1, W6, and W7. Note that if we implemented true
    # causal consistency, we could omit the W5 dependency from W7, but that would
    # not yield the correct results.
    #
    # Reads:
    #   (1) foo, {W1}
    first = rlist.first
    first.do_write(first.id(1), 'foo', 'bar')

    last = rlist.last
    last.do_write(last.id(2), 'baz', 'qux', [first.id(1)])
    last.do_write(last.id(3), 'baz', 'kkk')
    last.do_write(last.id(4), 'baz', 'kkk2', [last.id(3)])
    last.do_write(last.id(5), 'baz', 'kkk3', [last.id(2), last.id(4)])
    last.do_write(last.id(6), 'qux', 'xxx', [last.id(5)])
    last.do_write(last.id(7), 'baz', 'kkk4', [last.id(6), last.id(5)])
    # last.log <+ [[last.id(3), 'baz', 'kkk', []],
    #              [last.id(4), 'baz', 'kkk2', [last.id(3)]],
    #              [last.id(5), 'baz', 'kkk3', [last.id(2), last.id(4)]],
    #              [last.id(6), 'qux', 'xxx', [last.id(5)]],
    #              [last.id(7), 'baz', 'kkk4', [last.id(6), last.id(5)]]]

    c = CausalKvsReplica.new(@@opts)
    c.do_read(last.ip_port, c.id(1), 'foo', [first.id(1)])

    all_nodes = rlist + [c]
    15.times { all_nodes.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    assert_equal([].to_set, c.read_req.to_set)
    assert_equal([[c.ip_port, c.id(1), 'foo', 'bar']].to_set,
                 c.read_result.to_set)
    rlist.each do |r|
      assert_equal([].to_set, r.read_buf.to_set)
      assert_equal([].to_set, r.read_resp.to_set)
      assert_equal([].to_set, r.read_dep.to_set)
      assert_equal([].to_set, r.log.to_set)
      assert_equal([].to_set, r.dom.to_set)
      assert_equal([].to_set, r.dep.to_set)
      assert_equal([].to_set, r.safe_dep.to_set)
      assert_equal([[first.id(1), 'foo', 'bar'],
                    [last.id(6), 'qux', 'xxx'],
                    [last.id(7), 'baz', 'kkk4']].to_set,
                   r.safe.to_set)

      # We expect 7 logical elements in safe, but we only need to store 2
      assert_equal(7, r.safe_keys.length)
      assert_equal(2, r.safe_keys.physical_size)
    end

    all_nodes.each(&:stop)
  end

  def test_dominated_gc
    rlist = make_cluster

    first = rlist.first
    deps = []
    10.times do |i|
      first.do_write(first.id(i), "foo", "bar#{i}", deps)
      deps = [first.id(i)]
    end

    15.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    rlist.each do |r|
      assert_equal([].to_set, r.read_buf.to_set)
      assert_equal([].to_set, r.read_resp.to_set)
      assert_equal([].to_set, r.log.to_set)

      assert_equal(10, r.safe_keys.length)
      assert_equal(1, r.safe_keys.physical_size)

      assert_equal([[first.id(9), "foo", "bar9"]].to_set,
                   r.safe.to_set)
      assert_equal([].to_set, r.dom.to_set)
      assert_equal([].to_set, r.dep.to_set)
      assert_equal([].to_set, r.safe_dep.to_set)
    end

    rlist.each(&:stop)
  end

  def check_convergence(rlist)
    first = rlist.first
    state = [:log, :safe, :safe_keys, :dom, :view, :dep, :safe_dep,
             :seal_dep_id, :seal_read_dep_id, :read_buf, :read_dep, :read_desp]
    rlist.each do |r|
      next if r == first

      state.each do |t|
        r_t = r.tables[t]
        first_t = first.tables[t]
        assert_equal(first_t.to_set, r_t.to_set, "t = #{t}")
      end
    end
  end
end
