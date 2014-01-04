require_relative '../causal-kvs'
gem 'minitest'  
require 'minitest/autorun'

class TestCausalKvs < MiniTest::Unit::TestCase
  @@opts = { :quiet => true, :channel_stats => false, :range_stats => false }

  def make_cluster
    ports = (1..3).map {|i| i + 10001}
    addrs = ports.map {|p| "localhost:#{p}"}
    rlist = ports.map {|p| CausalKvsReplica.new(@@opts.merge(:ip => "localhost", :port => p))}
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
    #   (W7) baz -> kkk4, {W5,W6}
    #
    # The final view should contain W1, W5, and W7. Note that if we implemented true
    # causal consistency, we could omit the W5 dependency from W7, but that would
    # not yield the correct results.
    #
    # Reads:
    #   (1) foo, {W1}
    #   (2) baz, {W7}
    #   (3) qux, {W3,W6}
    first = rlist.first
    first.do_write(first.id(1), 'foo', 'bar')

    last = rlist.last
    last.do_write(last.id(2), 'baz', 'qux', [first.id(1)])
    last.do_write(last.id(3), 'baz', 'kkk')
    last.do_write(last.id(4), 'baz', 'kkk2', [last.id(3)])
    last.do_write(last.id(5), 'baz', 'kkk3', [last.id(2), last.id(4)])
    last.do_write(last.id(6), 'qux', 'xxx', [last.id(5)])
    last.do_write(last.id(7), 'baz', 'kkk4', [last.id(5), last.id(6)])

    c = CausalKvsReplica.new(@@opts)
    c.do_read(first.ip_port, c.id(1), 'foo', [first.id(1)])
    c.do_read(first.ip_port, c.id(2), 'baz', [last.id(7)])
    c.do_read(first.ip_port, c.id(3), 'qux', [last.id(6), last.id(3)])

    all_nodes = rlist + [c]
    15.times { all_nodes.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    check_empty(rlist, :read_buf, :read_resp, :read_dep, :log,
                :dom, :dep, :safe_dep)
    rlist.each do |r|
      assert_equal([[first.id(1), 'foo', 'bar'],
                    [last.id(6), 'qux', 'xxx'],
                    [last.id(7), 'baz', 'kkk4']].to_set,
                   r.get_safe)
      assert_equal([[first.id(1), 'foo', 'bar'],
                    [last.id(6), 'qux', 'xxx'],
                    [last.id(7), 'baz', 'kkk4']].to_set,
                   r.get_view)

      # We expect 7 logical elements in safe, but we only need to store 2
      assert_equal(7, r.safe_keys.length)
      assert_equal(2, r.safe_keys.physical_size)
    end

    check_empty([c], :read_req)
    assert_equal([[c.ip_port, c.id(1), 'foo', 'bar'],
                  [c.ip_port, c.id(2), 'baz', 'kkk4'],
                  [c.ip_port, c.id(3), 'qux', 'xxx']].to_set,
                 c.read_result.to_set)

    assert_equal(3, c.req_chn_approx.length)
    assert_equal(1, c.req_chn_approx.physical_size)

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
    check_empty(rlist, :log, :dom, :dep, :safe_dep)
    rlist.each do |r|
      assert_equal([[first.id(9), "foo", "bar9"]].to_set,
                   r.get_safe)
      assert_equal([[first.id(9), "foo", "bar9"]].to_set,
                   r.get_view)

      assert_equal(10, r.safe_keys.length)
      assert_equal(1, r.safe_keys.physical_size)
    end

    rlist.each(&:stop)
  end

  def test_concurrent_writes
    rlist = make_cluster

    # Writes:
    #   (W1)  qux -> baz, deps = []
    #   (W2)  foo -> bar, deps = [W1]
    #   (W2') foo -> baz, deps = [W1]
    first = rlist.first
    last = rlist.last
    first.do_write(first.id(1), "qux", "baz")
    first.do_write(first.id(2), "foo", "bar", [first.id(1)])
    last.do_write(last.id(1), "foo", "baz", [first.id(1)])
    15.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    check_empty(rlist, :log, :dom, :dep, :safe_dep)
    rlist.each do |r|
      assert_equal([[first.id(1), "qux", "baz"],
                    [first.id(2), "foo", "bar"],
                    [last.id(1), "foo", "baz"]].to_set, r.get_view)
    end

    # Writes:
    #   (W3) foo -> baxxx, deps = [W2, W2']
    last.do_write(last.id(2), "foo", "baxxx", [first.id(2), last.id(1)])
    15.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    check_empty(rlist, :log, :dom, :dep, :safe_dep)
    rlist.each do |r|
      assert_equal([[first.id(1), "qux", "baz"],
                    [last.id(2), "foo", "baxxx"]].to_set, r.get_view)
    end

    # Writes:
    #   (W4) qux -> baxxx, deps = [W1, W2']
    last.do_write(last.id(3), "qux", "baxxx", [first.id(1), last.id(1)])
    15.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    check_empty(rlist, :log, :dom, :dep, :safe_dep)
    rlist.each do |r|
      assert_equal([[last.id(3), "qux", "baxxx"],
                    [last.id(2), "foo", "baxxx"]].to_set, r.get_view)
    end

    # XXX: Check that both values are returned from a read of a key with two
    # concurrent writes

    rlist.each(&:stop)
  end

  def test_read_with_bad_deps
    rlist = make_cluster
  end

  def test_dom_with_bad_deps
    rlist = make_cluster

    # Writes:
    #   (W1) foo -> bar, deps = [W4]
    #   (W2) baz -> qux, deps = []
    #   (W3) baz -> qux2, deps = [W1, W2]
    first = rlist.first
    first.do_write(first.id(1), "foo", "bar", [first.id(4)])
    first.do_write(first.id(2), "baz", "qux", [])
    first.do_write(first.id(3), "baz", "qux2", [first.id(1), first.id(2)])
    15.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    check_empty(rlist, :safe_dep, :dom)
    rlist.each do |r|
      assert_equal([[first.id(1), first.id(4)],
                    [first.id(3), first.id(1)],
                    [first.id(3), first.id(2)]].to_set, r.dep.to_set)
      assert_equal([[first.id(2), "baz", "qux"]].to_set, r.get_view)
      assert_equal([[first.id(2), "baz", "qux"]].to_set, r.get_safe)
    end

    # Writes:
    #   (W4) baz -> qux3, deps = []
    first.do_write(first.id(4), "baz", "qux3")
    15.times { rlist.each(&:tick); sleep 0.1 }

    check_convergence(rlist)
    check_empty(rlist, :log, :dom, :dep, :safe_dep)
    rlist.each do |r|
      assert_equal([[first.id(1), "foo", "bar"],
                    [first.id(4), "baz", "qux3"],
                    [first.id(3), "baz", "qux2"]].to_set, r.get_view)
    end

    rlist.each(&:stop)
  end

  def test_dep_on_different_key
  end

  def check_convergence(rlist)
    first = rlist.first
    state = [:log, :safe, :safe_keys, :dom, :view, :dep, :safe_dep,
             :read_buf, :read_dep, :read_resp, :log_chn_approx]
    rlist.each do |r|
      next if r == first

      state.each do |t|
        r_t = r.tables[t]
        first_t = first.tables[t]
        assert_equal(first_t.to_set, r_t.to_set, "t = #{t}")
      end
    end
  end

  def check_empty(rlist, *rels)
    rlist.each do |r|
      rels.each do |rel|
        assert_equal([].to_set, r.tables[rel].to_set, "not empty: #{rel}")
      end
    end
  end
end
