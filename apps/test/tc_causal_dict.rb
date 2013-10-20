require_relative '../causal-dict'
gem 'minitest'  
require 'minitest/autorun'

class TestCausalDict < MiniTest::Unit::TestCase
  def test_causal_basic
    opts = { :channel_stats => false, :range_stats => false, :disable_rse => false }
    ports = (1..3).map {|i| i + 10001}
    addrs = ports.map {|p| "localhost:#{p}"}
    rlist = ports.map {|p| CausalDict.new(opts.merge(:ip => "localhost", :port => p))}
    rlist.each do |r|
      r.node <+ addrs.map {|a| [a]}
      r.tick
    end

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
    first.log <+ [[first.id(1), 'foo', 'bar', []]]

    last = rlist.last
    last.log <+ [[last.id(2), 'baz', 'qux', [first.id(1)]]]
    last.log <+ [[last.id(3), 'baz', 'kkk', []],
                 [last.id(4), 'baz', 'kkk2', [last.id(3)]],
                 [last.id(5), 'baz', 'kkk3', [last.id(2), last.id(4)]],
                 [last.id(6), 'qux', 'xxx', [last.id(5)]],
                 [last.id(7), 'baz', 'kkk4', [last.id(6), last.id(5)]]]

    c = CausalDict.new(opts)
    c.tick
    c.read_req <+ [[last.ip_port, c.id(1), 'foo', [first.id(1)]]]
    c.tick

    15.times { rlist.each(&:tick); sleep 0.1; c.tick }

    check_convergence(rlist)
    assert_equal([].to_set, c.read_req.to_set)
    assert_equal([[c.ip_port, c.id(1), 'foo', 'bar']].to_set,
                 c.read_result.to_set)
    rlist.each do |r|
      assert_equal([].to_set, r.read_buf.to_set)
      assert_equal([].to_set, r.read_resp.to_set)
      assert_equal([].to_set, r.log.to_set)
      assert_equal([[first.id(1), 'foo', 'bar', []],
                    [last.id(6), 'qux', 'xxx', [last.id(5)]],
                    [last.id(7), 'baz', 'kkk4', [last.id(6), last.id(5)]]].to_set,
                   r.safe_log.to_set)
    end

    c.stop
    rlist.each(&:stop)
  end

  def check_convergence(rlist)
    first = rlist.first
    state = [:log, :safe_log, :safe, :dominated, :view]
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
