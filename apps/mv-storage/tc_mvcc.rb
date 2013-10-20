require 'rubygems'
gem 'minitest'
require './mvcc_evo'

require 'minitest/autorun'

module BaseData
  bootstrap do
    # [:aid] => [:key, :val, :dep]
    write_log << [0, "foo", "bar", 0]
    write_log << [1, "peter", "thane of glamis", 0]
    write_log << [2, "banquo", "dead but gets kings", 0]
  end
end

module Debug
  bloom do
    #stdio <~ live{|l| ["LIVE #{budtime}: #{l}"]}
    #stdio <~ write{|l| ["WRITE #{budtime}: #{l}"]}
  end
end

class SimpleWrite
  include Bud
  include SimpleMV
  include BaseData
  include Debug
end


class MultiWrite
  include Bud
  include MultiKeyWrites
  include BaseData
  include Debug
end

class MultiReadWrite
  include Bud
  include MultiKeyWrites
  include MultiKeyReads
  include BaseData
  include Debug
end

class TestMVCCs < Minitest::Test
  def test_simple
    s = SimpleWrite.new
    s.tick
    assert_equal([[0, 'foo', 'bar', 0], [1, 'peter', 'thane of glamis', 0], [2, "banquo", "dead but gets kings", 0]], s.live.to_a.sort)
    s.write <+ [[100, 'foo', 'baz']]
    s.tick; s.tick
    assert_equal([[1, 'peter', 'thane of glamis', 0], [2, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1]], s.live.to_a.sort)
  end

  def multi_w_wload(m)
    m.write <+ [[100, 'foo', 'baz']]
    3.times{ m.tick }
    m.write <+ [[100, 'peter', 'thane of cawdor']]
    m.write_seal <+ [[100]]
    m.tick; m.tick
  end 


  def test_multiwrite
    m = MultiWrite.new
    m.tick
    assert_equal([[0, "foo", "bar", 0],[1, "peter", "thane of glamis", 0], [2, "banquo", "dead but gets kings", 0]], m.live.to_a.sort)
    multi_w_wload(m)
    assert_equal([[2, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1],[100, "peter", "thane of cawdor", 1]], m.live.to_a.sort)
  end

  def test_itall
    m = MultiReadWrite.new
    m.tick
    m.prepare <+ [[200, "foo"], [200, "peter"]]
    m.prepare_seal <+ [[200]]
    m.tick

    m.read <+ [[200, 'foo']]
    5.times{ m.tick }
    assert_equal([[200, 'foo', 'bar', 0]], m.read_response.to_a)
    multi_w_wload(m)
    m.prepare <+ [[300, "banquo"], [300, "peter"]]
    m.prepare_seal <+ [[300]]
    assert_equal([[200, 'foo', 'bar', 0]], m.read_response.to_a)

    m.read <+ [[200, 'peter']]
    m.tick
    assert_equal([[200, 'foo', 'bar', 0], [200, 'peter', 'thane of glamis', 0]], m.read_response.to_a)
    assert_equal([[2, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1], [100, 'peter', 'thane of cawdor', 1]], m.live.to_a.sort)

    m.read <+ [[300, "peter"]]
    m.tick
    assert_equal([[200, 'foo', 'bar', 0], [200, 'peter', 'thane of glamis', 0], [300, 'peter', 'thane of cawdor', 1]], m.read_response.to_a)
  end
end
