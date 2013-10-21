require 'rubygems'
gem 'minitest'
require './mvcc_evo_sets'

require 'minitest/autorun'

module BaseData
  bootstrap do
    # [:aid] => [:key, :val, :dep]
    write_log << [1, "foo", "bar", [0,1]]
    write_log << [2, "peter", "thane of glamis", [0,2]]
    write_log << [3, "banquo", "dead but gets kings", [0,3]]
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
    assert_equal([[1, 'foo', 'bar', [0,1]], [2, 'peter', 'thane of glamis', [0,2]], [3, "banquo", "dead but gets kings", [0,3]]], s.live.to_a.sort)
    s.write <+ [[100, 'foo', 'baz']]
    s.tick; s.tick
    assert_equal([[2, 'peter', 'thane of glamis', [0,2]], [3, "banquo", "dead but gets kings", [0,3]], [100, 'foo', 'baz', [0,1,100]]], s.live.to_a.sort)
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
    assert_equal([[1, "foo", "bar", [0,1]],[2, "peter", "thane of glamis", [0,2]], [3, "banquo", "dead but gets kings", [0,3]]], m.live.to_a.sort)
    multi_w_wload(m)
    assert_equal([[3, "banquo", "dead but gets kings", [0,3]], [100, "foo", "baz", [0,1,2,100]],[100, "peter", "thane of cawdor", [0,1,2,100]]], m.live.to_a.sort)
  end

  def test_itall
    m = MultiReadWrite.new
    m.tick
    m.prepare <+ [[200, "foo"], [200, "peter"]]
    m.prepare_seal <+ [[200]]
    5.times{ m.tick }
    assert_equal([[200, 'foo', 'bar', [0,1]], [200, "peter", "thane of glamis", [0,2]]], m.read_response.to_a.sort)
    multi_w_wload(m)
    m.prepare <+ [[300, "banquo"], [300, "peter"]]
    m.prepare_seal <+ [[300]]
    m.tick; m.tick
    assert_equal([[3, "banquo", "dead but gets kings", [0,3]], [100, 'foo', 'baz', [0,1,2,100]], [100, 'peter', 'thane of cawdor', [0,1,2,100]]], m.live.to_a.sort)
    assert_equal([[200, 'foo', 'bar', [0,1]], 
                 [200, 'peter', 'thane of glamis', [0,2]], 
                 [300, "banquo", "dead but gets kings", [0,3]], 
                 [300, 'peter', 'thane of cawdor', [0,1,2,100]]], 
              m.read_response.to_a)
  end

  def test_concurrent_writes
    s = SimpleWrite.new
    s.tick
    s.write <+ [[100, 'foo', 'baz']]
    assert_raises(Bud::KeyConstraintError){ s.write <+ [[100, 'foo', 'qux']] }

    m = MultiWrite.new
    m.tick
    m.write <+ [[100, 'foo', 'baz']]
    assert_raises(Bud::KeyConstraintError){ m.write <+ [[100, 'foo', 'qux']] }
  end

  def read_key(inst, key)
    inst.live.each do |l| 
      if l.key == key
        return l
      end
    end
    return nil
  end

  def test_snapshot_anomaly
    m = MultiWrite.new
    m.tick
    results = {}
    results['peter'] = read_key(m, 'peter')
    multi_w_wload(m)
    results['foo'] = read_key(m, 'foo')
    assert_equal('thane of glamis', results['peter'].val)
    assert_equal('baz', results['foo'].val)
  end
end
