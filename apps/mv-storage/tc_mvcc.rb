require 'rubygems'
gem 'minitest'
require './mvcc_evo'

require 'minitest/autorun'

module BaseData
  bootstrap do
    # [:aid] => [:key, :val, :dep]
    write_log <+ [[1, "foo", "bar", 0],
            [2, "peter", "thane of glamis", 0],
            [3, "banquo", "dead but gets kings", 0]]

    # if the analysis has synthesized this auxiliary table, populate it as though
    # we had used the write() interface properly.
    write_all_keys <+ [[1],[2],[3]] if self.tables.keys.include? :write_all_keys
  end
end

class SimpleWrite
  include Bud
  include SimpleMV
  include BaseData
end

class MultiWrite
  include Bud
  include MultiKeyWrites
  include BaseData
end

class MultiReadWrite
  include Bud
  include MultiKeyWrites
  include SimplerMultiKeyReads
  include BaseData
end

class TestMVCCs < Minitest::Test
  def test_simple
    s = SimpleWrite.new
    s.tick;s.tick
    assert_equal([[1, 'foo', 'bar', 0], [2, 'peter', 'thane of glamis', 0], [3, "banquo", "dead but gets kings", 0]], s.live.to_a.sort)
    s.write <+ [[100, 'foo', 'baz']]
    s.tick; s.tick
    assert_equal([[2, 'peter', 'thane of glamis', 0], [3, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1]], s.live.to_a.sort)
    4.times{ s.tick }

    # confirm that the redundant entries in writes() have been GC'd
    assert_equal([], s.write.to_a)

    # confirm that the foo=bar write_log entry has been GC'd
    assert_equal([[2, 'peter', 'thane of glamis', 0], [3, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1]], s.write_log.to_a.sort)
  end

  def multi_w_wload(m)
    m.write <+ [[100, 'foo', 'baz']]
    3.times{ m.tick }
    m.write <+ [[100, 'peter', 'thane of cawdor']]
    m.commit <+ [[100]]
    m.seal_write_xid <+ [[100]]
    4.times{ m.tick }
  end 

  def setup_multiwrite(m)
    pre_writes = [["foo", "bar", 0],
                  [2, "peter", "thane of glamis", 0],
                  [3, "banquo", "dead but gets kings", 0]]

    m.commit_log <+ [[0, -1]]
    m.tick
    pre_writes.each_with_index do |w, i|
      indx = i + 1
      m.commit_log <+ [[indx, i]]
      m.commit <+ [[indx]]
      m.seal_write_xid <+ [[indx]]
      m.tick
    end
  end 


  def test_multiwrite
    m = MultiWrite.new
    setup_multiwrite(m)
    m.tick; m.tick
    assert_equal([[1, "foo", "bar", 0],[2, "peter", "thane of glamis", 0], [3, "banquo", "dead but gets kings", 0]], m.live.to_a.sort)
    multi_w_wload(m)
    assert_equal([[3, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1],[100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)

    # confirm that the redundant entries in writes() have been GC'd
    assert_equal([], s.write.to_a)

    # confirm that the foo=bar write_log entry has been GC'd
    #assert_equal([[2, 'peter', 'thane of glamis', 0], [3, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1]], s.write_log.to_a.sort)
  end

  def test_itall
    m = MultiReadWrite.new
    setup_multiwrite(m)
    # pre-write set
    assert_equal([[1, "foo", "bar", 0],[2, "peter", "thane of glamis", 0], [3, "banquo", "dead but gets kings", 0]], m.live.to_a.sort)
    #m.tick
    m.read <+ [[200, 'foo',], [200, 'peter']]
    m.tick
    multi_w_wload(m)
    assert_equal([[3, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1],[100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)
    # write GC'd
    assert_equal([], s.write.to_a)
    m.read <+ [[300, 'peter'], [300, 'foo']]
    m.tick
    m.read <+ [[200, 'banquo']]


    assert_equal([[3, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1],[100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)
    m.commit <+ [[200]]
    m.seal_commit_xid <+ [[200]]
    4.times{m.tick}
    assert_equal([], s.read.to_a)
    assert_equal([[300, 3, "banquo", "dead but gets kings", 0], [300, 100, "foo", "baz", 1], [300, 100, "peter", "thane of cawdor", 2]], m.pinned_writes.to_a.sort)
    m.commit <+ [[300]]
    m.seal_commit_xid <+ [[300]]
    4.times{m.tick}
    assert_equal([], m.pinned_writes.to_a)
    #assert_equal([], m.commit.to_a)
    assert_equal([[3, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1], [100, "peter", "thane of cawdor", 2]], m.write_log.to_a.sort)

  end

  def test_concurrent_writes
    s = SimpleWrite.new
    s.tick
    s.write <+ [[100, 'foo', 'baz']]
    assert_raises(Bud::KeyConstraintError){ s.write <+ [[100, 'foo', 'qux']] }
    s.write <+ [[101, 'foo', 'qux']]
    #s.tick;s.tick
    s.live.each{|l| puts "L : #{l}"}
  

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
