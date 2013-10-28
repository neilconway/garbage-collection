require 'rubygems'
gem 'minitest'
require './mvcc_evo'
require './dev'
#require './test2'

require 'minitest/autorun'

module BaseData
  bootstrap do
    # [:aid] => [:key, :val, :dep]
    write_log <+ [[1, 1, "foo", "bar", 0],
            [2, 2, "peter", "thane of glamis", 0],
            [3, 3, "banquo", "dead but gets kings", 0]]

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
  #include SimpleReadOptimized2
  include BaseData
end

class MultiReadNew
  include Bud
  include HWM
  include MultiKeyWrites
  include AnotherApproach
  include BaseData
end

class PeterTest
  include Bud
  #include PeterApproach
  include BaseData
end

class TestMVCCs < Minitest::Test
  def test_simple
    s = SimpleWrite.new
    s.tick;s.tick
    assert_equal([[1, 1, 'foo', 'bar', 0], [2, 2, 'peter', 'thane of glamis', 0], [3, 3, "banquo", "dead but gets kings", 0]], s.live.to_a.sort)
    s.write <+ [[100, 100, 'foo', 'baz']]
    s.tick; s.tick
    assert_equal([[2, 2, 'peter', 'thane of glamis', 0], [3, 3, "banquo", "dead but gets kings", 0], [100, 100, 'foo', 'baz', 1]], s.live.to_a.sort)
    4.times{ s.tick }

    # confirm that the redundant entries in writes() have been GC'd
    assert_equal([], s.write.to_a)

    # confirm that the foo=bar write_log entry has been GC'd
    assert_equal([[2, 2, 'peter', 'thane of glamis', 0], [3, 3, "banquo", "dead but gets kings", 0], [100, 100, 'foo', 'baz', 1]], s.write_log.to_a.sort)
  end

  def multi_w_wload(m)
    m.write <+ [[10, 100,'foo', 'baz']]
    3.times{ m.tick }
    m.write <+ [[11, 100, 'peter', 'thane of cawdor']]
    m.commit <+ [[100]]
    #m.seal_write_xid <+ [[100]]
    m.seal_write_xact <+ [[100]]
    4.times{ m.tick }
  end 

  def multi_wload2(m)
    (1..20).each do |i|
      m.write <+ [[1000 + i, 'foo', i.to_s]]
      m.commit <+ [[1000 + i]]
      #m.seal_write_xid <+ [[1000 + i]]
      m.seal_write_xact <+ [[1000 + i]]
      4.times{ m.tick }
    end
  end

  def setup_multiwrite(m, clogs=false)
    pre_writes = [[1, 1, "foo", "bar", 0],
                  [2, 2, "peter", "thane of glamis", 0],
                  [3, 3, "banquo", "dead but gets kings", 0]]

    m.commit_log <+ [[0, -1]] if clogs
    m.tick
    pre_writes.each_with_index do |w, i|
      indx = i + 1
      m.commit_log <+ [[indx, i]] if clogs
      m.commit <+ [[indx]]
      #m.seal_write_xid <+ [[indx]]
      m.seal_write_xact <+ [[indx]]
      m.tick
    end
  end 


  def test_multiwrite
    m = MultiWrite.new(:trace => true, :port => 12345)
    setup_multiwrite(m)
    m.tick; m.tick
    assert_equal([[1, 1, "foo", "bar", 0],[2, 2, "peter", "thane of glamis", 0], [3, 3, "banquo", "dead but gets kings", 0]], m.live.to_a.sort)
    multi_w_wload(m)
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1],[11, 100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)

    puts "OK "

    # confirm that the redundant entries in writes() have been GC'd
    assert_equal([], s.write.to_a)

    # confirm that the foo=bar write_log entry has been GC'd
    #assert_equal([[2, 'peter', 'thane of glamis', 0], [3, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1]], s.write_log.to_a.sort)
  end

  def do_read(inst, id, key)
    @readid ||= 1000
    @readid += 1
    #inst.read <+ [[@readid, id, key]]
    inst.read <+ [[id, key]]
    inst.tick
  end

  def multiread_common(m, clog=false)
    setup_multiwrite(m, clog)
    # pre-write set
    assert_equal([[1, 1, "foo", "bar", 0],[2, 2, "peter", "thane of glamis", 0], [3, 3, "banquo", "dead but gets kings", 0]], m.live.to_a.sort)
    do_read(m, 200, 'foo')
    multi_w_wload(m)
    do_read(m, 200, 'peter')
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1],[11, 100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)
    # write GC'd
    assert_equal([], s.write.to_a)
    do_read(m, 300, 'peter')
    do_read(m, 300, 'foo')
    do_read(m, 200, 'banquo')
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1],[11, 100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)
    #m.commit <+ [[200]]

    #assert_equal([[200, 1, 1, "foo", "bar", 0], [200, 2, 2, "peter", "thane of glamis", 0], [200, 3, 3, "banquo", "dead but gets kings", 0]], m.read_view.to_a.sort, "incorrect contents of read_view")
    m.read_commit <+ [[200]]
    ##m.seal_read_commit_xid <+ [[200]]

    ##m.seal_commit_xid <+ [[200]]
    4.times{m.tick}
    assert_equal([], s.read.to_a)
  end

  def test_readpath
    m = MultiReadWrite.new(:print_rules => true, :trace => true, :port => 12346)
    multiread_common(m)
    # irrelevant entries GC'd
    assert_equal([[300, 3, 3, "banquo", "dead but gets kings", 0], [300, 10, 100, "foo", "baz", 1], [300, 11, 100, "peter", "thane of cawdor", 2]], m.snapshot.to_a.sort, "irrelevant snapshot entries")
    #m.commit <+ [[300]]
    #m.seal_commit_xid <+ [[300]]

    m.read_commit <+ [[300]]
    #m.seal_read_commit_xid <+ [[300]]
    4.times{m.tick}
    assert_equal([], m.snapshot.to_a)
    assert_equal([], m.read.to_a)
    assert_equal([], m.write.to_a)
    assert_equal([], m.commit.to_a)
    #assert_equal(1, m.commit_log.to_a.length)
    assert_equal(m.live.to_a.length, m.write_log.to_a.length)

    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1], [11, 100, "peter", "thane of cawdor", 2]], m.write_log.to_a.sort)
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1], [11, 100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)

    multi_wload2(m)
  
    
  end

  def Ntest_newapproach
    m = MultiReadNew.new(:trace => true, :port => 1234, :print_rules => true)
    #m = AnotherApproachCls.new(:trace => true, :port => 1234, :print_rules => true)
    #m = PeterTest.new(:trace => true, :port => 1234, :print_rules => true)
    multiread_common(m, true)
    m.commit <+ [[300]]
   ## m.seal_commit_xid <+ [[300]]
    4.times{m.tick}
    assert_equal([[3, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1], [100, "peter", "thane of cawdor", 2]], m.write_log.to_a.sort)
    assert_equal([[3, "banquo", "dead but gets kings", 0], [100, "foo", "baz", 1], [100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)

    m.read_view.to_a.each{|r| puts "RR #{r}"} 
    

  end

  def Ntest_concurrent_writes
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

  def Ntest_snapshot_anomaly
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
