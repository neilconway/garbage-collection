require 'rubygems'
gem 'minitest'
require 'minitest/autorun'
require './atomic_registers_complete'

def boots(cls)
  cls.write <+ [[1, 1, "foo", "bar"],
            [2, 2, "peter", "thane of glamis"],
            [3, 3, "banquo", "dead but gets kings"]]
  if cls.tables.keys.include? :seal_write_batch
    cls.commit <+ [[1],[2],[3]]
    cls.seal_write_batch <+ [[1],[2],[3]] 
  end
  cls.tick; cls.tick
end

class SimpleWrite
  include Bud
  include AtomicRegister
end

class MultiWrite
  include Bud
  include AtomicBatchWrites
end

class MultiReadWrite
  include Bud
  include AtomicBatchWrites
  include MinimalCopy
  #include HWM
  #include ReadTabs
  #include AnotherApproach
end

class TestMVCCs < Minitest::Test
  def do_write(s, w)
    s.write <+ [w]
    3.times{ s.tick }
  end

  def do_commit(s, i, wids=[])
    s.commit <+ [[i]]
    s.seal_write_batch <+ [[i]]
    unless wids.empty?
      wids.each do |w|
        s.seal_write_log_wid <+ [[w]] if s.tables.keys.include? :seal_write_log_wid
        s.seal_snapshot_wid <+ [[w]] if s.tables.keys.include? :seal_snapshot_wid
      end
    end
    3.times{ s.tick }
  end

  def test_simple
    s = SimpleWrite.new
    boots(s)
    assert_equal([[1, 1, 'foo', 'bar', 0], [2, 2, 'peter', 'thane of glamis', 0], [3, 3, "banquo", "dead but gets kings", 0]], s.live.to_a.sort)
    do_write(s, [100, 100, 'foo', 'baz'])
    assert_equal([[2, 2, 'peter', 'thane of glamis', 0], [3, 3, "banquo", "dead but gets kings", 0], [100, 100, 'foo', 'baz', 1]], s.live.to_a.sort)
    # confirm that the redundant entries in writes() have been GC'd
    assert_equal([], s.write.to_a)
    # confirm that the foo=bar write_log entry has been GC'd
    assert_equal([[2, 2, 'peter', 'thane of glamis', 0], [3, 3, "banquo", "dead but gets kings", 0], [100, 100, 'foo', 'baz', 1]], s.write_log.to_a.sort)
  end

  def multi_w_wload(m)
    do_write(m, [10, 100, 'foo', 'baz'])
    do_write(m, [11, 100, 'peter', 'thane of cawdor'])
    do_commit(m, 100, [10,11])
  end 

  def multi_wload2(m)
    (1..20).each do |i|
      do_write(m, [1000 + i, 'foo', i.to_s])
      do_commit(m, 1000 + i)
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
      m.seal_write_batch <+ [[indx]]
      m.seal_write_log_wid <+ [[1], [2], [3]] if m.tables.keys.include? :seal_write_log_wid
      m.seal_snapshot_wid <+ [[1], [2], [3]] if m.tables.keys.include? :seal_snapshot_wid
      m.tick
    end
  end 

  def test_multiwrite
    m = MultiWrite.new
    boots(m)
    setup_multiwrite(m)
    assert_equal([[1, 1, "foo", "bar", 0],[2, 2, "peter", "thane of glamis", 0], [3, 3, "banquo", "dead but gets kings", 0]], m.live.to_a.sort)
    multi_w_wload(m)
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1],[11, 100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)

    # confirm that the redundant entries in writes() have been GC'd
    assert_equal([], s.write.to_a)
    # confirm that the foo=bar write_log entry has been GC'd
    #assert_equal([[2, 'peter', 'thane of glamis', 0], [3, "banquo", "dead but gets kings", 0], [100, 'foo', 'baz', 1]], s.write_log.to_a.sort)
  end

  def do_read(inst, id, name)
    @readid ||= 1000
    @readid += 1
    inst.read <+ [[id, name]]
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
    do_read_commit(m, 200)

  
    #m.read_commit <+ [[200]]
    2.times{m.tick}
    assert_equal([], s.read.to_a)
  end
  
  def do_read_commit(m, bid)
    m.read_commit <+ [[bid]]
  end

  def test_readpath
    m = MultiReadWrite.new(:print_rules => true)
    boots(m)
    multiread_common(m)
    # irrelevant entries GC'd

    m.read_live.each{|l| puts "READ LIVE: #{l}"}

    #assert_equal([[300, 3, 3, "banquo", "dead but gets kings", 0], [300, 10, 100, "foo", "baz", 1], [300, 11, 100, "peter", "thane of cawdor", 2]], m.snapshot.to_a.sort, "irrelevant snapshot entries")
    #assert_equal([[300, 3, 3, "banquo", "dead but gets kings", 0], [300, 10, 100, "foo", "baz", 1], [300, 11, 100, "peter", "thane of cawdor", 2]], m.read_live.to_a.sort, "irrelevant snapshot entries")
    m.read_commit <+ [[300]]
    2.times{m.tick}
    assert_equal([], m.snapshot.to_a)
    assert_equal([], m.read.to_a)
    assert_equal([], m.write.to_a)
    assert_equal([], m.commit.to_a)
    assert_equal(m.live.to_a.length, m.write_log.to_a.length)
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1], [11, 100, "peter", "thane of cawdor", 2]], m.write_log.to_a.sort)
    assert_equal([[3, 3, "banquo", "dead but gets kings", 0], [10, 100, "foo", "baz", 1], [11, 100, "peter", "thane of cawdor", 2]], m.live.to_a.sort)
    multi_wload2(m)
  end

  def test_concurrent_writes
    s = SimpleWrite.new
    boots(s)
    s.write <+ [[100, 'foo', 'baz']]
    assert_raises(Bud::KeyConstraintError){ s.write <+ [[100, 'foo', 'qux']] }
    s.write <+ [[101, 'foo', 'qux']]
    m = MultiWrite.new
    boots(m)
    m.write <+ [[100, 'foo', 'baz']]
    assert_raises(Bud::KeyConstraintError){ m.write <+ [[100, 'foo', 'qux']] }
  end

  def read_name(inst, name)
    inst.live.each do |l| 
      if l.name == name
        return l
      end
    end
    return nil
  end

  def test_snapshot_anomaly
    m = MultiWrite.new
    boots(m)

    results = {}
    results['peter'] = read_name(m, 'peter')
    multi_w_wload(m)
    results['foo'] = read_name(m, 'foo')

    assert_equal('thane of glamis', results['peter'].val)
    assert_equal('baz', results['foo'].val)
  end
end
