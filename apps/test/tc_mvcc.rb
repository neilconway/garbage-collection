require_relative '../mvcc'
gem 'minitest'  
require 'minitest/autorun'



class TestKVS < MiniTest::Test
  def get_action_version(obj, aid, action_set, log = false)
    ver = Set.new([aid])
    obj.current.each do |cur|
      if action_set.include? cur.key
        ver = ver.union cur.deps
        if log
          obj.inquiry_result <+ [[aid, cur.key, cur.deps]]
        end
      end  
    end
    obj.tick if log
    ver.to_a
  end

  def seeds(obj)
    obj.current <+ [[0, "foo", "bar", [0], :write]]
    obj.current <+ [[0, "boom", "bip", [0], :write]]
    obj.current <+ [[0, "peter", "king", [0], :write]]
    obj.tick
  end

  def new_action_id
    @action_id ||= 0
    @action_id += 1
    return @action_id
  end

  def commit_writes(obj, write_set, read_set)
    aid = new_action_id
    vers = get_action_version(obj, aid, write_set.keys + read_set)
    write_set.each_pair do |k, v|
      obj.log <+ [[aid, k, v, vers, :write]]
    end
    obj.tick
  end

  def read_inquiry(obj, read_set)
    aid = new_action_id
    vers = get_action_version(obj, aid, read_set, true)
    [aid, vers]
  end

  def commit_reads(obj, aid, vers, read_set)
    read_set.each do |r|
      obj.log <+ [[aid, r, nil, vers, :read]]
    end
    obj.tick
  end

  def read_version(obj, key, vers)
    obj.hot_keys.each do |k|
      
    end
  end

  def test_1
    m = M.new
    seeds(m)
    commit_writes(m, {"foo" => "bip", "peter" => "prince"}, ["boom"])
    commit_writes(m, {"testnew" => "test"}, [])

    m.current.each do |c| 
      case c.key
        when "foo"
          assert_equal([0,1], c.deps.sort)
          assert_equal("bip", c.val)
        when "testnew"
          assert_equal([2], c.deps)
      end
    end

    #assert_equal(
    a, v = read_inquiry(m, ["foo", "testnew"])
    assert_equal(2, m.hot_versions.to_a.length)
    commit_writes(m, {"foo" => "bimsala"}, [])
    assert_equal(2, m.hot_versions.to_a.length)
    m.hot_versions.each do |v|
      assert_equal("bip", v.val) if v.key == "foo"
    end

    commit_reads(m, a, v, ["foo", "testnew"])

    assert_equal(0, m.hot_versions.to_a.length)

    m.current.each do |c|
      puts "CURR #{c}"
    end
  end
end
