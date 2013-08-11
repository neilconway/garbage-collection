require 'rubygems'
require 'minitest/autorun'

require './causalkvs.rb'


class CK
  include Bud
  include ClientSide
  include InternalKVS
  include Rep

  state do
    table :get_alog, get_response.schema
  end
  
  bloom do
    get_alog <= get_response{|r| r}
    stdio <~ get_alog{|g| ["GETLOG: #{g}"]}
  end
end


class TestCK < MiniTest::Unit::TestCase
  def put(c, d)
    c.sync_do {
      c.put <+ [d]
      c.tick 
    }
  end

  def get(c, d)
    c.sync_do {
      c.get <+ [d]
      c.tick 
    }
  end

  def sync_get(c, d)
    # for convenience
    c.sync_callback(:get, [d], :get_response) do |cb|
      cb.each do |row|
        if row.deps == d.last
          return row
        end
      end
    end
  end

  def start_cks(ports, trc)
    nodes = ports.map{|p| CK.new(:port => p, :trace => trc)}
    nodes.each do |c|
      nodes.each_with_index do |n, i|
        c.replicas << [i, n.ip_port]
      end
      puts "RUN #{c.ip_port}"
      c.run_bg
    end
    nodes 
  end

  def stop_cks(nodes)
    nodes.each{|n| n.stop_bg}
  end
  
  def test_ck
    plorts = [1234]
    trc = false
    nodes = start_cks(plorts, trc)
    f = nodes.first
    put(f, ["foo", "bar"])
    put(f, ["foo", "baz"])

    # this would be 1024
    #put(f, ["foo", "bim"])

    get(f, ["foo"])

    f.tick; f.tick

    #sleep 0.5

    assert_equal(["foo", "baz", 768], f.get_alog.to_a.first)


    f.sync_do {
      f.get_internal("foo", {"foo" => 1024})
    }
    
    assert_equal(1, f.get_alog.to_a.length)

    # this would be 1024
    put(f, ["foo", "bim"])
    #f.tick

    f.put_log.each{|p| puts "PL #{p}"}

    assert_equal(2, f.get_alog.to_a.length)
    assert_equal([["foo", "baz", 768], ["foo", "bim", 1024]], f.get_alog.to_a.sort)
    


    #f.sync_do{f.put_response <+ [[:foo, :qux]]}

    put(f, ["bar", "qux"])
    stop_cks(nodes)
  end

  def test_multikey
    nodes = start_cks([1234, 1235, 1236], false)
    f = nodes.first
    g = nodes[1]
    h = nodes[2]


    assert_equal([["DUMMY", 1]], g.context.to_a.sort)

    put(f, ["location", "fuck mountain"])
    put(f, ["kingshit", "peter"])

    res = sync_get(f, ["kingshit"])
    puts "RES #{res}"


    res2 = sync_get(g, ["kingshit"])
    put(g, ["witness", "king"])
    assert_equal([["DUMMY", 1], ["kingshit", 768], ["witness", 1025]], g.context.to_a.sort)
    assert_equal([["DUMMY", 1], ["kingshit", 768], ["location", 512]], f.context.to_a.sort)

    f.put_log.each{|l| puts "PL: #{l}"}


    #puts "h is #{h.ip_port}"
    assert_equal([["DUMMY", 1]], h.context.to_a.sort)

    # this guy is (logically) "partitioned away"
    guy = CK.new(:port => 1237)
    guy.replicas << [5, guy.ip_port]
    guy.run_bg

    put(guy, ["kingshit", "this other guy"])

    guy.tick



    # "heal the partition"
    guy.sync_do {
      nodes.each_with_index do |n, i|
        guy.replicas <+ [[i, n.ip_port]]
      end
    }
    nodes.each do |n|
      n.sync_do{n.replicas <+ [[5, guy.ip_port]]}
    end

    sleep 2

    guy.put_log.each{|l| puts "PUTLOGGG: #{l}"}

    res = sync_get(f, ["kingshit"])

    puts "RES is #{res}"

        
    stop_cks(nodes)
  end


end
