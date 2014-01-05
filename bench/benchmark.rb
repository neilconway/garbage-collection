#!/usr/bin/env ruby
require "rubygems"
require "benchmark"
require "bud"
require_relative '../apps/causal-kvs'

# Data generation for partition experiment.
# Each tuple dominates the last.
def gen_dom_data(size, start_index)
  data = []
  deps = []
  size.times do |i|
    data << [i + start_index, "foo", "bar#{i}", deps]
    deps = [i + start_index]
  end
  p data
  data
end

def gen_dom_data_less_overhead(size)
  data = []
  deps = [[], [], [], [], []]
  (0..(size-2)).step(2) do |i|
    p i
    data << [i, "key0", "bar#{i}", deps[0]]
    deps[0] = [i]
    data << [i + 1, "key1", "bar#{i + 1}", deps[1]]
    deps[1] = [i + 1]
    #data << [i + 2, "key2", "bar#{i + 2}", deps[2]]
    #deps[2] = [i + 2]
    #data << [i + 3, "key3", "bar#{i + 3}", deps[3]]
    #deps[3] = [i + 3]
    #data << [i + 4, "key4", "bar#{i + 4}", deps[4]]
    #deps[4] = [i + 4]
  end
  #p data
  data
end

# Data generation for non-partition experiment.
# Variable percentage of "dominated" tuples
def gen_incremental_update(size, percent)
  data = []
  id = 0
  num_updates = ((percent.to_f / 100) * 10).to_i
  num_orig = 10 - num_updates
  (size/10).times do
    update_id = id
    dep_id = id
    num_orig.times { 
      data << [id, "foo#{id}", id, []]
      id += 1
    }
    num_updates.times {
      data << [id, "foo#{update_id}", id, [dep_id]]
      dep_id = id
      id += 1
    }
  end
  data
end

def any_pending?(bud)
  bud.tables.each_value do |t|
    next if t.tabname == :dep
    next if t.tabname == :safe_dep
    #p t.tabname
    #return true if t.pending_work?
    if t.pending_work?
      #p t.tabname
      return true
    end
  end
  false
end

def any_pending_steady?(bud)
  bud.tables.each_value do |t|
    #p t.tabname
    #return true if t.pending_work?
    if t.pending_work?
      #p t.tabname
      return true
    end
  end
  false
end


def no_partition_bench(data, percent)
  d = data.reverse
  c = CausalKvsReplica.new
  storage = []
  converge_point = data.size - ((percent.to_f / 100) * data.size).to_i
  start = Time.now.to_f
  loop do
    before_insert = Time.now.to_f
    p before_insert
    
    batch = d.pop(50).reverse
    batch.each do |b|
      if b[3] == []
        c.do_write(b[0], b[1], b[2])  
      else
        c.do_write(b[0], b[1], b[2], b[3])
      end
    end  

    while any_pending?(c)
      c.tick
    end
    p "Tuples left: #{d.size}"
    storage << [(start - Time.now.to_f).abs, num_tuples(c)]
    elapsed = (before_insert - Time.now.to_f).abs
    if elapsed < 1
      sleep 1 - elapsed
    end
    if (start - Time.now.to_f).abs > 30
      break
    end
  end
  storage
end

def partition_bench(size)
  rlist = make_cluster
  first = rlist.first
  last = rlist.last
  storage = []
  tick_info = []
  start = Time.now.to_f
  #data = gen_dom_data_less_overhead(size).reverse
  data = gen_dom_data(size, 0).reverse
  rate = []
  
  disconnect1 = true
  connect1 = false
  disconnect2 = true
  connect2 = false
  
  disconnect_time = -1
  connect_time = -1
  disconnect_time2 = -1
  connect_time2 = -1

  data.size.times {
    #insert = data.pop(2).reverse
    #p insert
    #insert.each do |i|
    #  if i[3] == []
    #    first.do_write(i[0], i[1], i[2])  
    #  else
    #    first.do_write(i[0], i[1], i[2], i[3])
    #  end
    #end

    insert = data.pop
    p insert

    if insert[3] == []
      first.do_write(insert[0], insert[1], insert[2])  
    else
      first.do_write(insert[0], insert[1], insert[2], insert[3])
    end

    rate << Time.now.to_f
    p "Data size: #{data.size}"
    before_any_pending = Time.now.to_f
    num_ticks = 0
    tick_time = 0

    t = (start - Time.now.to_f).abs
    if t < 30
      while any_pending_steady?(first) or any_pending_steady?(last)
        rlist.each(&:tick)
      end 
    elsif t > 30 and t < 81
      while any_pending?(first) or any_pending?(last)
        rlist.each(&:tick)
      end
    elsif t > 81 and t < 140
      while any_pending_steady?(first) or any_pending_steady?(last)
        rlist.each(&:tick)
      end
    elsif t > 140 and t < 170
      while any_pending?(first) or any_pending?(last)
        rlist.each(&:tick)
      end
    else
      while any_pending_steady?(first) or any_pending_steady?(last)
        rlist.each(&:tick)
      end
    end

    #while any_pending?(first) or any_pending?(last)
      #num_ticks += 1
      #rlist.each(&:tick)
      #before_first = Time.now.to_f
    #  first.tick
      #after_first = Time.now.to_f
      #tick1_time = after_first - before_first
      #p "Time first tick: #{tick1_time}"
      
      #before_second = Time.now.to_f
    #  last.tick
      #after_second = Time.now.to_f
      #tick2_time = after_second - before_second
      #p "Time second tick: #{tick2_time}"

      #tick_time = tick_time + tick2_time + tick1_time
      #sleep 0.1;
    #end

    #25.times {rlist.each(&:tick)}

    #after_any_pending = Time.now.to_f
    #p "Time any pending: #{after_any_pending - before_any_pending}"
    #p "Time ticking: #{tick_time}"
    #tick_info << [num_ticks, (tick_time/num_ticks.to_f).to_f]


    sleep 0.5;
    storage << [(start - Time.now.to_f).abs, num_tuples(first)] 
    p (start - Time.now.to_f).abs
    
    if (start - Time.now.to_f).abs > 30 and disconnect1 == true
      p "disconnect1"
      disconnect_time = (start - Time.now.to_f).to_f
      first.disconnect_channels
      last.disconnect_channels
      disconnect1 = false
      connect1 = true
    end

    if (start - Time.now.to_f).abs > 80 and connect1 == true
      p "connect1"
      connect_time = (start - Time.now.to_f).to_f
      first.connect_channels
      last.connect_channels
      connect1 = false
    end

    if (start - Time.now.to_f).abs > 140 and disconnect2 == true
      p "disconnect2"
      disconnect_time2 = (start - Time.now.to_f).to_f
      first.disconnect_channels
      last.disconnect_channels
      disconnect2 = false
      connect2 = true
    end

    if (start - Time.now.to_f).abs > 170 and connect2 == true
      p "connect2"
      connect_time2 = (start - Time.now.to_f).to_f
      first.connect_channels
      last.connect_channels
      connect2 = false
    end

    puts "VIEW: #{first.view.to_set.inspect}"
    puts "log: #{first.log.to_a.size}"
    puts "dep: #{first.dep.to_a.size}"
    puts "safe_dep: #{first.safe_dep.to_a.size}"
    puts "dom: #{first.dom.to_a.size}"
    puts "safe: #{first.safe.to_a.size}"

    if (start - Time.now.to_f).abs > 200
      break
    end
  }
  #p storage
  p rate
  return storage, disconnect_time, connect_time, disconnect_time2, connect_time2, tick_info
end

def make_cluster
  ports = (1..2).map {|i| i + 10001}
  addrs = ports.map {|p| "localhost:#{p}"}
  rlist = ports.map {|p| CausalKvsReplica.new(:ip => "localhost", :port => p, :print_rules => true)}
  rlist.each {|r| r.node <+ addrs.map {|a| [a]}}
  rlist
end

def num_tuples(bud)
  puts "Log: #{bud.log.to_a.size}"
  #puts "dep_chn_approx: #{bud.dep_chn_approx.physical_size}"
  sizes = bud.app_tables.map do |t|
    if t.kind_of? Bud::BudRangeCompress
      t.physical_size
    elsif t.kind_of? Bud::BudTable
      t.to_a.size
    else
      0
    end
  end
  p sizes.reduce(:+)
  sizes.reduce(:+)
end

def bench(size, percent, variant)
  puts "Run #: size = #{size}, # percent = #{percent}, variant = #{variant}"
  case variant
  when "partition"
    storage, disconnect, connect, disconnect2, connect2, tick_info = partition_bench(size)
    $stderr.printf("%f\n", disconnect.abs)
    $stderr.printf("%f\n", connect.abs)
    $stderr.printf("%f\n", disconnect2.abs)
    $stderr.printf("%f\n", connect2.abs)
    storage.each do |s|
      $stderr.printf("%f %d\n", s[0], s[1])
    end
    #tick_info.each do |t|
    #  $stderr.printf("%f %f\n", t[0], t[1])
    #end
  when "no_partition"
    data = gen_incremental_update(size, percent)
    space_used = no_partition_bench(data, percent)
    space_used.each do |s|
      $stderr.printf("%f %d\n", s[0], s[1])
    end
  else
    raise "Unrecognized variant: #{variant}"
  end  
end

raise ArgumentError, "Usage: bench.rb number_updates percent_update variant" unless ARGV.length == 3
size, percent, variant = ARGV
bench(size.to_i, percent.to_i, variant)

#p gen_dom_data_less_overhead(1000)
#gen_dom_data(100, 0)
