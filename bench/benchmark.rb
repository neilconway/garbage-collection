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
    return true if t.pending_work?
  end
  false
end

def any_pending_steady?(bud)
  bud.tables.each_value do |t|
    return true if t.pending_work?
  end
  false
end

def in_steady_state?(time)
  if (time < 30) or (time > 81 and time < 140) or (time > 170)
    true
  else
    false
  end
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
  data = gen_dom_data(size, 0).reverse
  
  disconnect1 = true
  connect1 = false
  disconnect2 = true
  connect2 = false
  
  disconnect_time = -1
  connect_time = -1
  disconnect_time2 = -1
  connect_time2 = -1

  data.size.times {
    insert = data.pop
    p insert

    if insert[3] == []
      first.do_write(insert[0], insert[1], insert[2])  
    else
      first.do_write(insert[0], insert[1], insert[2], insert[3])
    end

    p "Data size: #{data.size}"
    before_any_pending = Time.now.to_f

    t = (start - Time.now.to_f).abs
    if in_steady_state?(t)
      while any_pending_steady?(first) or any_pending_steady?(last)
        rlist.each(&:tick)
      end
    else
      while any_pending?(first) or any_pending?(last)
        rlist.each(&:tick)
      end
    end

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

