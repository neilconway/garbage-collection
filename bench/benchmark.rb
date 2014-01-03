#!/usr/bin/env ruby
require "rubygems"
require "benchmark"
require "bud"
require_relative '../apps/causal-kvs'

def gen_data(size, percent)
  data = []
  num_updates = ((percent.to_f / 100) * size).to_i
  num_orig_writes = size - num_updates
  id = 0
  num_orig_writes.times do
    write = [id, id, id, []]    
    data << write
    id += 1
  end
  data = data | gen_dom_data(num_updates, id)
  data
end

def old_gen_dom_data(size, start_index)
  data = []
  deps = []
  size.times do |i|
    data << [i + start_index, "foo", "bar#{i}", deps]
    deps = [i + start_index]
  end
  p data
  data
end

def gen_dom_data(size, start_index)
  data = []
  deps = []
  seals = []
  size.times do |i|
    data << [i + start_index, "foo", "bar#{i}"]
    deps << [i + start_index, i + start_index - 1]
    seals << [i + start_index]
  end
  deps[0] = []
  return data, deps, seals
end

def gen_incremental_update(size, percent)
  #id, key, val
  data = []
  #id_new, id_dep_on
  deps = []
  #id_new
  seal_ids = []
  id = 0
  num_updates = ((percent.to_f / 100) * 10).to_i
  num_orig = 10 - num_updates
  (size/10).times do
    update_id = id
    dep_id = id
    num_orig.times { 
      data << [id, "foo#{id}", id]
      deps << []
      seal_ids << [id]
      id += 1
    }
    num_updates.times {
      data << [id, "foo#{update_id}", id]
      deps << [id, dep_id]
      seal_ids << [id]
      dep_id = id
      id += 1
    }
  end
  p data
  p deps
  p seal_ids
  return [data, deps, seal_ids]
end

def any_pending?(bud)
  bud.tables.each_value do |t|
    next if t.tabname == :safe_dep
    return true if t.pending_work?
  end
  false
end

def no_partition_bench(data)
  c = CausalKvsReplica.new
  c.log <+ data
  loop do
    c.tick
    break unless any_pending? c
  end
  raise unless c.log.to_a.empty? and c.dominated.to_a.empty? and c.safe.physical_size == 1
  num_tuples(c)
end

def no_partition_bench2(data, percent)
  d = data[0].reverse
  deps = data[1].reverse
  seal_ids = data[2].reverse
  c = CausalKvsReplica.new
  storage = []
  converge_point = data.size - ((percent.to_f / 100) * data.size).to_i
  start = Time.now.to_f
  loop do
    before_insert = Time.now.to_f
    p before_insert
    
    c.log <+ d.pop(50).reverse
    c.dep <+ deps.pop(50).reverse
    c.seal_dep_id <+ seal_ids.pop(50).reverse

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
    #if c.safe_log.to_a.size >= converge_point and c.log.to_a.size == 0
    #  break
    #end
  end
  storage
end

def partition_bench(size)
  rlist = make_cluster
  first = rlist.first
  last = rlist.last
  storage = []
  start = Time.now.to_f
  data = gen_dom_data(size, 0)
  
  d = data[0].reverse
  deps = data[1].reverse
  seals = data[2].reverse
  rate = []
  
  disconnect1 = true
  connect1 = false
  disconnect2 = true
  connect2 = false
  
  disconnect_time = -1
  connect_time = -1
  disconnect_time2 = -1
  connect_time2 = -1

  d.size.times {

    new_log = d.pop
    new_dep = deps.pop
    new_seal = seals.pop

    #p "Log: #{new_log}"
    #p "Dep: #{new_dep}"
    #p "Seal: #{new_seal}"

    if new_dep == []
      first.do_write(new_log[0], new_log[1], new_log[2])  
    else
      first.do_write(new_log[0], new_log[1], new_log[2], [new_dep[1]])
    end

    #first.log <+ [new_log]
    #first.dep <+ [new_dep]
    #first.seal_dep_id <+ [new_seal]
    rate << Time.now.to_f
    p "Data size: #{d.size}"
    #p "Total size: #{num_tuples(first)}"
    while any_pending?(first) or any_pending?(last)
      rlist.each(&:tick)
    end

    sleep 0.1;
    storage << [(start - Time.now.to_f).abs, num_tuples(first)] 
    p (start - Time.now.to_f).abs
    
    if (start - Time.now.to_f).abs > 30 and disconnect1 == true
      #p "disconnect1"
      disconnect_time = (start - Time.now.to_f).to_f
      first.disconnect_channels
      last.disconnect_channels
      disconnect1 = false
      connect1 = true
    end

    if (start - Time.now.to_f).abs > 80 and connect1 == true
      #p "connect1"
      connect_time = (start - Time.now.to_f).to_f
      first.connect_channels
      last.connect_channels
      connect1 = false
    end

    if (start - Time.now.to_f).abs > 140 and disconnect2 == true
      #p "disconnect2"
      disconnect_time2 = (start - Time.now.to_f).to_f
      first.disconnect_channels
      last.disconnect_channels
      disconnect2 = false
      connect2 = true
    end

    if (start - Time.now.to_f).abs > 170 and connect2 == true
      #p "connect2"
      connect_time2 = (start - Time.now.to_f).to_f
      first.connect_channels
      last.connect_channels
      connect2 = false
    end

    #puts "VIEW: #{first.view.to_set.inspect}"
    #puts "log: #{first.log.to_a.size}"
    #puts "dep: #{first.dep.to_a.size}"
    #puts "safe_dep: #{first.safe_dep.to_a.size}"
    #puts "dom: #{first.dom.to_a.size}"
    #puts "safe: #{first.safe.to_a.size}"

    if (start - Time.now.to_f).abs > 200
      break
    end
  }
  #p storage
  p rate
  return storage, disconnect_time, connect_time, disconnect_time2, connect_time2
end

def make_cluster
  ports = (1..2).map {|i| i + 10001}
  addrs = ports.map {|p| "localhost:#{p}"}
  rlist = ports.map {|p| CausalKvsReplica.new(:ip => "localhost", :port => p)}
  rlist.each {|r| r.node <+ addrs.map {|a| [a]}}
  rlist
end

def num_tuples(bud)
  puts "Log: #{bud.log.to_a.size}"
  #puts "safe_log: #{bud.safe_log.to_a.size}"
  #puts "safe: #{bud.safe.physical_size}"
  puts "dep_chn_approx: #{bud.dep_chn_approx.physical_size}"
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
  when "no_partition_old"
    data = gen_data(size, percent)
    space_used = no_partition_bench(data)
    $stderr.printf("%d %d %d\n", size, percent, space_used)
  when "partition"
    storage, disconnect, connect, disconnect2, connect2 = partition_bench(size)
    $stderr.printf("%f\n", disconnect.abs)
    $stderr.printf("%f\n", connect.abs)
    $stderr.printf("%f\n", disconnect2.abs)
    $stderr.printf("%f\n", connect2.abs)
    storage.each do |s|
      $stderr.printf("%f %d\n", s[0], s[1])
    end
  when "no_partition_new"
    data = gen_incremental_update(size, percent)
    space_used = no_partition_bench2(data, percent)
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

#p gen_dom_data(100, 0)
#p old_gen_dom_data(10,0)
#p gen_data_2(100, 90)
#p old_gen_incremental_update(100,50)
#gen_incremental_update(10, 50)
