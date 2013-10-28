#!/usr/bin/env ruby
require "rubygems"
require "benchmark"
require "bud"
require_relative 'causal-dict'

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
  #p data
  data
end

def gen_dom_data(size, start_index)
  data = []
  deps = []
  size.times do |i|
    data << [i + start_index, "foo", "bar#{i}", deps]
    deps = [i + start_index]
  end
  data
end

def any_pending?(bud)
  bud.tables.each_value do |t|
    return true if t.pending_work?
  end
  false
end

def no_partition_bench(data)
  c = CausalDict.new
  c.log <+ data
  loop do
    c.tick
    break unless any_pending? c
  end
  raise unless c.log.to_a.empty? and c.dominated.to_a.empty? and c.safe.physical_size == 1
  #p c.num_tuples
  c.num_tuples
end

def partition_bench(data1, data2)
  sites = make_cluster
  a = sites.first
  b = sites.last
  a.run_bg
  b.run_bg
  storage_before = -1
  storage_after = -1
  a.disconnect_channels
  b.disconnect_channels
  a.log <+ data1
  b.log <+ data2
  loop do
    a.tick
    b.tick
    break unless any_pending? a
    break unless any_pending? b
  end
  storage_before = a.num_tuples

  a.connect_channels
  b.connect_channels
  data1.size.times { a.tick }
  data2.size.times { b.tick }
  sleep 2
  raise unless a.log.to_a.empty? and a.dominated.to_a.empty? and a.safe.physical_size == 1
  raise unless b.log.to_a.empty? and b.dominated.to_a.empty? and b.safe.physical_size == 1
  storage_after = a.num_tuples
  p storage_before
  p storage_after
  return storage_before, storage_after
end

def make_cluster
  ports = (1..2).map {|i| i + 10001}
  addrs = ports.map {|p| "localhost:#{p}"}
  rlist = ports.map {|p| CausalDict.new(:ip => "localhost", :port => p)}
  rlist.each {|r| r.node <+ addrs.map {|a| [a]}}
  rlist
end

def bench(size, percent, variant)
  data = gen_data(size, percent)
  puts "Run #: size = #{size}, # percent = #{percent}, variant = #{variant}"

  case variant
  when "no_partition"
    space_used = no_partition_bench(data)
    $stderr.printf("%d %d %d\n", size, percent, space_used)
  when "partition"
    data1 = gen_data(size, percent)
    data2 = gen_data(size, percent)
    space_used_before, space_used_after = partition_bench(data1, data2)
    $stderr.printf("%d %d %d %d\n", size, percent, space_used_before, space_used_after)
  else
    raise "Unrecognized variant: #{variant}"
  end

  
end

raise ArgumentError, "Usage: bench.rb number_updates percent_update variant" unless ARGV.length == 3
size, percent, variant = ARGV
bench(size.to_i, percent.to_i, variant)

# vbggen_data(20, 50)
#bench(100, 70, "partition")
