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

def partition_bench2(size, percent, partition)
  rlist = make_cluster
  first = rlist.first
  last = rlist.last
  storage = []
  start = Time.now.to_f
  data = gen_data(size, percent)
  data1 = data[0..data.size/2]
  
  first.log <+ data1
  (data.size/2 + 5).times {
    #first.log <+ [data1.pop] 
    rlist.each(&:tick); 
    sleep 0.1; 
    storage << [(start - Time.now.to_f).abs, first.num_tuples] 
  }
  data2 = data[data.size/2..-1]
  
  disconnect_time = start - Time.now.to_f
  if partition
    first.disconnect_channels
    last.disconnect_channels
  end

  #first.log <+ data2
  (data.size/2 + 5).times { 
    first.log <+ [data2.pop]
    rlist.each(&:tick); 
    sleep 0.1; 
    storage << [(start - Time.now.to_f).abs, first.num_tuples] 
  }

  connect_time = start - Time.now.to_f
  if partition
    first.connect_channels
    last.connect_channels
  end

  (data.size/2 + 5).times { rlist.each(&:tick); sleep 0.1; storage << [(start - Time.now.to_f).abs, first.num_tuples] }
  return storage, disconnect_time, connect_time
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
    storage, disconnect, connect = partition_bench2(size, percent, true)
    $stderr.printf("%f\n", disconnect.abs)
    $stderr.printf("%f\n", connect.abs)
    storage.each do |s|
      $stderr.printf("%f %d\n", s[0], s[1])
    end
  when "partition_base"
    storage, disconnect, connect = partition_bench2(size, percent, false)
    $stderr.printf("%f\n", disconnect.abs)
    $stderr.printf("%f\n", connect.abs)
    storage.each do |s|
      $stderr.printf("%f %d\n", s[0], s[1])
    end
  else
    raise "Unrecognized variant: #{variant}"
  end  
end

raise ArgumentError, "Usage: bench.rb number_updates percent_update variant" unless ARGV.length == 3
size, percent, variant = ARGV
bench(size.to_i, percent.to_i, variant)

#partition_bench2(100, 20)
#bench(100, 20, "partition")