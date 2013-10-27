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
  p data
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
  p c.num_tuples
  c.num_tuples
end

def partition_bench(data)
  sites = make_cluster
  a = sites.first
  b = sites.last
  storage = []
  sliced_data = data.each_slice(data.size/2)
  a.log <+ sliced_data[0]
  b.log <+ sliced_data[1]
  a.stop_communication
  b.stop_communication
  500.times { c.tick }
  storage << a.num_tuples
  storage << b.num_tuples
  a.start_communication
  b.start_communication
  500.times { c.tick }
  storage << a.num_tuples
  storage << b.num_tuples
end

def make_cluster
  ports = (1..2).map {|i| i + 10001}
  addrs = ports.map {|p| "localhost:#{p}"}
  rlist = ports.map {|p| CausalDict.new(@@opts.merge(:ip => "localhost", :port => p))}
  rlist.each {|r| r.node <+ addrs.map {|a| [a]}}
  rlist
end

def bench(size, percent, variant)
  data = gen_data(size, percent)
  puts "Run #: size = #{size}, # percent = #{percent}, variant = #{variant}"

  case variant
  when "no_partition"
    space_used = no_partition_bench(data)
  when "partition"
    # work in progress
  else
    raise "Unrecognized variant: #{variant}"
  end

  $stderr.printf("%d %d %d\n", size, percent, space_used)
end

raise ArgumentError, "Usage: bench.rb number_updates percent_update variant" unless ARGV.length == 3
size, percent, variant = ARGV
bench(size.to_i, percent.to_i, variant)

# vbggen_data(20, 50)
#bench(1000, 70, "no_partition")
