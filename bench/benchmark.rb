#!/usr/bin/env ruby
require "rubygems"
require "benchmark"
require "bud"
require_relative 'causal-dict'

# First creates insertions with no dependencies. 
# All live inserties are kept track of in "possible_dependencies".
# For updates that should causally depend on another insertion,
# a dependency is picked at random from "possible_dependencies".
# "possible_dependencies" is then updated to only include the insertion
# that dominated the selected dependency.

def gen_data(num_writes, percent_update)
  num_updates = ((percent_update.to_f / 100) * num_writes).to_i
  num_orig_writes = num_writes - num_updates
  possible_dependencies = Hash.new
  id = 0
  data = []
  latest_updates = []
  num_orig_writes.times do
    write = [id, id, id, []]    
    data << write
    possible_dependencies[id] = [id, id, []]
    latest_updates << id
    id += 1
  end
  num_updates.times do
    random_dependency_id = latest_updates.sample(1)[0]
    latest_updates.delete(random_dependency_id)
    latest_updates << id
    dep = possible_dependencies[random_dependency_id]
    deps = dep[2] << random_dependency_id
    write = [id, dep[0], dep[1] + 1, deps]
    possible_dependencies[id] = [dep[0], dep[1] + 1, deps]
    id += 1
    data << write
  end
  return data
end

def no_partition_bench(data)
  c = CausalDict.new
  c.log <+ data
  # How many times should we tick?
  50.times { c.tick }
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
  50.times { c.tick }
  storage << a.num_tuples
  storage << b.num_tuples
  a.start_communication
  b.start_communication
  50.times { c.tick }
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

  $stderr.printf("%d %d %d\n",
                 size, percent, space_used)
end

raise ArgumentError, "Usage: bench.rb number_updates percent_update variant" unless ARGV.length == 3
size, percent, variant = ARGV
bench(size.to_i, percent.to_i, variant)

#gen_data(100, 50)
#bench(1000, 70, "no_partition")
