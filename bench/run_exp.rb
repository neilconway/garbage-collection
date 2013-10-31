#!/usr/bin/env ruby

# Run non-partition experiment
nruns = 2
size = 1000
percents = (0..90).select {|i| i % 10 == 0}
#variants = ["no_partition"]
variants = []
data_files = {}
variants.each {|v| data_files[v] = "#{v}.data"}
log_file = "exp_log"

`rm -f #{log_file}`
data_files.each_pair do |k, fname|
  `echo "#Variant: #{k}\n#Total_inserts percentage_update num_tuples" | cat > #{fname}`
end

percents.each do |p|
  puts "Running benchmark; percent = #{p}"
  variants.each do |v|
    print "  #{v} "
    nruns.times do |t|
      `ruby benchmark.rb #{size} #{p} #{v} >> #{log_file} 2>>#{data_files[v]}`
      raise "Error: #{$?}" unless $?.success?
      print "."
    end
    print "\n"
  end
end

# Run partition experiment
log_file = "exp_partition_log"
`rm -f #{log_file}`
`echo "#Variant: Partitioned Storage\n#Time storage" | cat > partition.data`
puts "Running Partition Experiment"
`ruby benchmark.rb 1000 100 partition >> #{log_file} 2>>partition.data`


module Enumerable
  def sum
    self.inject(0){|accum, i| accum + i }
  end

  def mean
    self.sum/self.length.to_f
  end
end


# Average data from the non-partition experiemtn
# and output to partition.summary
data_files.each_pair do |v, fname|
  File.open("#{v}.summary", "w") do |n|
    n.puts "#Variant: #{v}"
    n.puts "Percent MeanStorage"
    groups = {}
    File.open(fname, "r").each_line do |l|
      next if l =~ /^#/
      fields = l.split(" ")
      num_inserts = fields[0].to_i
      percent = fields[1].to_i
      storage = fields[2].to_i
      groups[percent] ||= []
      groups[percent] << storage 
    end
    groups.keys.sort.each do |k|
      entry = groups[k]
      n.printf("%d %0.6f\n", k , entry.mean)
    end
  end
end

# Extract the time channels were disconnected
# and re-connected from the data. Output to
# partition_time.summary
`rm -f partition_time.summary`
File.open("partition_time.summary", "w") do |n|
  n.puts "#Variant: Partition"
  n.puts "# Time"
  i = 0
  File.open('partition.data', "r").each_line do |l|
    next if l =~ /^#/
    if i >= 4
      break
    end
    fields = l.split(" ")
    i += 1
    if fields.size == 1
      n.printf("%f\n", fields[0])
    end
  end
end

# Extract the (time, storage) pairs from the data
# output to partition_data.summary
`rm -f partition_data.summary`
File.open("partition_data.summary", "w") do |n|
  n.puts "# Variant: Partition"
  n.puts "# Time, Storage"
  File.open('partition.data', "r").each_line do |l|
    next if l =~ /^#/
    fields = l.split(" ")
    if fields.size == 2
      n.printf("%f %f\n", fields[0], fields[1])
    end
  end
end
