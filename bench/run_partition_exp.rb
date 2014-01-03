#!/usr/bin/env ruby

# Run partition experiment
log_file = "exp_partition_log"
`rm -f #{log_file}`
`echo "#Variant: Partitioned Storage\n#Time storage" | cat > partition.data`
puts "Running Partition Experiment"
`ruby benchmark.rb 1000 100 partition >> #{log_file} 2>>partition.data`

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
