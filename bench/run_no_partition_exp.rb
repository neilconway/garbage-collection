size = 1000
percents = (0..90).select {|i| i % 10 == 0}
#percents = [0, 50, 90]
data_files = {}
percents.each {|v| data_files[v] = "no_partition_cont_#{v}.data"}
log_file = "exp_log"

`rm -f #{log_file}`
data_files.each_pair do |k, fname|
  `echo "#percent: #{k}\n#time num_tuples" | cat > #{fname}`
end

percents.each do |p|
  puts "Running benchmark; percent = #{p}"
  `ruby benchmark.rb #{size} #{p} no_partition_new >> #{log_file} 2>>#{data_files[p]}`
  raise "Error: #{$?}" unless $?.success?
  print "."
  print "\n"
end

