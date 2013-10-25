#!/usr/bin/env ruby
nruns = 5
#sizes = (1000..100000).select {|i| i % 1000 == 0}
size = 10000
percents = (0..90).select {|i| i % 10 == 0}
variants = ["no_partition"]
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

module Enumerable
  def sum
    self.inject(0){|accum, i| accum + i }
  end

  def mean
    self.sum/self.length.to_f
  end
end

# Compute summary data for each run. It would be more convenient to compute this
# in the gnuplot script, but it seems hard to do a grouped aggregate in gnuplot.
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
