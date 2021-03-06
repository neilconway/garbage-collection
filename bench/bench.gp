set terminal pdfcairo font "Helvetica,18" linewidth 4 rounded

set ylabel "Number of Tuples"
set xlabel "Time (s)"

unset key
set output "fig/partition-bench.pdf"
plot [*:200] "no_saw_tooth/partition_data.summary" using 1:2 notitle w lines

set output "fig/update-gc-bench.pdf"
set term pdfcairo dashed
set key title "Update Percentage" at 13.4,1275
#set yrange [0:1200]
set xrange [0:30]
plot "no_partition_data/no_partition_10.data" using 1:2 title "10%" w lines, \
	"no_partition_data/no_partition_30.data" using 1:2 title "30%" w lines lt 2, \
	"no_partition_data/no_partition_50.data" using 1:2 title "50%" w lines lt 3, \
	"no_partition_data/no_partition_70.data" using 1:2 title "70%" w lines lt 4, \
	"no_partition_data/no_partition_90.data" using 1:2 title "90%" w lines lt 5, \
    # "no_partition_data/no_partition_0.data" using 1:2 title "0%" w lines, \
	# "no_partition_data/no_partition_20.data" using 1:2 title "20%" w lines ls 3, \
	# "no_partition_data/no_partition_40.data" using 1:2 title "40%" w lines ls 1, \
	# "no_partition_data/no_partition_60.data" using 1:2 title "60%" w lines ls 3, \
	# "no_partition_data/no_partition_80.data" using 1:2 title "80%" w lines ls 1, \


