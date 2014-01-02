set terminal jpeg font "Helvetica,18" linewidth 4 rounded
set ylabel "Number of Tuples"
set xlabel "Time (s)"
set key top left

set style line 1 lt rgb "#A00000" lw 2 pt 1
set style line 2 lt rgb "#00A000" lw 2 pt 6
set style line 3 lt rgb "#5060D0" lw 2 pt 2
set style line 4 lt rgb "#F25900" lw 2 pt 9

set output "gc-bench.jpeg"
plot "no_partition_10.data" using 1:2 title "10%" w lp ls 1, \
	"no_partition_30.data" using 1:2 title "30%" w lp ls 2, \
	"no_partition_50.data" using 1:2 title "50%" w lp ls 3, \
	"no_partition_70.data" using 1:2 title "70%" w lp ls 4, \
	"no_partition_90.data" using 1:2 title "90%" w lp ls 1, \




