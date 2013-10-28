set terminal pdfcairo font "Helvetica,18" linewidth 4 rounded
set ylabel "Number of Tuples"
set xlabel "Update percentage"
set key top left

set style line 1 lt rgb "#A00000" lw 2 pt 1
set style line 2 lt rgb "#00A000" lw 2 pt 6
set style line 3 lt rgb "#5060D0" lw 2 pt 2
set style line 4 lt rgb "#F25900" lw 2 pt 9

plot "no_partition.summary" using 1:2 title "Storage Used vs Update Percentage " w lp ls 1

set xlabel "Time, seconds"
set key bottom right

plot "partition_data.summary" using 1:2 title "Partitioned Storage" w lp ls 3
