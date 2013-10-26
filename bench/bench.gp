set terminal pdfcairo font "Helvetica,18" linewidth 4 rounded
set ylabel "Number of Tuples"
set xlabel "Update percentage"
set key top left

set style line 1 lt rgb "#A00000" lw 2 pt 1

plot "no_partition.summary" using 1:2 title "Storage Used vs Update Percentage " w lp ls 1