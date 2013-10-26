EXP_DATA=$(wildcard *.data)
EXP_SUMMARY=$(wildcard *.summary)
fig/bench.pdf: bench.gp $(EXP_SUMMARY)
	gnuplot -e "set output \"$@\"" $<