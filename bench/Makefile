EXP_DATA=$(wildcard *.data)
EXP_SUMMARY=$(wildcard *.summary)
fig/partition-bench.pdf fig/update-gc-bench.pdf: bench.gp $(EXP_SUMMARY)
	gnuplot $<
