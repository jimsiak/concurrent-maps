#!/bin/bash

export LD_PRELOAD=/home/users/jimsiak/Downloads/jemalloc/lib/libjemalloc.so
export LD_LIBRARY_PATH=/home/users/jimsiak/concurentDataBenchmarks/predicatercu/

#TIMES=10
TIMES=10
RUNTIME=15
#RUNTIME=2
NR_OPERATIONS=10000000
#WORKLOADS="100_0_0 80_10_10 0_50_50"
WORKLOADS="100_0_0 90_5_5 80_10_10 50_25_25 20_40_40 0_50_50"
#INIT_SIZES="10000_10K 1000000_1M 10000000_10M"
INIT_SIZES="100_100 1000_1K 10000_10K 1000000_1M 10000000_10M"
#INIT_SIZES="10000000_10M"

arch=$(uname -m)
if [ "$arch" = "ppc64le" ]; then
	machine=power
	threads_str="1 2 3 4 5 10 20 40 60 80 100 120 140 160"
else
	machine=intel
	threads_str="1 2 3 4 8"

	host=$(hostname)
	if [ "x$host" = "xsandman" ]; then
		machine=sandman
		threads_str="1 2 4 8 16 32 64"
	elif [ "x$host" = "xhaci3" ]; then
		machine=haci3
		threads_str="1 2 4 7 14"
	elif [ "x$host" = "xbroady" ]; then
		machine=broady
		threads_str="1 2 4 8 16 22 44"
	elif [ "x$host" = "xbroady2" -o "x$host" = "xbroady3" ]; then
		machine=$host
		threads_str="1 2 4 8 10"
	fi
fi

#DIRNAME=results/`date +"%Y_%m_%d-%H_%M"`-blabla/$machine
#DIRNAME=results/testing-update-operation-new
#DIRNAME=results/TACO_review/btrees/
#DIRNAME=results/TACO_review/bsts/
DIRNAME=results/TACO_review/debra_reclamation/
mkdir -p $DIRNAME

for t in `seq 0 $((TIMES-1))`; do

SEED1=$RANDOM
SEED2=$RANDOM

for INIT_SIZE in $INIT_SIZES; do
	init_size=$(echo $INIT_SIZE | cut -d'_' -f'1')
	init_prefix=$(echo $INIT_SIZE | cut -d'_' -f'2')

for WORKLOAD in $WORKLOADS; do
	lookup_pct=$(echo $WORKLOAD | cut -d'_' -f'1')
	insert_pct=$(echo $WORKLOAD | cut -d'_' -f'2')
	delete_pct=$(echo $WORKLOAD | cut -d'_' -f'3')

for EXECUTABLE in x.main.*; do

for i in $threads_str; do

	echo "$EXECUTABLE $INIT_SIZE $WORKLOAD ($i threads)"
	FILENAME=${EXECUTABLE}.${init_prefix}_init.${lookup_pct}_${insert_pct}_${delete_pct}.$t.output
	./$EXECUTABLE -s$init_size -m$((2*init_size)) \
	              -l$lookup_pct -i$insert_pct -t$i \
				  -r$RUNTIME \
				  -e$SEED1 -j$SEED2 \
	              &>> $DIRNAME/$FILENAME

#				  -o$((i*1000000)) \
#	              -o${NR_OPERATIONS} \

done # i
done # EXECUTABLE
done # WORKLOAD
done # INIT_SIZE
done # t
