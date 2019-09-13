CC = gcc
CFLAGS = -Wall -Wextra -g

OPT_LEVEL ?= -O3
CFLAGS += $(OPT_LEVEL)

## Ignore unused variable/parameter warnings.
#CFLAGS += -Wno-unused-variable -Wno-unused-parameter -Wno-unused-but-set-variable
#CFLAGS += -Wno-array-bounds

## Number of transactional retries before resorting to non-tx fallback.
CFLAGS += -DTX_NUM_RETRIES=10

## Used to print verbose statistics in some implementations.
#CFLAGS += -DVERBOSE_STATISTICS
## For the flexible window on top-down implementations
#CFLAGS += -DACCESS_PATH_MAX_DEPTH=3

## Which workload do we want?
WORKLOAD_FLAG = -DWORKLOAD_TIME
#WORKLOAD_FLAG = -DWORKLOAD_FIXED
CFLAGS += $(WORKLOAD_FLAG)

INC_FLAGS = -Ilib/
CFLAGS += $(INC_FLAGS)

CFLAGS += -pthread

BENCHMARK_FILE=benchmarks/bench_pthreads_random_ops.c
NALLOC_FILE=maps/nalloc/nalloc_prealloc.c
SOURCE_FILES = main.c $(BENCHMARK_FILE) $(NALLOC_FILE)

all: x.btree.seq

## B+trees
x.btree.seq: $(SOURCE_FILES) maps/trees/btrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@
x.btree.cg_spin: $(SOURCE_FILES) maps/trees/btrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.btree.cg_htm: $(SOURCE_FILES) maps/trees/btrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM
x.btree.rcu_htm: $(SOURCE_FILES) maps/trees/btrees/rcu-htm.c
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm -f x.*
