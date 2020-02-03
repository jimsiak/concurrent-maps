CC = gcc
CFLAGS = -Wall -Wextra -g

OPT_LEVEL ?= -O3
CFLAGS += $(OPT_LEVEL)

## Ignore unused variable/parameter warnings.
CFLAGS += -Wno-unused-variable -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-unused-function -Wno-array-bounds

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

## What type of keys for the map data structures?
## Possible values: MAP_KEY_TYPE_INT, MAP_KEY_TYPE_BIG_INT, MAP_KEY_TYPE_STR
MAP_KEY_TYPE ?= MAP_KEY_TYPE_INT
BIG_INT_KEY_SZ ?= 50
STR_KEY_SZ ?= 50
CFLAGS += -D$(MAP_KEY_TYPE) -DSTR_KEY_SZ=$(STR_KEY_SZ) -DBIG_INT_KEY_SZ=$(BIG_INT_KEY_SZ)

INC_FLAGS = -Ilib/
CFLAGS += $(INC_FLAGS)

CFLAGS += -pthread

BENCHMARK_FILE=benchmarks/bench_pthreads_random_ops.c
NALLOC_FILE=maps/nalloc/nalloc_prealloc.c
SOURCE_FILES = main.c $(BENCHMARK_FILE) $(NALLOC_FILE)

all: x.btree.seq

## Binary Search Trees (BSTs)
x.bst.int.seq: $(SOURCE_FILES) maps/trees/bsts/seq-internal.c
	$(CC) $(CFLAGS) $^ -o $@
x.bst.int.cg_spin: $(SOURCE_FILES) maps/trees/bsts/seq-internal.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.bst.int.cg_htm: $(SOURCE_FILES) maps/trees/bsts/seq-internal.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM
x.bst.ext.seq: $(SOURCE_FILES) maps/trees/bsts/seq-external.c
	$(CC) $(CFLAGS) $^ -o $@
x.bst.ext.cg_spin: $(SOURCE_FILES) maps/trees/bsts/seq-external.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.bst.ext.cg_htm: $(SOURCE_FILES) maps/trees/bsts/seq-external.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM
x.bst.int.rcu_htm: $(SOURCE_FILES) maps/trees/bsts/rcu-htm-internal.c
	$(CC) $(CFLAGS) $^ -o $@

### AVL BSTs
x.bst.avl.bronson: $(SOURCE_FILES) maps/trees/bsts/avl/bronson.c
	$(CC) $(CFLAGS) $^ -o $@
x.bst.avl.ext.rcu_htm: $(SOURCE_FILES) ./maps/trees/bsts/avl/rcu-htm-external.c
	$(CC) $(CFLAGS) $^ -o $@

## B+trees
x.btree.seq: $(SOURCE_FILES) maps/trees/btrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@
x.btree.cg_spin: $(SOURCE_FILES) maps/trees/btrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.btree.cg_htm: $(SOURCE_FILES) maps/trees/btrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM
x.btree.rcu_htm: $(SOURCE_FILES) maps/trees/btrees/rcu-htm.c
	$(CC) $(CFLAGS) $^ -o $@
x.btree.blink_locks: $(SOURCE_FILES) maps/trees/btrees/blink-lock.c
	$(CC) $(CFLAGS) $^ -o $@

### (a-b)-trees
x.abtree.seq: $(SOURCE_FILES) maps/trees/btrees/abtrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@
x.abtree.cg_htm: $(SOURCE_FILES) maps/trees/btrees/abtrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM
x.abtree.cg_spin: $(SOURCE_FILES) maps/trees/btrees/abtrees/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.abtree.rcu_htm: $(SOURCE_FILES) maps/trees/btrees/abtrees/rcu-htm.c
	$(CC) $(CFLAGS) $^ -o $@

## Treaps
x.treap.seq: $(SOURCE_FILES) maps/trees/treaps/seq.c
	$(CC) $(CFLAGS) $^ -o $@
x.treap.cg_spin: $(SOURCE_FILES) maps/trees/treaps/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.treap.cg_htm: $(SOURCE_FILES) maps/trees/treaps/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM

## Skiplists
x.skiplist.seq: $(SOURCE_FILES) maps/skiplist/seq.c
	$(CC) $(CFLAGS) $^ -o $@
x.skiplist.cg_spin: $(SOURCE_FILES) maps/skiplist/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_SPINLOCK
x.skiplist.cg_htm: $(SOURCE_FILES) maps/skiplist/seq.c
	$(CC) $(CFLAGS) $^ -o $@ -DSYNC_CG_HTM

x.skiplist.herlihy: $(SOURCE_FILES) maps/skiplist/herlihy.c
	$(CC) $(CFLAGS) $^ -o $@
x.skiplist.pugh: $(SOURCE_FILES) maps/skiplist/pugh.c
	$(CC) $(CFLAGS) $^ -o $@

## Contention-adaptive generic scheme
x.treap.ca_locks: $(SOURCE_FILES) maps/contention-adaptive/ca-locks.c
	$(CC) $(CFLAGS) $^ -o $@ -DSEQ_DS_TYPE_TREAP

clean:
	rm -f x.*
