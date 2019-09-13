#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#ifdef WORKLOAD_TIME
#	include <unistd.h> //> sleep()
#endif

#include "benchmarks.h"
#include "aff.h"
#include "clargs.h"
#include "thread_data.h"
#include "warmup.h"
#include "../maps/map.h"
#include "../lib/alloc.h"
#include "../lib/timers.h"
#include "../lib/log.h"

pthread_barrier_t start_barrier;

__thread int seed;
int nextNatural(int n) {                                                                                                                                                                  
	seed ^= seed << 6;
	seed ^= seed >> 21;
	seed ^= seed << 7;
	int retval = (int) (seed % n);
	return (retval < 0 ? -retval : retval);
}

void *thread_fn(void *arg)
{
	thread_data_t *data = arg;
	int ret, tid = data->tid, cpu = data->cpu;
	void *map = data->map;
	int choice, key;
#	if defined(WORKLOAD_FIXED)
	int ops_performed = 0;
#	endif
	
	//> Set affinity.
	setaffinity_oncpu(cpu);

	//> Initialize per thread map data.
	data->map_tdata = map_tdata_new(tid);

	//> Initialize random number generator
	seed = (tid + 1) * clargs.thread_seed;

	//> Wait for the master to give the starting signal.
	pthread_barrier_wait(&start_barrier);

	//> Critical section.
	while (1) {
#		if defined(WORKLOAD_FIXED)
		if (ops_performed >= data->nr_operations - 1)
			break;
		ops_performed = data->operations_performed[OPS_TOTAL];
#		elif defined(WORKLOAD_TIME)
		if (*(data->time_to_leave))
			break;
#		endif

		//> Generate random number;
		choice = nextNatural(100);
		key = nextNatural(clargs.max_key);

		data->operations_performed[OPS_TOTAL]++;

		//> Perform operation on the RBT based on choice.
		if (choice < clargs.lookup_frac) {
			//> Lookup
			data->operations_performed[OPS_LOOKUP]++;
			ret = map_lookup(map, data->map_tdata, key);
			data->operations_succeeded[OPS_LOOKUP] += ret;
		} else if (choice < clargs.lookup_frac + clargs.rquery_frac) {
			//> Range-Query
			data->operations_performed[OPS_RQUERY]++;
			ret = map_rquery(map, data->map_tdata, key, key+100);
			data->operations_succeeded[OPS_RQUERY] += ret;
		} else {
			//> Update
			ret = map_update(map, data->map_tdata, key, NULL);
			if (ret == 0 || ret == 1) {
				data->operations_performed[OPS_INSERT]++;
				data->operations_succeeded[OPS_INSERT] += ret;
			} else if (ret == 2 || ret == 3) {
				ret -= 2;
				data->operations_performed[OPS_DELETE]++;
				data->operations_succeeded[OPS_DELETE] += ret;
			} else {
				log_error("Wrong return value from map_update() ret=%d\n", ret);
				exit(1);
			}
		}
//		} else if (choice  < clargs.lookup_frac + clargs.insert_frac) {
//			//> Insertion
//			data->operations_performed[OPS_INSERT]++;
//			ret = rbt_insert(rbt, data->rbt_thread_data, key, NULL);
//			data->operations_succeeded[OPS_INSERT] += ret;
//		} else {
//			//> Deletion
//			data->operations_performed[OPS_DELETE]++;
//			ret = rbt_delete(rbt, data->rbt_thread_data, key);
//			data->operations_succeeded[OPS_DELETE] += ret;
//		}
		data->operations_succeeded[OPS_TOTAL] += ret;
	}

	return NULL;
}

bench_res_t bench_execute(int argc, char **argv)
{
	int i, validation, nthreads;
	pthread_t *threads;
	thread_data_t **threads_data;
	unsigned int ncpus;
	unsigned int *cpus;
	int time_to_leave = 0;
	timer_tt *warmup_timer;
	void *map;

	//> Read command line arguments
	clargs_init(argc, argv);
	clargs_print();
	nthreads = clargs.num_threads;

	//> Initialize memory allocator.
	int warmup_core = 0;
	setaffinity_oncpu(warmup_core);
	log_info("\n");

	//> Initialize Red-Black tree.
	map = map_new();
	log_info("Benchmark\n");
	log_info("=======================\n");
	log_info("  MAP implementation: %s\n", map_name());

	//> Map warmup.
	warmup_timer = timer_init();
	log_info("\n");
	log_info("Tree initialization (at core %d)...\n", warmup_core);
	timer_start(warmup_timer);
	map_warmup(map, clargs.init_tree_size, clargs.max_key, clargs.init_seed);
	timer_stop(warmup_timer);
	log_info("Initialization finished in %.2lf sec\n", timer_report_sec(warmup_timer));

	//> Initialize the starting barrier.
	pthread_barrier_init(&start_barrier, NULL, nthreads+1);
	
	//> Initialize the arrays that hold the thread references and data.
	XMALLOC(threads, nthreads);
	XMALLOC(threads_data, nthreads);

	//> Get the mapping of threads to cpus
	log_info("\n");
	log_info("Reading MT_CONF, to get the thread->cpu mapping.\n");
	get_mtconf_options(&ncpus, &cpus);
	mt_conf_print(ncpus, cpus);

	//> Initialize per thread data and spawn threads.
	for (i=0; i < nthreads; i++) {
		int cpu = cpus[i];
		threads_data[i] = thread_data_new(i, cpu, map);
#		ifdef WORKLOAD_FIXED
		threads_data[i]->nr_operations = clargs.nr_operations / nthreads;
#		elif defined(WORKLOAD_TIME)
		threads_data[i]->time_to_leave = &time_to_leave;
#		endif
		pthread_create(&threads[i], NULL, thread_fn, threads_data[i]);
	}

	//> Wait until all threads go to the starting point.
	pthread_barrier_wait(&start_barrier);

	//> Init and start wall_timer.
	timer_tt *wall_timer = timer_init();
	timer_start(wall_timer);

#	if defined(WORKLOAD_TIME)
	sleep(clargs.run_time_sec);
	time_to_leave = 1;
#	endif

	//> Join threads.
	for (i=0; i < nthreads; i++)
		pthread_join(threads[i], NULL);

	//> Stop wall_timer.
	timer_stop(wall_timer);

	//> Print thread statistics.
	thread_data_t *total_data = thread_data_new(-1, -1, NULL);
	log_info("\nThread statistics\n");
	log_info("=======================\n");
	for (i=0; i < nthreads; i++) {
		thread_data_print(threads_data[i]);
		thread_data_add(threads_data[i], total_data, total_data);
	}
	log_info("-----------------------\n");
	thread_data_print(total_data);

	//> Print additional per thread statistics.
	total_data->map_tdata = map_tdata_new(-1);
	log_info("\n");
	log_info("\nAdditional per thread statistics\n");
	log_info("=======================\n");
	for (i=0; i < nthreads; i++) {
		thread_data_print_map_data(threads_data[i]);
		thread_data_add_map_data(threads_data[i], total_data, total_data);
	}
	log_info("-----------------------\n");
	thread_data_print_map_data(total_data);
	log_info("\n");

	//> Validate the final RBT.
	validation = map_validate(map);

	//> Print elapsed time.
	double time_elapsed = timer_report_sec(wall_timer);
	double throughput_usec = total_data->operations_performed[OPS_TOTAL] / 
	                         time_elapsed / 1000000.0;
	log_info("Time elapsed: %6.2lf\n", time_elapsed);
	log_info("Throughput(Ops/usec): %7.3lf\n", throughput_usec);

	log_info("Expected size of RBT: %llu\n",
	        (long long unsigned)clargs.init_tree_size +
	        total_data->operations_succeeded[OPS_INSERT] - 
	        total_data->operations_succeeded[OPS_DELETE]);

//	return validation;
	return BENCH_SUCCESS;
}

char *bench_name()
{
	return "pthreads-random-operations";
}
