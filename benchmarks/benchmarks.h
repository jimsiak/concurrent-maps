#ifndef _BENCHMARKS_H_
#define _BENCHMARKS_H_

typedef enum {
	BENCH_SUCCESS = 0,
	BENCH_FAILURE = 1
} bench_res_t;

//> Executes the benchmark and returns 
bench_res_t bench_execute(int argc, char **argv);
char *bench_name();

#endif /* _BENCHMARKS_H_ */
