#include <stdio.h>
#include "benchmarks/benchmarks.h"
#include "lib/log.h"

int main(int argc, char **argv)
{
	log_info("Benchmark: %s\n", bench_name());
	bench_res_t ret = bench_execute(argc, argv);
	return ret;
}
