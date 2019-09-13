#include <stdio.h>
#include "benchmarks.h"
#include "log.h"

bench_res_t bench_execute(int argc, char **argv)
{
	log_info("This is an INFO message\n");
	log_critical("This is a CRITICAL message\n");
	return BENCH_SUCCESS;
}

char *bench_name()
{
	return "dummy";
}
