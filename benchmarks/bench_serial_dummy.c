#include <stdio.h>
#include <stdlib.h>

#include "../maps/map.h"
#include "../lib/log.h"
#include "benchmarks.h"

bench_res_t bench_execute(int argc, char **argv)
{
	int nr_operations, i;
	void *map, *tdata;

	log_info("Benchmark: Serial\n");

	if (argc != 2) {
		log_error(stderr, "Usage: %s <number of insertions>\n", argv[0]);
		exit(1);
	}

	map = map_new();
	tdata = map_tdata_new(0);
	printf("Map: %s\n", map_name());

	nr_operations = atoi(argv[1]);
	for (i=0; i < nr_operations; i++)
		map_insert(map, tdata, i, NULL);

	map_validate(map);

	return BENCH_SUCCESS;
}

char *bench_name()
{
	return "serial-dummy";
}
