#ifndef _WARMUP_H_
#define _WARMUP_H_

#include <stdlib.h>
#include "../maps/map.h"

static inline int map_warmup(void *map, int nr_nodes, int max_key,
                             unsigned int seed)
{
	void *tdata = map_tdata_new(-1);
	int nodes_inserted = 0, ret = 0, randint;
	map_key_t key;
	
	srand(seed);
	while (nodes_inserted < nr_nodes) {
		randint = rand() % max_key;
		KEY_GET(key, randint);

		ret = map_insert(map, tdata, key, NULL);
		nodes_inserted += ret;
	}

	return nodes_inserted;
}


#endif /* _WARMUP_H_ */
