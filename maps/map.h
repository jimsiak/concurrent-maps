#ifndef _MAP_H_
#define _MAP_H_

#include "key/key.h"

//> Not thread-safe interface functions.
//> Should only be called during initialization and termination phase
//> by a single thread.
void *map_new();
char *map_name();
int   map_validate(void *map);

//> Initialize per thread data and statistics.
void *map_tdata_new(int tid);
void  map_tdata_print(void *tdata);
void  map_tdata_add(void *tdata1, void *tdata2, void *tdata_dst);

//> Thread-safe interface functions.
//> Can handle multiple threads at the same time and produce correct results.
//> The map_update() function performs an insert or a delete depending
//>  on whether the key is present or not in the map.
//>  It returns 0 or 1 if insert was performed and 2 or 3 if delete was performed.
int map_lookup(void *map, void *tdata, map_key_t key);
int map_insert(void *map, void *tdata, map_key_t key, void *value);
int map_delete(void *map, void *tdata, map_key_t key);
int map_update(void *map, void *tdata, map_key_t key, void *value);
int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2);

//> Debugging functions
void map_print(void *map);

//> Thread-safe node allocator functions
void *nalloc_init();
void *nalloc_thread_init(int tid, size_t sz);
void *nalloc_alloc_node(void *nalloc);
void  nalloc_free_node(void *nalloc, void *node);

#endif /* _MAP_H_ */
