#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "alloc.h"

#define NR_NODES 10000000

typedef struct {
	void *free_nodes[NR_NODES];
	int index;
	int tid;
} tdata_t;

void *nalloc_init()
{
	return NULL;
}

void *nalloc_thread_init(int tid, size_t sz)
{
	int i;
	tdata_t *ret;
	XMALLOC(ret, 1);
	for (i=0; i < NR_NODES; i++) {
		ret->free_nodes[i] = malloc(sz);
		memset(ret->free_nodes[i], 0, sz);
	}
	ret->index = 0;
	ret->tid = tid;
	return ret;
}

void *nalloc_alloc_node(void *_nalloc)
{
	tdata_t *nalloc = _nalloc;
	assert(nalloc->index < NR_NODES);
	return nalloc->free_nodes[nalloc->index++];
}

void nalloc_free_node(void *nalloc, void *node)
{
}
