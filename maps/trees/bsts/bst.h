#ifndef _BST_H_
#define _BST_H_

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
#include <pthread.h> //> pthread_spinlock_t
#endif
#include <string.h>  //> memcpy()
#include "../../key/key.h"
#include "../../map.h"
#include "alloc.h"

typedef struct bst_node_s {
	map_key_t key;
	void *data;

	struct bst_node_s *right,
	                  *left;

#	ifdef NODE_HAS_PARENT
	struct bst_node_s *parent;
#	endif

#	ifdef NODE_HAS_LOCK
	pthread_spinlock_t lock;
#	endif

#	ifdef NODE_HAS_VERSION
	long long version;
#	endif

#	ifdef NODE_HAS_HEIGHT
	int height;
#	endif

#	ifdef NODE_HAS_UPDATE
	info_t *update;
#	endif

#	ifdef NODE_HAS_ISLEAF
	char isleaf;
#	endif
} bst_node_t;

typedef struct {
	bst_node_t *root;
#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spinlock_t lock;
#	endif
} bst_t;

static __thread void *nalloc;

#ifdef NODE_HAS_ISLEAF
static bst_node_t *bst_node_new(map_key_t key, void *data, char isleaf)
#else
static bst_node_t *bst_node_new(map_key_t key, void *data)
#endif
{
	bst_node_t *node = nalloc_alloc_node(nalloc);
	memset(node, 0, sizeof(*node));
	KEY_COPY(node->key, key);
	node->data = data;
#ifdef NODE_HAS_ISLEAF
	node->isleaf = isleaf;
#endif
	return node;
}

static bst_node_t *bst_node_new_copy(bst_node_t *src)
{
	bst_node_t *node = nalloc_alloc_node(nalloc);
	memcpy(node, src, sizeof(*node));
	return node;
}

static bst_t *_bst_new_helper()
{
	bst_t *bst;
	XMALLOC(bst, 1);
	bst->root = NULL;
#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spin_init(&bst->lock, PTHREAD_PROCESS_SHARED);
#	endif
	return bst;
}

#endif /* _BST_H_ */
