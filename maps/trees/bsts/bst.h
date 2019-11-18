#ifndef _BST_H_
#define _BST_H_

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
#include <pthread.h> //> pthread_spinlock_t
#endif
#include "../../key/key.h"
#include "../../map.h"
#include "alloc.h"

typedef struct bst_node_s {
	map_key_t key;
	void *data;

	struct bst_node_s *right,
	                  *left;
} bst_node_t;

typedef struct {
	bst_node_t *root;
#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spinlock_t lock;
#	endif
} bst_t;

static __thread void *nalloc;

static bst_node_t *bst_node_new(map_key_t key, void *data)
{
	bst_node_t *node = nalloc_alloc_node(nalloc);
	KEY_COPY(node->key, key);
	node->data = data;
	node->right = NULL;
	node->left = NULL;
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
