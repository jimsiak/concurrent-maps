#ifndef _BTREE_H_
#define _BTREE_H_

#include <stdio.h>
#include "../../key/key.h"
#include "../../map.h"
#include "alloc.h"
#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#define BTREE_ORDER 8

typedef struct btree_node_s {
	int leaf;
	int no_keys;
	struct btree_node_s *sibling;
	map_key_t keys[2*BTREE_ORDER];
	__attribute__((aligned(16))) void *children[2*BTREE_ORDER + 1];
} __attribute__((packed)) btree_node_t;

typedef struct {
	btree_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t btree_lock;
#	endif
} btree_t;

static __thread void *nalloc;

/**
 * Creates a new btree_node_t
 **/
static btree_node_t *btree_node_new(char leaf)
{
	btree_node_t *ret = nalloc_alloc_node(nalloc);
	ret->no_keys = 0;
	ret->leaf = leaf;
	return ret;
}

static int btree_node_search(btree_node_t *n, map_key_t key)
{
	int i = 0;
	while (i < n->no_keys && KEY_CMP(key, n->keys[i]) > 0) i++;
	return i;
}

static void btree_node_delete_index(btree_node_t *n, int index)
{
	int i;
	assert(index < n->no_keys);
	for (i=index+1; i < n->no_keys; i++) {
		KEY_COPY(n->keys[i-1], n->keys[i]);
		n->children[i] = n->children[i+1];
	}
	n->no_keys--;
}

/**
 * Insert 'key' in position 'index' of node 'n'.
 * 'ptr' is either a pointer to data record in the case of a leaf 
 * or a pointer to a child btree_node_t in the case of an internal node.
 **/
static void btree_node_insert_index(btree_node_t *n, int index, map_key_t key,
                                    void *ptr)
{
	int i;

	for (i=n->no_keys; i > index; i--) {
		KEY_COPY(n->keys[i], n->keys[i-1]);
		n->children[i+1] = n->children[i];
	}
	KEY_COPY(n->keys[index], key);
	n->children[index+1] = ptr;

	n->no_keys++;
}

static void btree_node_print(btree_node_t *n)
{
	int i;

	printf("btree_node: [");
	if (!n) {
		printf("]\n");
		return;
	}

	for (i=0; i < n->no_keys; i++)
		KEY_PRINT(n->keys[i], " ", " |");
	printf("]");
	printf("%s\n", n->leaf ? " LEAF" : "");
}

static btree_t *btree_new()
{
	btree_t *ret;
	XMALLOC(ret, 1);
	ret->root = NULL;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spin_init(&ret->btree_lock, PTHREAD_PROCESS_SHARED);
#	endif

	return ret;
}

#endif /* _BTREE_H_ */
