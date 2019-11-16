#ifndef _BTREE_H_
#define _BTREE_H_

#include <stdio.h>
#include <string.h>
#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
#include <pthread.h> //> pthread_spinlock_t
#endif
#include "../../key/key.h"
#include "../../map.h"
#include "alloc.h"

#ifndef BTREE_ORDER
#define BTREE_ORDER 8
#endif

typedef struct btree_node_s {
	int leaf;
	int no_keys;
	struct btree_node_s *sibling;
	map_key_t keys[2*BTREE_ORDER];
	__attribute__((aligned(16))) void *children[2*BTREE_ORDER + 1];
#	ifdef RWLOCK_PER_NODE
	pthread_rwlock_t lock;
#	endif
#	ifdef HIGHKEY_PER_NODE
	map_key_t highkey;
#	endif
} __attribute__((packed)) btree_node_t;

typedef struct {
	btree_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spinlock_t lock;
#	endif
} btree_t;

static __thread void *nalloc;

/**
 * Creates a new btree_node_t
 **/
static btree_node_t *btree_node_new(char leaf)
{
	btree_node_t *ret = nalloc_alloc_node(nalloc);
	memset(ret, 0, sizeof(*ret));
	ret->leaf = leaf;
#	ifdef RWLOCK_PER_NODE
	pthread_rwlock_init(&ret->lock, NULL);
#	endif
#	ifdef HIGHKEY_PER_NODE
	ret->highkey = MAX_KEY;
#	endif
	return ret;
}

static btree_node_t *btree_node_new_copy(btree_node_t *n)
{
	btree_node_t *ret = btree_node_new(0);
	memcpy(ret, n, sizeof(*n));
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

/**
 * Splits a leaf node into two leaf nodes which also contain the newly
 * inserted key 'key'.
 **/ 
btree_node_t *btree_leaf_split(btree_node_t *n, int index, map_key_t key, void *ptr)
{
       int i;
       btree_node_t *rnode = btree_node_new(1);

       //> Move half of the keys on the new node.
       for (i=BTREE_ORDER; i < 2 *BTREE_ORDER; i++) {
               KEY_COPY(rnode->keys[i - BTREE_ORDER], n->keys[i]);
               rnode->children[i - BTREE_ORDER] = n->children[i];
       }
       rnode->children[i - BTREE_ORDER] = n->children[i];

       //> Update number of keys for the two split nodes.
       n->no_keys = BTREE_ORDER;
       rnode->no_keys = BTREE_ORDER;

       //> Insert the new key in the appropriate node.
       if (index < BTREE_ORDER) btree_node_insert_index(n, index, key, ptr);
       else   btree_node_insert_index(rnode, index - BTREE_ORDER, key, ptr);

       //> Fix the sibling pointer of `n`
       rnode->sibling = n->sibling;
       n->sibling = rnode;

       return rnode;
}

/**
 * Splits an internal node into two internal nodes.
 **/ 
static btree_node_t *btree_internal_split(btree_node_t *n, int index, map_key_t key,
                                          void *ptr, map_key_t *key_left_outside)
{
	int i, mid_index;
	btree_node_t *rnode = btree_node_new(0);
	map_key_t left_outside;

	mid_index = BTREE_ORDER;
	if (index < BTREE_ORDER) mid_index--;

	KEY_COPY(left_outside, n->keys[mid_index]);

	//> Move half of the keys on the new node.
	for (i=mid_index+1; i < 2 *BTREE_ORDER; i++) {
		KEY_COPY(rnode->keys[i - (mid_index+1)], n->keys[i]);
		rnode->children[i - (mid_index+1)] = n->children[i];
	}
	rnode->children[i - (mid_index+1)] = n->children[i];

	//> Update number of keys for the two split nodes.
	n->no_keys = mid_index;
	rnode->no_keys = 2 * BTREE_ORDER - mid_index - 1;

	//> Insert the new key in the appropriate node.
	if (n->no_keys < BTREE_ORDER) {
		btree_node_insert_index(n, index, key, ptr);
	} else if (index == mid_index) {
		btree_node_insert_index(rnode, 0, n->keys[mid_index],
		                                  n->children[mid_index+1]);
		rnode->children[0] = ptr;
		KEY_COPY(left_outside, key);
	} else {
		btree_node_insert_index(rnode, index - (mid_index+1), key, ptr);
	}

	rnode->sibling = n->sibling;
	n->sibling = rnode;

	KEY_COPY(*key_left_outside, left_outside);
	return rnode;
}

static btree_t *btree_new()
{
	btree_t *ret;
	XMALLOC(ret, 1);
	ret->root = NULL;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spin_init(&ret->lock, PTHREAD_PROCESS_SHARED);
#	endif

	return ret;
}

#endif /* _BTREE_H_ */
