/**
 * Author: Dimitris Siakavaras (jimsiak<at>cslab.ece.ntua.gr)
 * This file contains an implementation of the B+tree version presented in:
 * P. Lehman and S. Yao, Efficient Locking for Concurrent Operations on B-Trees,
 * ACM Transactions on Database Systems, Vol 6, No. 4, December 1981, pp 650-670
 *
 * Main characteristics of the B+tree version:
 * - Every node has a sibling pointer and a highkey field.
 * - Reader locks are used for lookups and traversals of the tree (different
 *   from the original paper version).
 * - Insertions write-lock the target leaf during their traversal and 
 *   propagate splits upwards also using writer locks.
 * - Deletions write-lock the target leaf, but do not perform rebalance.
 *   Leaf nodes can be less than half-full.
 *
 * Differences to the paper version:
 * - We use read locks for the tree traversals. This is necessary since for
 *   in-memory B+trees Lehman and Yao's assumption that `get` and `put`
 *   operations on pages (i.e., nodes of the tree) are indivisible.
 *   When the pages are in main memory they may be modified by one thread
 *   while being read by another.
 *
 * Known BUGs:
 * - When multiple threads update the btree->root pointer updates can be lost.
 *   This happens because we have no way to determine if btree->root was
 *   split by a concurrent thread.
 **/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <limits.h> //> For INT_MAX
#include <pthread.h> //> pthread_spinlock_t

#include "../../key/key.h"
#define RWLOCK_PER_NODE
#define HIGHKEY_PER_NODE
#define SYNC_CG_SPINLOCK
#include "btree.h"
#define NOT_FULL_NODES_ALLOWED
#include "validate.h"
#include "print.h"

#define TRYRDLOCK_NODE(n) pthread_rwlock_tryrdlock(&((n)->lock))
#define TRYWRLOCK_NODE(n) pthread_rwlock_trywrlock(&((n)->lock))
#define LOCK_NODE(n)      pthread_rwlock_wrlock(&((n)->lock))
#define UNLOCK_NODE(n)    pthread_rwlock_unlock(&((n)->lock))

/**
 * Scans node `n` to find the appropriate pointer of the next node for `key`
 * If the key is inside the range of n the appropriate child pointer
 * is returned, otherwise n's right node is returned.
 **/
static void *btree_node_scan(btree_node_t *n, map_key_t key, int *link_ptr_ret,
                             int *index)
{
	int i = 0;
	*link_ptr_ret = 0;
	*index = -1;
	if (n->highkey != MAX_KEY && key > n->highkey) {
		*link_ptr_ret = 1;
		return n->sibling;
	}
	while (i < n->no_keys && key > n->keys[i]) i++;
	*index = i;
	return n->children[i];
}

static int btree_lookup(btree_t *btree, map_key_t key)
{
	int link_ptr_ret = 0, index = 0, not_locked, ret;
	btree_node_t *n, *t;

TOP:
	//> Empty tree.
	pthread_spin_lock(&btree->lock);
	n = btree->root;
	if (!n) {
		pthread_spin_unlock(&btree->lock);
		return 0;
	}

	if (TRYRDLOCK_NODE(n)) {
		pthread_spin_unlock(&btree->lock);
		goto TOP;
	}
	pthread_spin_unlock(&btree->lock);
	while (!n->leaf) {
		t = n;
		n = btree_node_scan(n, key, &link_ptr_ret, &index);
		not_locked = TRYRDLOCK_NODE(n);
		UNLOCK_NODE(t);
		if (not_locked) goto TOP;
	}

	do {
		t = n;
		n = btree_node_scan(n, key, &link_ptr_ret, &index);
		if (n != NULL) {
			not_locked = TRYRDLOCK_NODE(n);
			UNLOCK_NODE(t);
			if (not_locked) goto TOP;
		}
	} while (link_ptr_ret);

	ret = (t->keys[index] == key);
	UNLOCK_NODE(t);
	return ret;
}

static void btree_traverse_stack(btree_t *btree, map_key_t key,
                          btree_node_t **node_stack, int *node_stack_indexes,
                          int *stack_top)
{
	int index, link_ptr_ret;
	btree_node_t *n, *t;

TOP:
	*stack_top = -1;
	pthread_spin_lock(&btree->lock);
	n = btree->root;
	if (!n) return;

	if ((!n->leaf && TRYRDLOCK_NODE(n)) ||
	     (n->leaf && TRYWRLOCK_NODE(n))) {
		pthread_spin_unlock(&btree->lock);
		goto TOP;
	}
	pthread_spin_unlock(&btree->lock);
	while (!n->leaf) {
		t = n;
		n = btree_node_scan(n, key, &link_ptr_ret, &index);
		if (!link_ptr_ret) {
			node_stack[++(*stack_top)] = t;
			node_stack_indexes[*stack_top] = index;
		}
		if ((!n->leaf && TRYRDLOCK_NODE(n)) ||
		     (n->leaf && TRYWRLOCK_NODE(n))) {
			UNLOCK_NODE(t);
			goto TOP;
		}
		UNLOCK_NODE(t);
	}
	index = btree_node_search(n, key);
	node_stack[++(*stack_top)] = n;
	node_stack_indexes[*stack_top] = index;
}

static btree_node_t *move_right(btree_node_t *n, map_key_t key, int *index)
{
	int link_ptr_ret;
	btree_node_t *t = n;

	n = btree_node_scan(n, key, &link_ptr_ret, index);
	while (link_ptr_ret) {
		LOCK_NODE(n);
		UNLOCK_NODE(t);
		t = n;
		n = btree_node_scan(n, key, &link_ptr_ret, index);
	}
	return t;
}

static int _do_insert(btree_t *btree, map_key_t key, void *val,
                      btree_node_t **node_stack, int *node_stack_indexes,
                      int stack_top)
{
	btree_node_t *n, *internal;
	int index, internal_index, key_to_add;
	void *ptr_to_add;
  
	n = node_stack[stack_top];
//	LOCK_NODE(n);
	n = move_right(n, key, &index);

	//> Key already in the leaf.
	if (index < 2 * BTREE_ORDER && index < n->no_keys && key == n->keys[index]) {
		UNLOCK_NODE(n);
		return 0;
	}

	//> Case of a not full leaf.
	if (n->no_keys < 2 * BTREE_ORDER) {
		btree_node_insert_index(n, index, key, val);
		UNLOCK_NODE(n);
		return 1;
	}

	//> Case of full leaf.
	btree_node_t *rnode = btree_leaf_split(n, index, key, val);

	key_to_add = n->keys[n->no_keys-1];
	ptr_to_add = rnode;
	rnode->highkey = n->highkey;
	n->highkey = key_to_add;

	while (1) {
		stack_top--;

		//> We surpassed the root. New root needs to be created.
		if (stack_top < 0) {
			pthread_spin_lock(&btree->lock);
			btree->root = btree_node_new(0);
			btree_node_insert_index(btree->root, 0, key_to_add, ptr_to_add);
			btree->root->children[0] = n;
			pthread_spin_unlock(&btree->lock);
			UNLOCK_NODE(n);
			break;
		}

		internal = node_stack[stack_top];
		LOCK_NODE(internal);
		internal = move_right(internal, key_to_add, &internal_index);

		//> Internal node not full.
		if (internal->no_keys < 2 * BTREE_ORDER) {
			btree_node_insert_index(internal, internal_index, key_to_add, ptr_to_add);
			UNLOCK_NODE(n);
			UNLOCK_NODE(internal);
			break;
		}

		//> Internal node full.
		rnode = btree_internal_split(internal, internal_index, key_to_add, ptr_to_add,
		                             &key_to_add);
		rnode->highkey = internal->highkey;
		if (KEY_CMP(rnode->keys[rnode->no_keys-1], rnode->highkey) > 0)
			rnode->highkey = rnode->keys[rnode->no_keys-1];
		internal->highkey = key_to_add;
		ptr_to_add = rnode;
		UNLOCK_NODE(n);
		n = internal;
	}
	return 1;
}

static int btree_insert(btree_t *btree, map_key_t key, void *val)
{
	btree_node_t *n;
	btree_node_t *node_stack[20];
	int node_stack_indexes[20];
	int stack_top = -1;

	//> Route to the appropriate leaf.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes,
	                     &stack_top);

	//> Empty tree case.
	if (stack_top == -1) {
//		pthread_spin_lock(&btree->lock);
		assert(btree->root == NULL);
		n = btree_node_new(1);
		btree_node_insert_index(n, 0, key, val);
		btree->root = n;
		n->sibling = NULL;
		pthread_spin_unlock(&btree->lock);
		return 1;
	}

	return _do_insert(btree, key, val, node_stack, node_stack_indexes, stack_top);
}

static int _do_delete(map_key_t key, btree_node_t **node_stack,
                      int *node_stack_indexes, int stack_top)
{
	int index = node_stack_indexes[stack_top];
	int ret = 1;
	btree_node_t *n = node_stack[stack_top];

//	LOCK_NODE(n);
	if (index >= n->no_keys || key != n->keys[index]) ret = 0;
	else btree_node_delete_index(n, index);
	UNLOCK_NODE(n);
	return ret;
}

static int btree_delete(btree_t *btree, map_key_t key)
{
	btree_node_t *node_stack[20];
	int node_stack_indexes[20];
	int stack_top = -1;

	//> Route to the appropriate leaf.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
	//> Empty tree case.
	if (stack_top == -1) {
		pthread_spin_unlock(&btree->lock);
		return 0;
	}
	return _do_delete(key, node_stack, node_stack_indexes, stack_top);
}

static int btree_update(btree_t *btree, map_key_t key, void *val)
{
	btree_node_t *n;
	btree_node_t *node_stack[20];
	int node_stack_indexes[20];
	int stack_top = -1, index;

	//> Route to the appropriate leaf.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);

	//> Empty tree case.
	if (stack_top == -1) {
//		pthread_spin_lock(&btree->lock);
		assert(btree->root == NULL);
		n = btree_node_new(1);
		btree_node_insert_index(n, 0, key, val);
		btree->root = n;
		n->sibling = NULL;
		pthread_spin_unlock(&btree->lock);
		return 1;
	}

	index = node_stack_indexes[stack_top];
	n = node_stack[stack_top];

	if (index >= n->no_keys || key != n->keys[index])
		return _do_insert(btree, key, val, node_stack, node_stack_indexes,
		                  stack_top);
	else
		return _do_delete(key, node_stack, node_stack_indexes, stack_top) + 2;
}

/******************************************************************************/
/*      Map interface implementation                                          */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(btree_node_t));
	return btree_new();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(btree_node_t));
	return NULL;
}

void map_tdata_print(void *thread_data)
{
	return;
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
}

int map_lookup(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = btree_lookup(map, key);
	return ret; 
}

int map_rquery(void *map, void *thread_data, map_key_t key1, map_key_t key2)
{
	printf("Range Query operation is not implemented\n");
	return 0;
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = btree_insert(map, key, value);
	return ret;
}

int map_delete(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = btree_delete(map, key);
	return ret;
}

int map_update(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = btree_update(map, key, value);
	return ret;
}

int map_validate(void *map)
{
	int ret = 0;
	ret = btree_validate_helper(map);
	return ret;
}

char *map_name()
{
	return "btree-blink-locks";
}

void map_print(void *map)
{
	btree_print(map);
}
