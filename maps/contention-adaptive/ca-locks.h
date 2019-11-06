#ifndef _CA_LOCKS_H_
#define _CA_LOCKS_H_

#include <assert.h>
#include <pthread.h>
#include "alloc.h"
#include "stack.h"
#include "tdata.h"
#include "../key/key.h"
#include "seq_ds.h"

#define CA_NODE_MAGIC_NUMBER 18

#define STAT_LOCK_HIGH_CONTENTION_LIMIT 1000
#define STAT_LOCK_LOW_CONTENTION_LIMIT -1000
#define STAT_LOCK_FAIL_CONTRIB 250
#define STAT_LOCK_SUCC_CONTRIB 1

typedef struct {
	char magic_number;
	char is_route;
	char valid;
	pthread_spinlock_t lock;
	map_key_t key;
	void *left, *right;
} route_node_t;

typedef struct {
	char magic_number;
	char is_route;
	char valid;
	pthread_spinlock_t lock;
	int lock_statistics;
	seq_ds_t *root;
	void *padding;
} base_node_t;

typedef struct {
	pthread_spinlock_t lock;
	//> Root is either a route or a base node
	void *root;
} ca_t;

//> Allocators for route and base nodes
static __thread void *nalloc_route;
static __thread void *nalloc_base;

//> key is only used for route nodes
static void *ca_node_new(map_key_t key, int is_route)
{
	route_node_t *rnode;
	base_node_t *bnode;

	//> FIXME do not use XMALLOC here
	if (is_route) {
		rnode = nalloc_alloc_node(nalloc_route);
		rnode->magic_number = CA_NODE_MAGIC_NUMBER;
		rnode->is_route = 1;
		rnode->valid = 1;
		pthread_spin_init(&rnode->lock, PTHREAD_PROCESS_SHARED);
		KEY_COPY(rnode->key, key);
		rnode->left = rnode->right = 0;
		return rnode;
	} else {
		bnode = nalloc_alloc_node(nalloc_base);
		bnode->magic_number = CA_NODE_MAGIC_NUMBER;
		bnode->is_route = 0;
		bnode->valid = 1;
		pthread_spin_init(&bnode->lock, PTHREAD_PROCESS_SHARED);
		bnode->lock_statistics = 0;
		bnode->root = seq_ds_new();
		return bnode;
	}
}

static char ca_node_is_route(void *node)
{
	char *chr_ptr = (char *)node;
	char magic_number = chr_ptr[0];
	if (magic_number != CA_NODE_MAGIC_NUMBER) {
		printf("CA node magic number is different than %d (it is %d)\n",
		       CA_NODE_MAGIC_NUMBER, magic_number);
		return -1;
	}
	return chr_ptr[1];
}

static void ca_node_route_lock(route_node_t *rnode)
{
	pthread_spin_lock(&rnode->lock);
}

static void ca_node_route_unlock(route_node_t *rnode)
{
	pthread_spin_unlock(&rnode->lock);
}

static void ca_node_base_lock(base_node_t *bnode)
{
	if (pthread_spin_trylock(&bnode->lock) == 0) {
		//> No contention
		bnode->lock_statistics -= STAT_LOCK_SUCC_CONTRIB;
	} else {
		//> Could not lock with trylock(), we have to block
		pthread_spin_lock(&bnode->lock);
		bnode->lock_statistics += STAT_LOCK_FAIL_CONTRIB;
	}
}

static void ca_node_base_unlock(base_node_t *bnode)
{
	pthread_spin_unlock(&bnode->lock);
}

static base_node_t *_get_base_node(ca_t *ca,
                                   route_node_t **parent,
                                   route_node_t **gparent,
                                   map_key_t key)
{
	route_node_t *p = NULL, *gp = NULL;
	void *curr = ca->root;
	route_node_t *rnode;
	while (ca_node_is_route(curr)) {
		gp = p;
		p = curr;
		rnode = curr;
		if (KEY_CMP(key, rnode->key) <= 0) curr = rnode->left;
		else                               curr = rnode->right;
	}
	*parent = p;
	*gparent = gp;
	return curr;
}

static base_node_t *_get_base_node_stack(ca_t *ca, stack_t *stack, map_key_t key)
{
	void *curr = ca->root;
	route_node_t *rnode;
	while (ca_node_is_route(curr)) {
		stack_push(stack, curr);
		rnode = curr;
		if (KEY_CMP(key, rnode->key) <= 0) curr = rnode->left;
		else                               curr = rnode->right;
	}
	stack_push(stack, curr);
	return curr;
}

static base_node_t *ca_leftmost_base(void *node, route_node_t **parent,
                                                 route_node_t **gparent)
{
	route_node_t *p = NULL, *gp = NULL;
	route_node_t *rnode;
	while (ca_node_is_route(node)) {
		gp = p;
		p = node;
		rnode = node;
		node = rnode->left;
	}
	*parent = p;
	*gparent = gp;
	return node;
}

static base_node_t *ca_rightmost_base(void *node, route_node_t **parent,
                                                  route_node_t **gparent)
{
	route_node_t *p = NULL, *gp = NULL;
	route_node_t *rnode;
	while (ca_node_is_route(node)) {
		gp = p;
		p = node;
		rnode = node;
		node = rnode->right;
	}
	*parent = p;
	*gparent = gp;
	return node;
}

//> Called with bnode locked
static void ca_split(ca_t *ca, base_node_t *bnode,
                               route_node_t *parent)
{
	base_node_t *left_bnode, *right_bnode;
	route_node_t *new_rnode;

	if (seq_ds_size(bnode->root) < 10) return;

	left_bnode = ca_node_new(-1, 0);
	right_bnode = ca_node_new(-1, 0);
	left_bnode->root = seq_ds_split(bnode->root, &right_bnode->root);

	assert(left_bnode->root != NULL);

	bnode->valid = 0;

	new_rnode = ca_node_new(seq_ds_max_key(left_bnode->root), 1);
	new_rnode->left  = left_bnode;
	new_rnode->right = right_bnode;
	if (parent) {
		if (parent->left == bnode) parent->left = new_rnode;
		else                       parent->right = new_rnode;
	} else {
		ca->root = new_rnode;
	}
}

//> Called with bnode locked
static void ca_join(ca_t *ca, base_node_t *bnode,
                    route_node_t *parent, route_node_t *gparent)
{
	base_node_t *new_bnode;
	base_node_t *lmost_base, *rmost_base;
	route_node_t *lmparent, *lmgparent; //> Left-most's parent and grandparent
	route_node_t *rmparent, *rmgparent; //> Right-most's parent and grandparent
	void *sibling;

	if (parent == NULL) return;

	new_bnode = ca_node_new(-1, 0);

	if (parent->left == bnode) {
		sibling = parent->right;

		lmost_base = ca_leftmost_base(sibling, &lmparent, &lmgparent);
		if (lmgparent == NULL) lmgparent = gparent;
		if (lmparent == NULL)  lmparent = parent;
		if (lmgparent == NULL && lmparent != NULL) lmgparent = parent;

		//> Try to lock lmost_base and check if valid
		if (pthread_spin_trylock(&lmost_base->lock) != 0) {
			return;
		} else if (lmost_base->valid == 0) {
			pthread_spin_unlock(&lmost_base->lock);
			return;
		}

		//> Unlink bnode
		if (gparent == NULL) ca->root = parent->right;
		else if (gparent->left == parent) gparent->left = parent->right;
		else if (gparent->right == parent) gparent->right = parent->right;
		bnode->valid = 0;
		parent->valid = 0;

		new_bnode->root = seq_ds_join(bnode->root, lmost_base->root);
		if (lmparent == parent) lmparent = gparent; //> lmparent has been spliced out
		if (lmparent == NULL) ca->root = new_bnode;
		else if (lmparent->left == lmost_base) lmparent->left = new_bnode;
		else                                   lmparent->right = new_bnode;
		lmost_base->valid = 0;
		pthread_spin_unlock(&lmost_base->lock);
	} else if (parent->right == bnode) {
		sibling = parent->left;

		rmost_base = ca_rightmost_base(sibling, &rmparent, &rmgparent);
		if (rmgparent == NULL) rmgparent = gparent;
		if (rmparent == NULL)  rmparent = parent;
		if (rmgparent == NULL && rmparent != NULL) rmgparent = parent;

		//> Try to lock rmost_base and check if valid
		if (pthread_spin_trylock(&rmost_base->lock) != 0) {
			return;
		} else if (rmost_base->valid == 0) {
			pthread_spin_unlock(&rmost_base->lock);
			return;
		}

		//> Unlink bnode
		if (gparent == NULL) ca->root = parent->left;
		else if (gparent->left == parent) gparent->left = parent->left;
		else if (gparent->right == parent) gparent->right = parent->left;
		bnode->valid = 0;
		parent->valid = 0;

		new_bnode->root = seq_ds_join(rmost_base->root, bnode->root);
		if (rmparent == parent) rmparent = gparent; //> rmparent has been spliced out
		if (rmparent == NULL) ca->root = new_bnode;
		else if (rmparent->left == rmost_base) rmparent->left = new_bnode;
		else                                   rmparent->right = new_bnode;
		rmost_base->valid = 0;
		pthread_spin_unlock(&rmost_base->lock);
	}
}

//> Called with bnode locked
static void ca_adapt_if_needed(ca_t *ca,
                               base_node_t *bnode,
                               route_node_t *parent,
                               route_node_t *gparent,
                               ca_tdata_t *tdata)
{
	if (bnode->lock_statistics > STAT_LOCK_HIGH_CONTENTION_LIMIT) {
		ca_split(ca, bnode, parent);
		bnode->lock_statistics = 0;
		tdata->splits++;
	} else if (bnode->lock_statistics < STAT_LOCK_LOW_CONTENTION_LIMIT) {
		ca_join(ca, bnode, parent, gparent);
		bnode->lock_statistics = 0;
		tdata->joins++;
	}
}

static ca_t *ca_new()
{
	ca_t *ca;
	XMALLOC(ca, 1);
	pthread_spin_init(&ca->lock, PTHREAD_PROCESS_SHARED);
	ca->root = ca_node_new(-1, 0);
	return ca;
}

static void _print_helper_rec(void *node, int depth)
{
	int i;
	route_node_t *rnode;
	base_node_t *bnode;

	if (ca_node_is_route(node)) {
		rnode = node;
		_print_helper_rec(rnode->right, depth+1);
		for (i=0; i < depth; i++) printf("-");
		printf("-> [ROUTE] ");
		KEY_PRINT(rnode->key, "", "\n");
		_print_helper_rec(rnode->left, depth+1);
	} else {
		bnode = node;
		for (i=0; i < depth; i++) printf("-");
		printf("-> [BASE] (size: %u min: %d max: %d)\n",
		        seq_ds_size(bnode->root),
		        seq_ds_min_key(bnode->root),
		        seq_ds_max_key(bnode->root));
	}
}

static void ca_print(ca_t *ca)
{
	_print_helper_rec(ca->root, 0);
}

#endif /* _CA_LOCKS_H_ */
