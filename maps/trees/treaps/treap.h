#ifndef _TREAP_H_
#define _TREAP_H_

#include <assert.h>
#include <string.h> //> memset()
#include <stdlib.h> //> rand() 
#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
#include <pthread.h> //> pthread_spinlock_t
#endif
#include "../../key/key.h"
#include "../../map.h"
#include "alloc.h"

#ifndef TREAP_EXTERNAL_NODE_ORDER
#define TREAP_EXTERNAL_NODE_ORDER 64
#endif

#define TREAP_NODE_MAGIC_NUMBER 13

typedef struct treap_node_internal treap_node_internal_t;
typedef struct treap_node_external treap_node_external_t;

//> 'left' and 'right' point either to internal or external type of nodes
struct treap_node_internal {
	char magic_number;
	char is_internal;
	map_key_t key;
	unsigned long long weight;
	void *left,
	     *right;
};

struct treap_node_external {
	char magic_number;
	char is_internal;
	int nr_keys;
	map_key_t keys[TREAP_EXTERNAL_NODE_ORDER];
	void *values[TREAP_EXTERNAL_NODE_ORDER];
};

typedef struct {
	//> root points to either an internal or an external node
	void *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spinlock_t lock;
#	endif
} treap_t;

//> Allocators for internal and external nodes
static __thread void *nalloc_internal;
static __thread void *nalloc_external;

//> Value is only used for external nodes.
static void *treap_node_new(map_key_t key, void *value, int is_internal)
{
	treap_node_internal_t *internal;
	treap_node_external_t *external;

	// FIXME: don't use rand() for random weights
	if (is_internal) {
		internal = nalloc_alloc_node(nalloc_internal);
		memset(internal, 0, sizeof(*internal));
		internal->magic_number = TREAP_NODE_MAGIC_NUMBER;
		internal->is_internal = 1;
		KEY_COPY(internal->key, key);
		internal->weight = rand() % 1000000;
		return internal;
	} else {
		external = nalloc_alloc_node(nalloc_external);
		memset(external, 0, sizeof(*external));
		external->magic_number = TREAP_NODE_MAGIC_NUMBER;
		KEY_COPY(external->keys[0], key);
		external->values[0] = value;
		external->nr_keys = 1;
		return external;
	}
}

static char treap_node_is_internal(void *node)
{
	char *chr_ptr = (char *)node;
	char magic_number = chr_ptr[0];
	if (magic_number != TREAP_NODE_MAGIC_NUMBER) {
		printf("treap node magic number is different than %d (it is %d)\n",
		       TREAP_NODE_MAGIC_NUMBER, magic_number);
		return -1;
	}
	return chr_ptr[1];
}

static void treap_node_print_internal(treap_node_internal_t *node)
{
	printf("I: [key: ");
	KEY_PRINT(node->key, "", "");
	printf(", weight: %llu]\n", node->weight);
}

static void treap_node_print_external(treap_node_external_t *node)
{
	int i;
	printf("E: [keys: ");
	for (i=0; i < node->nr_keys; i++)
		KEY_PRINT(node->keys[i], "", "| ");
	printf("]\n");
}

static void treap_node_print(void *node)
{
	char is_internal = treap_node_is_internal(node);
	if (is_internal == 1)      treap_node_print_internal(node);
	else if (is_internal == 0) treap_node_print_external(node);
	else                       printf("ERROR\n");
}

static char treap_node_external_full(treap_node_external_t *node)
{
	return node->nr_keys >= TREAP_EXTERNAL_NODE_ORDER;
}

static char treap_node_external_empty(treap_node_external_t *node)
{
	return node->nr_keys == 0;
}

static int treap_node_external_indexof(treap_node_external_t *node, map_key_t key)
{
	int cmp, i;

	for (i=0; i < node->nr_keys; i++) {
		cmp = KEY_CMP(node->keys[i], key);
		if (cmp == 0) return i;
		if (cmp > 0) return -1;
	}
	return -1;
}

static treap_node_external_t *treap_node_external_split(treap_node_external_t *node)
{
	int i;
	treap_node_external_t *new_external = treap_node_new(node->keys[0], NULL, 0);

	for (i=node->nr_keys/2; i < node->nr_keys; i++) {
		KEY_COPY(new_external->keys[i - node->nr_keys/2], node->keys[i]);
		new_external->values[i - node->nr_keys/2] = node->values[i];
	}
	new_external->nr_keys = node->nr_keys - node->nr_keys / 2;
	node->nr_keys = node->nr_keys / 2;
	return new_external;
}

static void treap_node_external_insert(treap_node_external_t *node, map_key_t key,
                                       void *value)
{
	assert(node->nr_keys < TREAP_EXTERNAL_NODE_ORDER);

	int i = node->nr_keys;
	while (i-1 >= 0 && KEY_CMP(node->keys[i-1], key) > 0) {
		KEY_COPY(node->keys[i], node->keys[i-1]);
		node->values[i] = node->values[i-1];
		i--;
	}
	KEY_COPY(node->keys[i], key);
	node->values[i] = value;
	node->nr_keys++;
}

static void treap_node_external_delete_index(treap_node_external_t *node, int index)
{
	assert(index >= 0 && index < TREAP_EXTERNAL_NODE_ORDER);
	int i;
	for (i=index; i < node->nr_keys; i++) {
		KEY_COPY(node->keys[i], node->keys[i+1]);
		node->values[i] = node->values[i+1];
	}
	node->nr_keys--;
}

static void treap_print_rec(void *node, int level)
{
	int i;
	if (treap_node_is_internal(node)) {
		treap_node_internal_t *internal = node;
		treap_print_rec(internal->right, level+1);
		for (i=0; i < level; i++) printf("-"); printf("> ");
		treap_node_print_internal(internal);
		treap_print_rec(internal->left, level+1);
	} else {
		for (i=0; i < level; i++) printf("-"); printf("> ");
		treap_node_print_external(node);
	}
}

static void treap_print(treap_t *treap)
{
	if (treap->root == NULL) printf("EMPTY\n");
	else treap_print_rec(treap->root, 0);
}

static map_key_t treap_max_key(treap_t *treap)
{
	treap_node_internal_t *internal;
	treap_node_external_t *external;
	void *curr = treap->root;

	while (treap_node_is_internal(curr)) {
		internal = curr;
		curr = internal->right;
	}
	external = curr;
	return external->keys[external->nr_keys-1];
}

static treap_t *treap_new()
{
	treap_t *ret;
	XMALLOC(ret, 1);
	ret->root = NULL;

	//> FIXME
	nalloc_internal = nalloc_thread_init(0, sizeof(treap_node_internal_t));
	nalloc_external = nalloc_thread_init(0, sizeof(treap_node_external_t));

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM) || defined(SYNC_RCU_HTM)
	pthread_spin_init(&ret->lock, PTHREAD_PROCESS_SHARED);
#	endif

	return ret;
}

#endif /* _TREAP_H_ */
