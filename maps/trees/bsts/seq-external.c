/**
 * An external (unbalanced) binary search tree.
 **/
#include <stdio.h>
#include <stdlib.h>

#include "bst.h"
#include "print.h"
#define BST_EXTERNAL
#include "validate.h"

#if defined(SYNC_CG_HTM)
#	include "htm/htm.h"
#endif

#define IS_EXTERNAL_NODE(node) \
    ( (node)->left == NULL && (node)->right == NULL )

/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (empty tree) or the last node in the
 * access path. `parent` is either leaf's parent (if `leaf` != NULL) or
 * NULL.
 **/
static inline void _traverse(bst_t *bst, int key, bst_node_t **gparent,
                                                  bst_node_t **parent,
                                                  bst_node_t **leaf)
{
	*gparent = NULL;
	*parent = NULL;
	*leaf = bst->root;

	if (*leaf == NULL)
		return;

	while (!IS_EXTERNAL_NODE(*leaf)) {
		int leaf_key = (*leaf)->key;

		*gparent = *parent;
		*parent = *leaf;
		*leaf = (key <= leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}

static int _bst_lookup_helper(bst_t *bst, int key)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);
	return (leaf && leaf->key == key);
}

static int _bst_insert_helper(bst_t *bst, int key, void *value)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree case
	if (!leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree.
	if (leaf->key == key)
		return 0;

	// Create new internal and leaf nodes.
	bst_node_t *new_internal = bst_node_new(key, NULL);
	if (key <= leaf->key) {
		new_internal->left = bst_node_new(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
		new_internal->right = bst_node_new(key, value);
		new_internal->key = leaf->key;
	}
	if (!parent)                 bst->root = new_internal;
	else if (key <= parent->key) parent->left = new_internal;
	else                         parent->right = new_internal;
	return 1;
}

static int _bst_delete_helper(bst_t *bst, int key)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree or key not in the tree.
	if (!leaf || leaf->key != key)
		return 0;

	// Only one node in the tree.
	if (!parent) {
		bst->root = NULL;
		return 1;
	}
	bst_node_t *sibling = (key <= parent->key) ? parent->right : parent->left;
	if (!gparent)                 bst->root = sibling;
	else if (key <= gparent->key) gparent->left = sibling;
	else                          gparent->right = sibling;
	return 1;
}

static int _bst_update_helper(bst_t *bst, int key, void *value)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree case. Insert
	if (!leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree. Delete
	if (leaf->key == key) {
		if (!parent) {
			bst->root = NULL;
			return 3;
		}
		bst_node_t *sibling = (key <= parent->key) ? parent->right : parent->left;
		if (!gparent)                 bst->root = sibling;
		else if (key <= gparent->key) gparent->left = sibling;
		else                          gparent->right = sibling;
		return 3;
	}

	// Create new internal and leaf nodes.
	bst_node_t *new_internal = bst_node_new(key, NULL);
	if (key <= leaf->key) {
		new_internal->left = bst_node_new(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
		new_internal->right = bst_node_new(key, value);
		new_internal->key = leaf->key;
	}

	if (!parent)                 bst->root = new_internal;
	else if (key <= parent->key) parent->left = new_internal;
	else                         parent->right = new_internal;

	return 1;
}

/******************************************************************************/
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(bst_node_t));
	return _bst_new_helper();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(bst_node_t));
#	if defined(SYNC_CG_HTM)
	return tx_thread_data_new(tid);
#	else
	return NULL;
#	endif
}

void map_tdata_print(void *thread_data)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(thread_data);
#	endif
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(d1, d2, dst);
#	endif
}

int map_lookup(void *map, void *thread_data, int key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((bst_t *)map)->lock);
#	endif

	ret = _bst_lookup_helper(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((bst_t *)map)->lock);
#	endif

	return ret; 
}

int map_rquery(void *map, void *tdata, int key1, int key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *map, void *thread_data, int key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((bst_t *)map)->lock);
#	endif

	ret = _bst_insert_helper(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((bst_t *)map)->lock);
#	endif

	return ret;
}

int map_delete(void *map, void *thread_data, int key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((bst_t *)map)->lock);
#	endif

	ret = _bst_delete_helper(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((bst_t *)map)->lock);
#	endif

	return ret;
}

int map_update(void *map, void *thread_data, int key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((bst_t *)map)->lock);
#	endif

	ret = _bst_update_helper(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((bst_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((bst_t *)map)->lock);
#	endif

	return ret;
}

int map_validate(void *map)
{
	int ret = 0;
	ret = bst_validate(map);
	return ret;
}

char *map_name()
{
#	if defined(SYNC_CG_SPINLOCK)
	return "bst-cg-lock-external";
#	elif defined(SYNC_CG_HTM)
	return "bst-cg-htm-external";
#	else
	return "bst-sequential-external";
#	endif
}

void map_print(void *map)
{
	bst_print(map);
}
