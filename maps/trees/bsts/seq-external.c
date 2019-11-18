/**
 * An external (unbalanced) binary search tree.
 **/
#include <stdio.h>
#include <stdlib.h>

#include "../../key/key.h"

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
static inline void _traverse(bst_t *bst, map_key_t key, bst_node_t **gparent,
                             bst_node_t **parent, bst_node_t **leaf)
{
	*gparent = NULL;
	*parent = NULL;
	*leaf = bst->root;

	if (*leaf == NULL) return;

	while (!IS_EXTERNAL_NODE(*leaf)) {
		*gparent = *parent;
		*parent = *leaf;
		*leaf = (KEY_CMP(key, (*leaf)->key) <= 0) ? (*leaf)->left : (*leaf)->right;
	}
}

static int _bst_lookup_helper(bst_t *bst, map_key_t key)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);
	return (leaf) && (KEY_CMP(leaf->key, key) == 0);
}

static int _bst_insert_helper(bst_t *bst, map_key_t key, void *value)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree case
	if (!leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree.
	if (KEY_CMP(leaf->key, key) == 0) return 0;

	// Create new internal and leaf nodes.
	bst_node_t *new_internal = bst_node_new(key, NULL);
	if (KEY_CMP(key, leaf->key) <= 0) {
		new_internal->left = bst_node_new(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
		new_internal->right = bst_node_new(key, value);
		KEY_COPY(new_internal->key, leaf->key);
	}
	if (!parent)                             bst->root = new_internal;
	else if (KEY_CMP(key, parent->key) <= 0) parent->left = new_internal;
	else                                     parent->right = new_internal;
	return 1;
}

static int _bst_delete_helper(bst_t *bst, map_key_t key)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree or key not in the tree.
	if (!leaf || KEY_CMP(leaf->key, key) != 0)
		return 0;

	// Only one node in the tree.
	if (!parent) {
		bst->root = NULL;
		return 1;
	}

	bst_node_t *sibling = KEY_CMP(key, parent->key) <= 0 ? parent->right :
	                                                       parent->left;
	if (!gparent)                 bst->root = sibling;
	else if (KEY_CMP(key, gparent->key) <= 0) gparent->left = sibling;
	else                          gparent->right = sibling;
	return 1;
}

static int _bst_update_helper(bst_t *bst, map_key_t key, void *value)
{
	bst_node_t *gparent, *parent, *leaf;

	_traverse(bst, key, &gparent, &parent, &leaf);

	// Empty tree case. Insert
	if (!leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree. Delete
	if (KEY_CMP(leaf->key, key) == 0) {
		if (!parent) {
			bst->root = NULL;
			return 3;
		}

		bst_node_t *sibling = KEY_CMP(key, parent->key) <= 0 ? parent->right :
		                                                       parent->left;
		if (!gparent)                 bst->root = sibling;
		else if (KEY_CMP(key, gparent->key) <= 0) gparent->left = sibling;
		else                          gparent->right = sibling;
		return 3;
	}

	// Create new internal and leaf nodes.
	bst_node_t *new_internal = bst_node_new(key, NULL);
	if (KEY_CMP(key, leaf->key) <= 0) {
		new_internal->left = bst_node_new(key, value);
		new_internal->right = leaf;
	} else {
		new_internal->left = leaf;
		new_internal->right = bst_node_new(key, value);
		KEY_COPY(new_internal->key, leaf->key);
	}

	if (!parent)                 bst->root = new_internal;
	else if (KEY_CMP(key, parent->key) <= 0) parent->left = new_internal;
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

int map_lookup(void *map, void *thread_data, map_key_t key)
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

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
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

int map_delete(void *map, void *thread_data, map_key_t key)
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

int map_update(void *map, void *thread_data, map_key_t key, void *value)
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
