/**
 * An internal (unbalanced) binary search tree.
 **/
#include <stdio.h>
#include <stdlib.h>

#include "../../key/key.h"

#include "bst.h"
#include "print.h"
#define BST_INTERNAL
#include "validate.h"

#if defined(SYNC_CG_HTM)
#	include "htm/htm.h"
#endif

/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 **/
static inline void _traverse(bst_t *bst, map_key_t key,
                             bst_node_t **parent, bst_node_t **leaf)
{
	*parent = NULL;
	*leaf = bst->root;

	while (*leaf) {
		if (KEY_CMP((*leaf)->key, key) == 0) return;

		*parent = *leaf;
		*leaf = (KEY_CMP(key, (*leaf)->key) < 0) ? (*leaf)->left : (*leaf)->right;
	}
}

static int _bst_lookup_helper(bst_t *bst, map_key_t key)
{
	bst_node_t *parent, *leaf;
	_traverse(bst, key, &parent, &leaf);
	return (leaf != NULL);
}

static int _bst_insert_helper(bst_t *bst, map_key_t key, void *value)
{
	bst_node_t *parent, *leaf;

	_traverse(bst, key, &parent, &leaf);

	// Empty tree case
	if (!parent && !leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Key already in the tree.
	if (leaf) return 0;

	if (KEY_CMP(key, parent->key) < 0) parent->left = bst_node_new(key, value);
	else                               parent->right = bst_node_new(key, value);

	return 1;
}

static inline void _find_successor(bst_node_t *node, bst_node_t **parent,
                                                     bst_node_t **leaf)
{
	*parent = node;
	*leaf = node->right;

	while ((*leaf)->left) {
		*parent = *leaf;
		*leaf = (*leaf)->left;
	}
}

static int _bst_delete_helper(bst_t *bst, map_key_t key)
{
	bst_node_t *parent, *leaf, *succ, *succ_parent;

	_traverse(bst, key, &parent, &leaf);

	// Key not in the tree (also includes empty tree case).
	if (!leaf) return 0;

	if (!leaf->left) {
		if (!parent) bst->root = leaf->right;
		else if (parent->left == leaf) parent->left = leaf->right;
		else if (parent->right == leaf) parent->right = leaf->right;
	} else if (!leaf->right) {
		if (!parent) bst->root = leaf->left;
		else if (parent->left == leaf) parent->left = leaf->left;
		else if (parent->right == leaf) parent->right = leaf->left;
	} else { // Leaf has two children.
		_find_successor(leaf, &succ_parent, &succ);

		KEY_COPY(leaf->key, succ->key);
		if (succ_parent->left == succ) succ_parent->left = succ->right;
		else succ_parent->right = succ->right;
	}

	return 1;
}

static int _bst_update_helper(bst_t *bst, map_key_t key, void *value)
{
	bst_node_t *parent, *leaf, *succ, *succ_parent;

	_traverse(bst, key, &parent, &leaf);

	// Empty tree case
	if (!parent && !leaf) {
		bst->root = bst_node_new(key, value);
		return 1;
	}

	// Insertion
	if (!leaf) {
		if (KEY_CMP(key, parent->key) < 0) parent->left = bst_node_new(key, value);
		else                               parent->right = bst_node_new(key, value);
		return 1;
	}

	//> Deletion
	if (!leaf->left) {
		if (!parent) bst->root = leaf->right;
		else if (parent->left == leaf) parent->left = leaf->right;
		else if (parent->right == leaf) parent->right = leaf->right;
	} else if (!leaf->right) {
		if (!parent) bst->root = leaf->left;
		else if (parent->left == leaf) parent->left = leaf->left;
		else if (parent->right == leaf) parent->right = leaf->left;
	} else { // Leaf has two children.
		_find_successor(leaf, &succ_parent, &succ);
		KEY_COPY(leaf->key, succ->key);
		if (succ_parent->left == succ) succ_parent->left = succ->right;
		else succ_parent->right = succ->right;
	}

	return 3;

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
	return bst_validate(map);
}

char *map_name()
{
#	if defined(SYNC_CG_SPINLOCK)
	return "bst-cg-lock-internal";
#	elif defined(SYNC_CG_HTM)
	return "bst-cg-htm-internal";
#	else
	return "bst-sequential-internal";
#	endif
}

void map_print(void *map)
{
	bst_print(map);
}
