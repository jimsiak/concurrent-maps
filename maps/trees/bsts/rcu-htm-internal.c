#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <string.h>  //> memset() for per thread allocator

#include "alloc.h"
#include "ht.h"
#include "../../map.h"
#include "../../key/key.h"
#include "../../rcu-htm/tdata.h"
#include "htm/htm.h"
#define SYNC_RCU_HTM
#include "bst.h"
#include "print.h"
#define BST_INTERNAL
#include "validate.h"

/**
 * Traverses the tree `bst` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 **/
static inline void _traverse(bst_t *bst, int key, bst_node_t **parent,
                                                  bst_node_t **leaf)
{
	*parent = NULL;
	*leaf = bst->root;

	while (*leaf) {
		int leaf_key = (*leaf)->key;
		if (leaf_key == key)
			return;

		*parent = *leaf;
		*leaf = (key < leaf_key) ? (*leaf)->left : (*leaf)->right;
	}
}

//> Be careful when changing this
#define MAX_HEIGHT 100

static inline void _traverse_with_stack(bst_t *avl, int key,
                                        bst_node_t *node_stack[MAX_HEIGHT],
                                        int *stack_top)
{
	bst_node_t *parent = NULL, *leaf = avl->root;
	*stack_top = -1;
	while (leaf) {
		node_stack[++(*stack_top)] = leaf;
		int leaf_key = leaf->key;
		if (leaf_key == key) return;
		parent = leaf;
		leaf = (key < leaf_key) ? leaf->left : leaf->right;
	}
}

static int _bst_lookup_helper(bst_t *bst, int key)
{
	bst_node_t *parent, *leaf;

	_traverse(bst, key, &parent, &leaf);
	return (leaf != NULL);
}

static bst_node_t *_insert_with_copy(int key, void *value,
        bst_node_t *node_stack[MAX_HEIGHT], int stack_top, tdata_t *tdata,
        bst_node_t **tree_copy_root_ret, int *connection_point_stack_index)
{
	bst_node_t *new_node;
	bst_node_t *connection_point;

	// Create new node.
	new_node = bst_node_new(key, value);

	*tree_copy_root_ret = new_node;
	*connection_point_stack_index = (stack_top >= 0) ? stack_top : -1;
	connection_point = (stack_top >= 0) ? node_stack[stack_top] : NULL;

	return connection_point;
}

static int _bst_insert_helper(bst_t *bst, int key, void *value, tdata_t *tdata)
{
	bst_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i;
	bst_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&bst->lock);
		_traverse_with_stack(bst, key, node_stack, &stack_top);
		if (stack_top >= 0 && node_stack[stack_top]->key == key) {
			pthread_spin_unlock(&bst->lock);
			return 0;
		}
		connection_point_stack_index = -1;
		connection_point = _insert_with_copy(key, value,
		                           node_stack, stack_top, tdata, &tree_copy_root,
		                           &connection_point_stack_index);
		if (!connection_point) {
			bst->root = tree_copy_root;
		} else {
			if (key < connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}
		pthread_spin_unlock(&bst->lock);
		return 1;
	}

	/* Asynchronized traversal. If key is there we can safely return. */
	_traverse_with_stack(bst, key, node_stack, &stack_top);
	if (stack_top >= 0 && node_stack[stack_top]->key == key)
		return 0;

	// For now let's ignore empty tree case and case with only one node in the tree.
	connection_point_stack_index = -1;
	connection_point = _insert_with_copy(key, value,
	                             node_stack, stack_top, tdata, &tree_copy_root,
	                             &connection_point_stack_index);

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	/* Transactional verification. */
	while (bst->lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (bst->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (key < node_stack[stack_top]->key && node_stack[stack_top]->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (key > node_stack[stack_top]->key && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (bst->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			bst_node_t *curr = bst->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key <= node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				bst_node_t **np = tdata->ht->entries[i][j];
				bst_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}


		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			bst->root = tree_copy_root;
		} else {
			if (key < connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return 1;
}

static inline void _find_successor_with_stack(bst_node_t *node,
                                              bst_node_t *node_stack[MAX_HEIGHT],
                                              int *stack_top, tdata_t *tdata)
{
	bst_node_t *parent, *leaf, *l, *r;

	l = node->left;
	r = node->right;
	ht_insert(tdata->ht, &node->left, l);
	ht_insert(tdata->ht, &node->right, r);
	if (!l || !r)
		return;

	parent = node;
	leaf = r;
	node_stack[++(*stack_top)] = leaf;

	while ((l = leaf->left) != NULL) {
		ht_insert(tdata->ht, &leaf->left, l);
		parent = leaf;
		leaf = l;
		node_stack[++(*stack_top)] = leaf;
	}
	ht_insert(tdata->ht, &leaf->left, NULL);
}

static int _delete_with_copy(int key,
        bst_node_t *node_stack[MAX_HEIGHT], int stack_top, tdata_t *tdata,
        bst_node_t **tree_copy_root_ret, int *connection_point_stack_index,
        int *new_stack_top, bst_node_t **connection_point)
{
	bst_node_t *tree_copy_root;
	bst_node_t *original_to_be_deleted = node_stack[stack_top];
	bst_node_t *to_be_deleted;
	int to_be_deleted_stack_index = stack_top, i;

	assert(stack_top >= 0);

	_find_successor_with_stack(original_to_be_deleted, node_stack, &stack_top, tdata);
	*new_stack_top = stack_top;
	to_be_deleted = node_stack[stack_top];

	bst_node_t *l = to_be_deleted->left;
	bst_node_t *r = to_be_deleted->right;
	ht_insert(tdata->ht, &to_be_deleted->left, l);
	ht_insert(tdata->ht, &to_be_deleted->right, r);
	tree_copy_root = l != NULL ? l : r;
	*connection_point_stack_index = (stack_top > 0) ? stack_top - 1 : -1;
	*connection_point = (stack_top > 0) ? node_stack[stack_top-1] : NULL;

	// We may need to copy the access path from the originally deleted node
	// up to the current connection_point.
	if (to_be_deleted_stack_index <= *connection_point_stack_index) {
		for (i=*connection_point_stack_index; i >= to_be_deleted_stack_index; i--) {
			bst_node_t *curr_cp = bst_node_new_copy(node_stack[i]);
			ht_insert(tdata->ht, &node_stack[i]->left, curr_cp->left);
			ht_insert(tdata->ht, &node_stack[i]->right, curr_cp->right);

			if (key < curr_cp->key) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
		}
		tree_copy_root->key = to_be_deleted->key;
		*connection_point = to_be_deleted_stack_index > 0 ? 
		                             node_stack[to_be_deleted_stack_index - 1] :
		                             NULL;
		*connection_point_stack_index = to_be_deleted_stack_index - 1;
	}

	*tree_copy_root_ret = tree_copy_root;
	return 1;
}

static int _bst_delete_helper(bst_t *bst, int key, tdata_t *tdata)
{
	bst_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i, ret;
	bst_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&bst->lock);
		_traverse_with_stack(bst, key, node_stack, &stack_top);
		if (stack_top < 0 || node_stack[stack_top]->key != key) {
			pthread_spin_unlock(&bst->lock);
			return 0;
		}
		connection_point_stack_index = -1;
		ret = _delete_with_copy(key,
		                        node_stack, stack_top, tdata, &tree_copy_root,
		                        &connection_point_stack_index, &stack_top,
		                        &connection_point);
		if (ret == 0) {
			pthread_spin_unlock(&bst->lock);
			return 0;
		}
		if (!connection_point) {
			bst->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}
		pthread_spin_unlock(&bst->lock);
		return 1;
	}

	/* Asynchronized traversal. If key is there we can safely return. */
	_traverse_with_stack(bst, key, node_stack, &stack_top);
	if (stack_top < 0 || node_stack[stack_top]->key != key)
		return 0;

	// For now let's ignore empty tree case and case with only one node in the tree.
	connection_point_stack_index = -1;
	ret = _delete_with_copy(key,
	                        node_stack, stack_top, tdata, &tree_copy_root,
	                        &connection_point_stack_index, &stack_top,
	                        &connection_point);
	if (ret == 0) return 0;

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	/* Transactional verification. */
	while (bst->lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (bst->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (bst->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (node_stack[stack_top]->left != NULL && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key < node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			bst_node_t *curr = bst->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key < node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				bst_node_t **np = tdata->ht->entries[i][j];
				bst_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}


		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			bst->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return 1;
}

static int _bst_update_helper(bst_t *bst, int key, void *value, tdata_t *tdata)
{
	bst_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i, ret;
	bst_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;
	int op_is_insert = -1;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&bst->lock);
		_traverse_with_stack(bst, key, node_stack, &stack_top);
		if (op_is_insert == -1) {
			if (stack_top < 0)                          op_is_insert = 1;
			else if (node_stack[stack_top]->key == key) op_is_insert = 0;
			else                                        op_is_insert = 1;
		}

		if (op_is_insert && stack_top >= 0 && node_stack[stack_top]->key == key) {
			pthread_spin_unlock(&bst->lock);
			return 0;
		} else if (!op_is_insert && (stack_top < 0 || node_stack[stack_top]->key != key)) {
			pthread_spin_unlock(&bst->lock);
			return 2;
		}

		connection_point_stack_index = -1;
		if (op_is_insert) {
			connection_point = _insert_with_copy(key, value,
			                            node_stack, stack_top, tdata, &tree_copy_root,
			                            &connection_point_stack_index);
			ret = 1;
		} else {
			ret = _delete_with_copy(key,
			                        node_stack, stack_top, tdata, &tree_copy_root,
			                        &connection_point_stack_index, &stack_top,
			                        &connection_point);
			if (ret == 0) {
				pthread_spin_unlock(&bst->lock);
				return 2;
			}
			ret = 3;
		}

		if (!connection_point) {
			bst->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}
		pthread_spin_unlock(&bst->lock);
		return ret;
	}

	//> Asynchronized traversal.
	_traverse_with_stack(bst, key, node_stack, &stack_top);
	if (op_is_insert == -1) {
		if (stack_top < 0)                          op_is_insert = 0;
		else if (node_stack[stack_top]->key == key) op_is_insert = 0;
		else                                        op_is_insert = 1;
	}
	if (op_is_insert && stack_top >= 0 && node_stack[stack_top]->key == key)
		return 0;
	else if (!op_is_insert && (stack_top < 0 || node_stack[stack_top]->key != key))
		return 2;

	//> For now let's ignore empty tree case and case with only one node in the tree.
	connection_point_stack_index = -1;
	if (op_is_insert) {
		connection_point = _insert_with_copy(key, value,
		                             node_stack, stack_top, tdata, &tree_copy_root,
		                             &connection_point_stack_index);
		ret = 1;
	} else {
		ret = _delete_with_copy(key,
		                        node_stack, stack_top, tdata, &tree_copy_root,
		                        &connection_point_stack_index, &stack_top,
		                        &connection_point);
		if (ret == 0)
			return 2;
		ret = 3;
	}

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES)
		goto try_from_scratch;

	/* Transactional verification. */
	while (bst->lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (bst->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (op_is_insert) {
			if (key < node_stack[stack_top]->key && node_stack[stack_top]->left != NULL)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (key > node_stack[stack_top]->key && node_stack[stack_top]->right != NULL)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (bst->root != node_stack[0])
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		} else {
			if (bst->root != node_stack[0])
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (node_stack[stack_top]->left != NULL && node_stack[stack_top]->right != NULL)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (key < node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			bst_node_t *curr = bst->root;
			while (curr && curr != connection_point)
				curr = (key <= curr->key) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (key < node_stack[i]->key) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		}
	
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				bst_node_t **np = tdata->ht->entries[i][j];
				bst_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			bst->root = tree_copy_root;
		} else {
			if (key <= connection_point->key)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		TX_END(0);
	} else {
		tdata->tx_aborts++;
		if (ABORT_IS_EXPLICIT(status) && 
		    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
			tdata->tx_aborts_explicit_validation++;
			goto try_from_scratch;
		} else {
			goto validate_and_connect_copy;
		}
	}

	return ret;
}

/******************************************************************************/
/*           Map interface implementation                                     */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(bst_node_t));
	return _bst_new_helper();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(bst_node_t));
	tdata_t *tdata = tdata_new(tid);
	return tdata;
}

void map_tdata_print(void *thread_data)
{
	tdata_t *tdata = thread_data;
	tdata_print(tdata);
	return;
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	tdata_add(d1, d2, dst);
}

int map_lookup(void *map, void *thread_data, int key)
{
	int ret = 0;
	ret = _bst_lookup_helper(map, key);
	return ret; 
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *map, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = _bst_insert_helper(map, key, value, thread_data);
	return ret;
}

int map_delete(void *map, void *thread_data, int key)
{
	int ret = 0;
	ret = _bst_delete_helper(map, key, thread_data);
	return ret;
}

int map_update(void *map, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = _bst_update_helper(map, key, value, thread_data);
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
	return "bst-rcu-htm-internal";
}
