/**
 * An internal AVL tree.
 **/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "alloc.h"

#include "ht.h"
#include "htm/htm.h"

#define SYNC_RCU_HTM
#include "../../../map.h"
#include "../../../rcu-htm/tdata.h"
#include "validate.h"
#include "print.h"

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )
#define MAX_HEIGHT 50

static __thread void *nalloc;

static avl_node_t *avl_node_new(map_key_t key, void *data)
{
	avl_node_t *node = nalloc_alloc_node(nalloc);
	KEY_COPY(node->key, key);
	node->data = data;
	node->height = 0; // new nodes have height 0 and NULL has height -1.
	node->right = node->left = NULL;
	return node;
}

static void avl_node_copy(avl_node_t *dest, avl_node_t *src)
{
	KEY_COPY(dest->key, src->key);
	dest->data = src->data;
	dest->height = src->height;
	dest->left = src->left;
	dest->right = src->right;
	__sync_synchronize();
}

static avl_node_t *avl_node_new_copy(avl_node_t *src, tdata_t *tdata)
{
	avl_node_t *node = nalloc_alloc_node(nalloc);
	avl_node_copy(node, src);
	return node;
}

static inline int node_height(avl_node_t *n)
{
	if (!n)
		return -1;
	else
		return n->height;
}

static inline int node_balance(avl_node_t *n)
{
	if (!n)
		return 0;

	return node_height(n->left) - node_height(n->right);
}

static inline avl_node_t *rotate_right(avl_node_t *node)
{
	assert(node != NULL && node->left != NULL);

	avl_node_t *node_left = node->left;

	node->left = node->left->right;
	node_left->right = node;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_left->height = MAX(node_height(node_left->left), node_height(node_left->right)) + 1;
	return node_left;
}
static inline avl_node_t *rotate_left(avl_node_t *node)
{
	assert(node != NULL && node->right != NULL);

	avl_node_t *node_right = node->right;

	node->right = node->right->left;
	node_right->left = node;

	node->height = MAX(node_height(node->left), node_height(node->right)) + 1;
	node_right->height = MAX(node_height(node_right->left), node_height(node_right->right)) + 1;
	return node_right;
}

/**
 * Traverses the tree `avl` as dictated by `key`.
 * When returning, `leaf` is either NULL (key not found) or the leaf that
 * contains `key`. `parent` is either leaf's parent (if `leaf` != NULL) or
 * the node that will be the parent of the inserted node.
 * In the case of an empty tree both `parent` and `leaf` are NULL.
 **/
static inline void _traverse(avl_t *avl, map_key_t key,
                             avl_node_t **parent, avl_node_t **leaf, int *hops)
{
	*parent = NULL;
	*leaf = avl->root;
	*hops = 0;

	while (*leaf) {
		map_key_t leaf_key;
		KEY_COPY(leaf_key, (*leaf)->key);
		if (KEY_CMP(leaf_key, key) == 0)
			return;

		*parent = *leaf;
		*leaf = (KEY_CMP(key, leaf_key) < 0) ? (*leaf)->left : (*leaf)->right;
		(*hops)++;
	}
}
static inline void _traverse_with_stack(avl_t *avl, map_key_t key,
                                        avl_node_t *node_stack[MAX_HEIGHT],
                                        int *stack_top)
{
	avl_node_t *parent, *leaf;

	parent = NULL;
	leaf = avl->root;
	*stack_top = -1;

	while (leaf) {
		node_stack[++(*stack_top)] = leaf;

		map_key_t leaf_key;
		KEY_COPY(leaf_key, leaf->key);
		if (KEY_CMP(leaf_key, key) == 0)
			return;

		parent = leaf;
		leaf = (KEY_CMP(key, leaf_key) < 0) ? leaf->left : leaf->right;
	}
}

static int _avl_lookup_helper(avl_t *avl, map_key_t key, int *hops)
{
	avl_node_t *parent, *leaf;

	_traverse(avl, key, &parent, &leaf, hops);
	return (leaf != NULL);
}

static inline void _avl_insert_fixup(avl_t *avl, map_key_t key,
                                     avl_node_t *node_stack[MAX_HEIGHT],
                                     int top)
{
	avl_node_t *curr, *parent;

	while (top >= 0) {
		curr = node_stack[top--];

		parent = NULL;
		if (top >= 0) parent = node_stack[top];

		int balance = node_balance(curr);
		if (balance == 2) {
			int balance2 = node_balance(curr->left);

			if (balance2 == 1) { // LEFT-LEFT case
				if (!parent)                avl->root = rotate_right(curr);
				else if (KEY_CMP(key, parent->key) < 0) parent->left = rotate_right(curr);
				else                        parent->right = rotate_right(curr);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				curr->left = rotate_left(curr->left);
				if (!parent)                avl->root = rotate_right(curr); 
				else if (KEY_CMP(key, parent->key) < 0) parent->left = rotate_right(curr);
				else                        parent->right = rotate_right(curr);
			} else {
				assert(0);
			}

			break;
		} else if (balance == -2) {
			int balance2 = node_balance(curr->right);

			if (balance2 == -1) { // RIGHT-RIGHT case
				if (!parent)                avl->root = rotate_left(curr);
				else if (KEY_CMP(key, parent->key) < 0) parent->left = rotate_left(curr);
				else                        parent->right = rotate_left(curr);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				curr->right = rotate_right(curr->right);
				if (!parent)                avl->root = rotate_left(curr);
				else if (KEY_CMP(key, parent->key) < 0) parent->left = rotate_left(curr);
				else                        parent->right = rotate_left(curr);
			} else {
				assert(0);
			}

			break;
		}

		/* Update the height of current node. */
		int height_saved = node_height(curr);
		int height_new = MAX(node_height(curr->left), node_height(curr->right)) + 1;
		curr->height = height_new;
		if (height_saved == height_new)
			break;
	}
}

static avl_node_t *_insert_and_rebalance_with_copy(map_key_t key, void *value,
        avl_node_t *node_stack[MAX_HEIGHT], int stack_top, tdata_t *tdata,
        avl_node_t **tree_copy_root_ret, int *connection_point_stack_index,
        avl_node_t *nodes_to_free[MAX_HEIGHT], int *ntf_top,
        avl_node_t *nodes_alloced[MAX_HEIGHT], int *nalloced_top)
{
	avl_node_t *tree_copy_root, *connection_point;

	*ntf_top = -1;
	*nalloced_top = -1;

	//> Start the tree copying with the new node.
	tree_copy_root = avl_node_new(key, value);
	nodes_alloced[++(*nalloced_top)] = tree_copy_root;
	*connection_point_stack_index = stack_top;
	connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;

	//> Empty tree case
	if (stack_top < 0) {
		*tree_copy_root_ret = tree_copy_root;
		return connection_point;
	}

	while (stack_top >= -1) {
		//> If we've reached and passed root return.
		if (!connection_point)
			break;

		//> If no height change occurs we can break.
		if (tree_copy_root->height + 1 <= connection_point->height)
			break;

		//> Copy the current node and link it to the local copy.
		avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
		nodes_to_free[++(*ntf_top)] = connection_point;
		nodes_alloced[++(*nalloced_top)] = curr_cp;
		ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
		ht_insert(tdata->ht, &connection_point->right, curr_cp->right);

		curr_cp->height = tree_copy_root->height + 1;
		if (KEY_CMP(key, curr_cp->key) < 0) curr_cp->left = tree_copy_root;
		else                    curr_cp->right = tree_copy_root;
		tree_copy_root = curr_cp;

		// Move one level up
		*connection_point_stack_index = stack_top;
		connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;

		// Get current node's balance
		avl_node_t *sibling;
		int curr_balance;
		if (KEY_CMP(key, curr_cp->key) < 0) {
			sibling = curr_cp->right;
			curr_balance = node_height(curr_cp->left) - node_height(sibling);
		} else {
			sibling = curr_cp->left;
			curr_balance = node_height(sibling) - node_height(curr_cp->right);
		}

		if (curr_balance == 2) {
			int balance2 = node_balance(tree_copy_root->left);

			if (balance2 == 1) { // LEFT-LEFT case
				tree_copy_root = rotate_right(tree_copy_root);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				tree_copy_root->left = rotate_left(tree_copy_root->left);
				tree_copy_root = rotate_right(tree_copy_root);
			} else {
				assert(0);
			}

			break;
		} else if (curr_balance == -2) {
			int balance2 = node_balance(tree_copy_root->right);

			if (balance2 == -1) { // RIGHT-RIGHT case
				tree_copy_root = rotate_left(tree_copy_root);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				tree_copy_root->right = rotate_right(tree_copy_root->right);
				tree_copy_root = rotate_left(tree_copy_root);
			} else {
				assert(0);
			}

			break;
		}
	}

	*tree_copy_root_ret = tree_copy_root;
	return connection_point;
}

static int _avl_insert_helper(avl_t *avl, map_key_t key, void *value, tdata_t *tdata)
{
	avl_node_t *nodes_to_free[MAX_HEIGHT], *nodes_alloced[MAX_HEIGHT];
	int ntf_top, nalloced_top;
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i;
	avl_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->lock);
		_traverse_with_stack(avl, key, node_stack, &stack_top);
		if (stack_top >= 0 && KEY_CMP(node_stack[stack_top]->key, key) == 0) {
			pthread_spin_unlock(&avl->lock);
			return 0;
		}
		connection_point_stack_index = -1;
		connection_point = _insert_and_rebalance_with_copy(key, value,
		                           node_stack, stack_top, tdata, &tree_copy_root,
		                           &connection_point_stack_index,
		                           nodes_to_free, &ntf_top,
		                           nodes_alloced, &nalloced_top);
//		tdata->replaced_nodes += ntf_top + 1;
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (KEY_CMP(key, connection_point->key) <= 0)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}
		pthread_spin_unlock(&avl->lock);
		return 1;
	}

	/* Asynchronized traversal. If key is there we can safely return. */
	_traverse_with_stack(avl, key, node_stack, &stack_top);
	if (stack_top >= 0 && KEY_CMP(node_stack[stack_top]->key, key) == 0) {
		return 0;
	}

	// For now let's ignore empty tree case and case with only one node in the tree.
//	assert(stack_top >= 2);

	connection_point_stack_index = -1;
	connection_point = _insert_and_rebalance_with_copy(key, value,
	                             node_stack, stack_top, tdata, &tree_copy_root,
	                             &connection_point_stack_index,
	                             nodes_to_free, &ntf_top,
	                             nodes_alloced, &nalloced_top);
//	tdata->replaced_nodes += ntf_top + 1;

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) {
		goto try_from_scratch;
	}

	/* Transactional verification. */
	while (avl->lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (KEY_CMP(key, node_stack[stack_top]->key) < 0 && node_stack[stack_top]->left != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (KEY_CMP(key, node_stack[stack_top]->key) > 0 && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (avl->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (KEY_CMP(key, node_stack[i]->key) <= 0) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			avl_node_t *curr = avl->root;
			while (curr && curr != connection_point)
				curr = (KEY_CMP(key, curr->key) <= 0) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (KEY_CMP(key, node_stack[i]->key) <= 0) {
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
				avl_node_t **np = tdata->ht->entries[i][j];
				avl_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}


		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (KEY_CMP(key, connection_point->key) <= 0)
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

static inline void _find_successor_with_stack(avl_node_t *node,
                                              avl_node_t *node_stack[MAX_HEIGHT],
                                              int *stack_top, tdata_t *tdata)
{
	avl_node_t *parent, *leaf, *l, *r;

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
		parent = leaf;
		leaf = l;
		node_stack[++(*stack_top)] = leaf;
	}
	ht_insert(tdata->ht, &leaf->left, NULL);
}

static avl_node_t *_delete_and_rebalance_with_copy(map_key_t key,
                       avl_node_t *node_stack[MAX_HEIGHT], int stack_top,
                       tdata_t *tdata, avl_node_t **tree_copy_root_ret,
                       int *connection_point_stack_index, int *new_stack_top,
                       avl_node_t *nodes_to_free[MAX_HEIGHT], int *ntf_top,
                       avl_node_t *nodes_alloced[MAX_HEIGHT], int *nalloced_top)
{
	avl_node_t *tree_copy_root, *connection_point;
	int to_be_deleted_stack_index = stack_top;
	avl_node_t *original_to_be_deleted = node_stack[stack_top];
	avl_node_t *l, *r;

	*ntf_top = -1;
	*nalloced_top = -1;

	// If needed move on to the node to be deleted.
	_find_successor_with_stack(original_to_be_deleted, node_stack, &stack_top, tdata);
	*new_stack_top = stack_top;

	avl_node_t *to_be_deleted = node_stack[stack_top];
	nodes_to_free[++(*ntf_top)] = to_be_deleted;
	l = to_be_deleted->left; r = to_be_deleted->right;
	ht_insert(tdata->ht, &to_be_deleted->left, l);
	ht_insert(tdata->ht, &to_be_deleted->right, r);
	tree_copy_root = (l != NULL) ? l : r;
	stack_top--;
	*connection_point_stack_index = stack_top;
	connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;

	while (stack_top >= -1) {
		// If we've reached and passed root return.
		if (!connection_point)
			break;

		avl_node_t *sibling;
		int curr_balance;
		if (KEY_CMP(key, connection_point->key) < 0) {
			sibling = connection_point->right;
			ht_insert(tdata->ht, &connection_point->right, sibling);
			curr_balance = node_height(tree_copy_root) - node_height(sibling);
		} else {
			sibling = connection_point->left;
			ht_insert(tdata->ht, &connection_point->left, sibling);
			curr_balance = node_height(sibling) - node_height(tree_copy_root);
		}

		// Check if rotation(s) is(are) necessary.
		if (curr_balance == 2) {
			avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
			nodes_to_free[++(*ntf_top)] = connection_point;
			nodes_alloced[++(*nalloced_top)] = curr_cp;
			ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
			ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
			curr_cp->left = sibling;

			if (KEY_CMP(key, curr_cp->key) < 0) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
			if (connection_point == original_to_be_deleted)
				KEY_COPY(tree_copy_root->key, to_be_deleted->key);
			curr_cp = avl_node_new_copy(tree_copy_root->left, tdata);
			nodes_to_free[++(*ntf_top)] = tree_copy_root->left;
			nodes_alloced[++(*nalloced_top)] = curr_cp;
			ht_insert(tdata->ht, &tree_copy_root->left->left, curr_cp->left);
			ht_insert(tdata->ht, &tree_copy_root->left->right, curr_cp->right);
			tree_copy_root->left = curr_cp;

			int balance2 = node_balance(tree_copy_root->left);

			if (balance2 == 0 || balance2 == 1) { // LEFT-LEFT case
				tree_copy_root = rotate_right(tree_copy_root);
			} else if (balance2 == -1) { // LEFT-RIGHT case
				curr_cp = avl_node_new_copy(tree_copy_root->left->right, tdata);
				nodes_to_free[++(*ntf_top)] = tree_copy_root->left->right;
				nodes_alloced[++(*nalloced_top)] = curr_cp;
				ht_insert(tdata->ht, &tree_copy_root->left->right->left, curr_cp->left);
				ht_insert(tdata->ht, &tree_copy_root->left->right->right, curr_cp->right);
				tree_copy_root->left->right = curr_cp;

				tree_copy_root->left = rotate_left(tree_copy_root->left);
				tree_copy_root = rotate_right(tree_copy_root);
			} else {
				assert(0);
			}

			// Move one level up
			*connection_point_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
			continue;
		} else if (curr_balance == -2) {
			avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
			nodes_to_free[++(*ntf_top)] = connection_point;
			nodes_alloced[++(*nalloced_top)] = curr_cp;
			ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
			ht_insert(tdata->ht, &connection_point->right, curr_cp->right);
			curr_cp->right = sibling;

			if (KEY_CMP(key, curr_cp->key) < 0) curr_cp->left = tree_copy_root;
			else                    curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
			if (connection_point == original_to_be_deleted)
				KEY_COPY(tree_copy_root->key, to_be_deleted->key);
			curr_cp = avl_node_new_copy(tree_copy_root->right, tdata);
			nodes_to_free[++(*ntf_top)] = tree_copy_root->right;
			nodes_alloced[++(*nalloced_top)] = curr_cp;
			ht_insert(tdata->ht, &tree_copy_root->right->left, curr_cp->left);
			ht_insert(tdata->ht, &tree_copy_root->right->right, curr_cp->right);
			tree_copy_root->right = curr_cp;

			int balance2 = node_balance(tree_copy_root->right);

			if (balance2 == 0 || balance2 == -1) { // RIGHT-RIGHT case
				tree_copy_root = rotate_left(tree_copy_root);
			} else if (balance2 == 1) { // RIGHT-LEFT case
				curr_cp = avl_node_new_copy(tree_copy_root->right->left, tdata);
				nodes_to_free[++(*ntf_top)] = tree_copy_root->right->left;
				nodes_alloced[++(*nalloced_top)] = curr_cp;
				ht_insert(tdata->ht, &tree_copy_root->right->left->left, curr_cp->left);
				ht_insert(tdata->ht, &tree_copy_root->right->left->right, curr_cp->right);
				tree_copy_root->right->left = curr_cp;
				tree_copy_root->right = rotate_right(tree_copy_root->right);
				tree_copy_root = rotate_left(tree_copy_root);
			} else {
				assert(0);
			}

			// Move one level up
			*connection_point_stack_index = stack_top;
			connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;
			continue;
		}

		// Check whether current node's height is to change.
		int old_height = connection_point->height;
		int new_height = MAX(node_height(tree_copy_root), node_height(sibling)) + 1;
		if (old_height == new_height)
			break;

		// Copy the current node and link it to the local copy.
		avl_node_t *curr_cp = avl_node_new_copy(connection_point, tdata);
		nodes_to_free[++(*ntf_top)] = connection_point;
		nodes_alloced[++(*nalloced_top)] = curr_cp;
		if (KEY_CMP(key, curr_cp->key) < 0) curr_cp->right = sibling;
		else                                curr_cp->left = sibling;

		ht_insert(tdata->ht, &connection_point->left, curr_cp->left);
		ht_insert(tdata->ht, &connection_point->right, curr_cp->right);

		// Change the height of current node's copy + the key if needed.
		curr_cp->height = new_height;
		if (KEY_CMP(key, curr_cp->key) < 0) curr_cp->left = tree_copy_root;
		else                                curr_cp->right = tree_copy_root;
		tree_copy_root = curr_cp;
		if (connection_point == original_to_be_deleted)
			KEY_COPY(tree_copy_root->key, to_be_deleted->key);

		// Move one level up
		*connection_point_stack_index = stack_top;
		connection_point = stack_top >= 0 ? node_stack[stack_top--] : NULL;

	}

	// We may need to copy the access path from the originally deleted node
	// up to the current connection_point.
	if (to_be_deleted_stack_index <= *connection_point_stack_index) {
		int i;
		for (i=*connection_point_stack_index; i >= to_be_deleted_stack_index; i--) {
			avl_node_t *curr_cp = avl_node_new_copy(node_stack[i], tdata);
			nodes_to_free[++(*ntf_top)] = node_stack[i];
			nodes_alloced[++(*nalloced_top)] = curr_cp;
			ht_insert(tdata->ht, &node_stack[i]->left, curr_cp->left);
			ht_insert(tdata->ht, &node_stack[i]->right, curr_cp->right);

			if (KEY_CMP(key, curr_cp->key) < 0) curr_cp->left = tree_copy_root;
			else                                curr_cp->right = tree_copy_root;
			tree_copy_root = curr_cp;
		}
		KEY_COPY(tree_copy_root->key, to_be_deleted->key);
		connection_point = to_be_deleted_stack_index > 0 ? 
		                             node_stack[to_be_deleted_stack_index - 1] :
		                             NULL;
		*connection_point_stack_index = to_be_deleted_stack_index - 1;
	}

	*tree_copy_root_ret = tree_copy_root;
	return connection_point;
}

static int _avl_delete_helper(avl_t *avl, map_key_t key, tdata_t *tdata)
{
	avl_node_t *nodes_to_free[MAX_HEIGHT], *nodes_alloced[MAX_HEIGHT];
	int ntf_top, nalloced_top;
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i;
	avl_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);

	/* Global lock fallback.*/
	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->lock);
		_traverse_with_stack(avl, key, node_stack, &stack_top);
		if (stack_top < 0 || KEY_CMP(node_stack[stack_top]->key, key) != 0) {
			pthread_spin_unlock(&avl->lock);
			return 0;
		}
		connection_point_stack_index = -1;
		connection_point = _delete_and_rebalance_with_copy(key,
		                           node_stack, stack_top, tdata,
		                           &tree_copy_root, &connection_point_stack_index, &stack_top,
		                           nodes_to_free, &ntf_top,
		                           nodes_alloced, &nalloced_top);
//		tdata->replaced_nodes += ntf_top + 1;
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (KEY_CMP(key, connection_point->key) <= 0)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}

		pthread_spin_unlock(&avl->lock);
		return 1;
	}

	/* Asynchronized traversal. If key is not there we can safely return. */
	_traverse_with_stack(avl, key, node_stack, &stack_top);
	if (stack_top < 0 || KEY_CMP(node_stack[stack_top]->key, key) != 0) {
		return 0;
	}

	connection_point_stack_index = -1;
	connection_point = _delete_and_rebalance_with_copy(key,
	                             node_stack, stack_top, tdata,
	                             &tree_copy_root, &connection_point_stack_index, &stack_top,
	                             nodes_to_free, &ntf_top,
	                             nodes_alloced, &nalloced_top);
//	tdata->replaced_nodes += ntf_top + 1;

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) {
		goto try_from_scratch;
	}

	/* Transactional verification. */
	while (avl->lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (node_stack[stack_top]->left != NULL && node_stack[stack_top]->right != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (avl->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);

		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if (KEY_CMP(key, node_stack[i]->key) < 0) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			avl_node_t *curr = avl->root;
			while (curr && curr != connection_point)
				curr = (KEY_CMP(key, curr->key) <= 0) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if (KEY_CMP(key, node_stack[i]->key) < 0) {
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
				avl_node_t **np = tdata->ht->entries[i][j];
				avl_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (KEY_CMP(key, connection_point->key) <= 0)
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

static int _avl_update_helper(avl_t *avl, map_key_t key, void *value, tdata_t *tdata)
{
	avl_node_t *nodes_to_free[MAX_HEIGHT], *nodes_alloced[MAX_HEIGHT];
	int ntf_top, nalloced_top;
	avl_node_t *node_stack[MAX_HEIGHT];
	int stack_top;
	tm_begin_ret_t status;
	int retries = -1;
	int i;
	avl_node_t *tree_copy_root, *connection_point;
	int connection_point_stack_index;
	int op_is_insert = -1, ret;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&avl->lock);
		_traverse_with_stack(avl, key, node_stack, &stack_top);
		if (op_is_insert == -1) {
			if (stack_top < 0)                                      op_is_insert = 1;
			else if (KEY_CMP(node_stack[stack_top]->key, key) == 0) op_is_insert = 0;
			else                                                    op_is_insert = 1;
		}

		if (op_is_insert && stack_top >= 0 && KEY_CMP(node_stack[stack_top]->key, key) == 0) {
			pthread_spin_unlock(&avl->lock);
			return 0;
		} else if (!op_is_insert && (stack_top < 0 || KEY_CMP(node_stack[stack_top]->key, key) != 0)) {
			pthread_spin_unlock(&avl->lock);
			return 2;
		}

		connection_point_stack_index = -1;
		if (op_is_insert) {
			connection_point = _insert_and_rebalance_with_copy(key, value,
			                           node_stack, stack_top, tdata, &tree_copy_root,
			                           &connection_point_stack_index,
			                           nodes_to_free, &ntf_top,
			                           nodes_alloced, &nalloced_top);
			ret = 1;
		} else {
			connection_point = _delete_and_rebalance_with_copy(key,
			                           node_stack, stack_top, tdata,
			                           &tree_copy_root, &connection_point_stack_index,
			                           &stack_top, nodes_to_free, &ntf_top,
			                           nodes_alloced, &nalloced_top);
			ret = 3;
		}
//		tdata->replaced_nodes += ntf_top + 1;
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (KEY_CMP(key, connection_point->key) <= 0)
				connection_point->left = tree_copy_root;
			else
				connection_point->right = tree_copy_root;
		}
		pthread_spin_unlock(&avl->lock);
		return ret;
	}

	/* Asynchronized traversal. If key is there we can safely return. */
	_traverse_with_stack(avl, key, node_stack, &stack_top);
	if (op_is_insert == -1) {
		if (stack_top < 0)                                      op_is_insert = 0;
		else if (KEY_CMP(node_stack[stack_top]->key, key) == 0) op_is_insert = 0;
		else                                                    op_is_insert = 1;
	}
	if (op_is_insert && stack_top >= 0 && KEY_CMP(node_stack[stack_top]->key, key) == 0) {
		return 0;
	} else if (!op_is_insert && (stack_top < 0 || KEY_CMP(node_stack[stack_top]->key, key) != 0)) {
		return 2;
	}

	if (op_is_insert) {
		// For now let's ignore empty tree case and case with only one node in the tree.
		assert(stack_top >= 2);
	}

	connection_point_stack_index = -1;
	if (op_is_insert) {
		connection_point = _insert_and_rebalance_with_copy(key, value,
		                             node_stack, stack_top, tdata, &tree_copy_root,
		                             &connection_point_stack_index,
		                             nodes_to_free, &ntf_top,
		                             nodes_alloced, &nalloced_top);
		ret = 1;
	} else {
		connection_point = _delete_and_rebalance_with_copy(key,
		                           node_stack, stack_top, tdata,
		                           &tree_copy_root, &connection_point_stack_index,
		                           &stack_top, nodes_to_free, &ntf_top,
		                           nodes_alloced, &nalloced_top);
		ret = 3;
	}
//	tdata->replaced_nodes += ntf_top + 1;

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) {
		goto try_from_scratch;
	}

	/* Transactional verification. */
	while (avl->lock != LOCK_FREE)
		;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (avl->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		// Validate copy
		if (op_is_insert) {
			if (KEY_CMP(key, node_stack[stack_top]->key) < 0 && node_stack[stack_top]->left != NULL)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			if (KEY_CMP(key, node_stack[stack_top]->key) > 0 && node_stack[stack_top]->right != NULL)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		} else {
			if (node_stack[stack_top]->left != NULL && node_stack[stack_top]->right != NULL)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
		if (avl->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
	
		if (connection_point_stack_index <= 0) {
			for (i=0; i < stack_top; i++) {
				if ((op_is_insert && KEY_CMP(key, node_stack[i]->key) <= 0) ||
				    (!op_is_insert && KEY_CMP(key, node_stack[i]->key) < 0)) {
					if (node_stack[i]->left != node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				} else {
					if (node_stack[i]->right!= node_stack[i+1])
						TX_ABORT(ABORT_VALIDATION_FAILURE);
				}
			}
		} else {
			avl_node_t *curr = avl->root;
			while (curr && curr != connection_point)
				curr = (KEY_CMP(key, curr->key) <= 0) ? curr->left : curr->right;
			if (curr != connection_point)
				TX_ABORT(ABORT_VALIDATION_FAILURE);
			for (i=connection_point_stack_index; i < stack_top; i++) {
				if ((op_is_insert && KEY_CMP(key, node_stack[i]->key) <= 0) ||
				    (!op_is_insert && KEY_CMP(key, node_stack[i]->key) < 0)) {
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
				avl_node_t **np = tdata->ht->entries[i][j];
				avl_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (!connection_point) {
			avl->root = tree_copy_root;
		} else {
			if (KEY_CMP(key, connection_point->key) <= 0)
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
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(avl_node_t));
	return avl_new();
}

void *map_tdata_new(int tid)
{
	tdata_t *tdata = tdata_new(tid);
	nalloc = nalloc_thread_init(tid, sizeof(avl_node_t));
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

int map_lookup(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	int hops = 0;
	ret = _avl_lookup_helper(map, key, &hops);
	return ret; 
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *map, void *tdata, map_key_t key, void *value)
{
	int ret = 0;
	ret = _avl_insert_helper(map, key, value, tdata);
	return ret;
}

int map_delete(void *map, void *tdata, map_key_t key)
{
	int ret = 0;
	ret = _avl_delete_helper(map, key, tdata);
	return ret;
}

int map_update(void *map, void *tdata, map_key_t key, void *value)
{
	int ret = 0;
	ret = _avl_update_helper(map, key, value, tdata);
	return ret;
}

int map_validate(void *map)
{
	int ret = 0;
	ret = _avl_validate_helper(((avl_t *)map)->root);
	return ret;
}

void map_print(void *map)
{
	avl_print(map);
}

char *map_name()
{
	return "avl-rcu-htm-internal";
}
