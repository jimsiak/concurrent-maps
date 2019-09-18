#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h> //> For INT_MAX
#include <pthread.h>

#include "alloc.h"
#include "ht.h"
#include "../../map.h"
#include "../../key/key.h"
#include "htm/htm.h"
#define SYNC_RCU_HTM
#include "btree.h"
#include "validate.h"
#include "print.h"

/******************************************************************************/
//> Per thread data
/******************************************************************************/
typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts, 
	                   tx_aborts_explicit_validation, lacqs;
	ht_t *ht;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->tx_aborts_explicit_validation = 0;
	ret->lacqs = 0;
	ret->ht = ht_new();
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	      tdata->tx_aborts, tdata->tx_aborts_explicit_validation, tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_explicit_validation = d1->tx_aborts_explicit_validation +
	                                     d2->tx_aborts_explicit_validation;
	dst->lacqs = d1->lacqs + d2->lacqs;
}

/******************************************************************************/
/******************************************************************************/
/******************************************************************************/

int btree_lookup(btree_t *btree, map_key_t key)
{
	int index;
	btree_node_t *n = btree->root;

	//> Empty tree.
	if (!n) return 0;

	while (!n->leaf) {
		index = btree_node_search(n, key);
		n = n->children[index];
	}
	index = btree_node_search(n, key);
	return (KEY_CMP(n->keys[index], key) == 0);
}

int btree_rquery(void *map, map_key_t key1, map_key_t key2)
{
	return 0;
}

void btree_traverse_stack(btree_t *btree, map_key_t key,
                          btree_node_t **node_stack, int *node_stack_indexes,
                          int *node_stack_top)
{
	int index;
	btree_node_t *n;

	*node_stack_top = -1;
	n = btree->root;
	if (!n) return;

	while (!n->leaf) {
		index = btree_node_search(n, key);
		node_stack[++(*node_stack_top)] = n;
		node_stack_indexes[*node_stack_top] = index;
		n = n->children[index];
	}
	index = btree_node_search(n, key);
	node_stack[++(*node_stack_top)] = n;
	node_stack_indexes[*node_stack_top] = index;
}

btree_node_t *find_left_sibling(btree_node_t **node_stack,
                                int *node_stack_indexes,
                                int stack_top, tdata_t *tdata)
{
	btree_node_t *cur, *next;
	int cur_index;

	if (stack_top == 0) return NULL;
	
	stack_top--;
	while (stack_top >= 0 && node_stack_indexes[stack_top] == 0)
		stack_top--;

	if (stack_top < 0) return NULL;

	//> Start from the first left sibling and walk down the tree
	cur = node_stack[stack_top];
	cur_index = node_stack_indexes[stack_top];
	next = cur->children[cur_index - 1];
	ht_insert(tdata->ht, &cur->children[cur_index - 1], next);
	cur = next;
	while (!cur->leaf) {
		next = cur->children[cur->no_keys];
		ht_insert(tdata->ht, &cur->children[cur->no_keys], next);
		cur = next;
	}
	return cur;
}

/**
 * Distributes the keys of 'n' on the two nodes and also adds 'key'.
 * The distribution is done so as 'n' contains BTREE_ORDER + 1 keys
 * and 'rnode' contains BTREE_ORDER keys.
 * CAUTION: rnode->children[0] is left NULL.
 **/
void distribute_keys(btree_node_t *n, btree_node_t *rnode, map_key_t key,
                     void *ptr, int index)
{
	int i, mid;

	mid = BTREE_ORDER;
	if (index > BTREE_ORDER) mid++;

	//> Move half of the keys on the new node.
	for (i = mid; i < 2 * BTREE_ORDER; i++) {
		KEY_COPY(rnode->keys[i - mid], n->keys[i]);
		rnode->children[i - mid] = n->children[i];
	}
	rnode->children[i - mid] = n->children[i];

	n->no_keys = mid;
	rnode->no_keys = 2 * BTREE_ORDER - mid;

	//> Insert the new key in the appropriate node.
	if (index > BTREE_ORDER) btree_node_insert_index(rnode, index - mid, key, ptr);
	else btree_node_insert_index(n, index, key, ptr);
}

btree_node_t *btree_node_split(btree_node_t *n, map_key_t key, void *ptr, int index,
                               map_key_t *key_ret)
{
	btree_node_t *rnode = btree_node_new(n->leaf);
	distribute_keys(n, rnode, key, ptr, index);

	//> This is the key that will be propagated upwards.
	KEY_COPY(*key_ret, n->keys[BTREE_ORDER]);

	if (!n->leaf) {
		rnode->children[0] = n->children[n->no_keys];
		n->no_keys--;
	}

	if (n->leaf) {
		rnode->sibling = n->sibling;
		n->sibling = rnode;
	}

	return rnode;
}

/**
 * Inserts 'key' in the tree and returns the connection point.
 **/
btree_node_t *btree_insert_with_copy(map_key_t key, void *val,
                             btree_node_t **node_stack, int *node_stack_indexes,
                             int stack_top,
                             btree_node_t **tree_cp_root,
                             int *connection_point_stack_index,
                             btree_node_t **to_modify_sibling, 
                             btree_node_t **new_sibling, tdata_t *tdata)
{
	btree_node_t *cur = NULL, *cur_cp = NULL, *cur_cp_prev;
	btree_node_t *conn_point;
	int index, i;
	map_key_t key_to_add = key;
	void *ptr_to_add = val;

	while (1) {
		//> We surpassed the root. New root needs to be created.
		if (stack_top < 0) {
			btree_node_t *new = btree_node_new(cur == NULL ? 1 : 0);
			btree_node_insert_index(new, 0, key_to_add, ptr_to_add);
			new->children[0] = cur_cp;
			*tree_cp_root = new;
			break;
		}

		cur = node_stack[stack_top];
		index = node_stack_indexes[stack_top];

		//> Copy current node
		cur_cp_prev = cur_cp;
		cur_cp = btree_node_new_copy(cur);
		for (i=0; i <= cur_cp->no_keys; i++)
			ht_insert(tdata->ht, &cur->children[i], cur_cp->children[i]);
		ht_insert(tdata->ht, &cur->sibling, cur_cp->sibling);

		//> If leaf, keep the new sibling to be installed later
		if (cur_cp->leaf) {
			*to_modify_sibling = find_left_sibling(node_stack, node_stack_indexes,
			                                       stack_top, tdata);
			*new_sibling = cur_cp;
		}

		//> Connect copied node with the rest of the copied tree.
		if (cur_cp_prev) cur_cp->children[index] = cur_cp_prev;

		//> No split required.
		if (cur_cp->no_keys < 2 * BTREE_ORDER) {
			btree_node_insert_index(cur_cp, index, key_to_add, ptr_to_add);
			*tree_cp_root = cur_cp;
			break;
		}

		ptr_to_add = btree_node_split(cur_cp, key_to_add, ptr_to_add, index,
		                              &key_to_add);

		stack_top--;
	}

	*connection_point_stack_index = stack_top - 1;
	conn_point = stack_top <= 0 ? NULL : node_stack[stack_top-1];
	return conn_point;
}

int btree_insert(btree_t *btree, map_key_t key, void *val, tdata_t *tdata)
{
	tm_begin_ret_t status;
	btree_node_t *node_stack[20];
	btree_node_t *connection_point, *tree_cp_root;
	btree_node_t *to_modify_sibling, *new_sibling;
	int node_stack_indexes[20], stack_top, index;
	int retries = -1;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);
	to_modify_sibling = new_sibling = NULL;

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&btree->lock);
		btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
		if (stack_top >= 0 && node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
		        KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0) {
			pthread_spin_unlock(&btree->lock);
			return 0;
		}
		connection_point = btree_insert_with_copy(key, val, 
		                                  node_stack, node_stack_indexes, stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index,
		                                  &to_modify_sibling, &new_sibling, tdata);
		if (connection_point == NULL) {
			btree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
		}
		if (to_modify_sibling != NULL) to_modify_sibling->sibling = new_sibling;
		pthread_spin_unlock(&btree->lock);
		return 1;
	}

	//> Asynchronized traversal. If key is there we can safely return.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
	if (stack_top >= 0 && node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
	        KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0)
		return 0;

	connection_point = btree_insert_with_copy(key, val, 
	                                  node_stack, node_stack_indexes, stack_top,
	                                  &tree_cp_root,
	                                  &connection_point_stack_index,
	                                  &to_modify_sibling, &new_sibling, tdata);

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) goto try_from_scratch;
	while (btree->lock != LOCK_FREE) ;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (btree->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		//> Validate copy
		if (stack_top < 0 && btree->root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && btree->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		int i;
		btree_node_t *n1, *n2;
		for (i=0; i < stack_top; i++) {
			n1 = node_stack[i];
			index = node_stack_indexes[i];
			n2 = n1->children[index];
			if (n2 != node_stack[i+1])
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				btree_node_t **np = tdata->ht->entries[i][j];
				btree_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (connection_point == NULL) {
			btree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
		}
		if (to_modify_sibling != NULL) to_modify_sibling->sibling = new_sibling;
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

/**
 * c = current
 * p = parent
 * pindex = parent_index
 * Returns: pointer to the node that remains.
 **/
btree_node_t *btree_merge_with_copy(btree_node_t *c, btree_node_t *p, int pindex,
                                    int *merged_with_left_sibling, tdata_t *tdata,
                                    btree_node_t *sibling_left,
                                    btree_node_t *sibling_right)
{
	int i, sibling_index;
	btree_node_t *sibling, *sibling_cp;

	//> Left sibling first.
	if (pindex > 0) {
		sibling = sibling_left;
		sibling_cp = btree_node_new_copy(sibling);
		for (i=0; i <= sibling_cp->no_keys; i++)
			ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
		ht_insert(tdata->ht, &sibling->sibling, sibling_cp->sibling);

		sibling_index = sibling_cp->no_keys;

		if (!c->leaf) {
			KEY_COPY(sibling_cp->keys[sibling_index], p->keys[pindex - 1]);
			sibling_cp->children[sibling_index+1] = c->children[0];
			sibling_index++;
		}
		for (i=0; i < c->no_keys; i++) {
			KEY_COPY(sibling_cp->keys[sibling_index], c->keys[i]);
			sibling_cp->children[sibling_index + 1] = c->children[i + 1];
			sibling_index++;
		}

		sibling_cp->no_keys = sibling_index;
		*merged_with_left_sibling = 1;
		sibling_cp->sibling = c->sibling;
		return sibling_cp;
	}

	//> Right sibling next
	if (pindex < p->no_keys) {
		sibling = sibling_right;
		sibling_cp = btree_node_new_copy(sibling);
		ht_insert(tdata->ht, &p->children[pindex+1], sibling);
		for (i=0; i <= sibling_cp->no_keys; i++)
			ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
		ht_insert(tdata->ht, &sibling->sibling, sibling_cp->sibling);

		sibling_index = c->no_keys;

		if (!c->leaf) {
			KEY_COPY(c->keys[sibling_index], p->keys[pindex]);
			c->children[sibling_index+1] = sibling_cp->children[0];
			sibling_index++;
		}
		for (i=0; i < sibling_cp->no_keys; i++) {
			KEY_COPY(c->keys[sibling_index], sibling_cp->keys[i]);
			c->children[sibling_index + 1] = sibling_cp->children[i + 1];
			sibling_index++;
		}

		c->no_keys = sibling_index;
		*merged_with_left_sibling = 0;
		c->sibling = sibling_cp->sibling;
		return c;
	}

	//> Unreachable code.
	assert(0);
	return NULL;
}

/**
 * c = current
 * p = parent
 * pindex = parent_index
 * Returns: parent_cp if borrowing was successful, NULL otherwise.
 **/
btree_node_t *btree_borrow_keys_with_copies(btree_node_t *c, btree_node_t *p, int pindex,
                                            tdata_t *tdata,
                                            btree_node_t **sibling_left,
                                            btree_node_t **sibling_right,
                                            int *borrowed_from_left_sibling)
{
	int i;
	btree_node_t *sibling, *sibling_cp, *parent_cp;

	*borrowed_from_left_sibling = 0;

	//> Left sibling first.
	if (pindex > 0) {
		sibling = p->children[pindex - 1];
		*sibling_left = sibling;
		ht_insert(tdata->ht, &p->children[pindex-1], sibling);
		if (sibling->no_keys > BTREE_ORDER) {
			sibling_cp = btree_node_new_copy(sibling);
			parent_cp = btree_node_new_copy(p);
			for (i=0; i <= sibling_cp->no_keys; i++)
				ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
			for (i=0; i <= parent_cp->no_keys; i++)
				ht_insert(tdata->ht, &p->children[i], parent_cp->children[i]);
			ht_insert(tdata->ht, &sibling->sibling, sibling_cp->sibling);
			ht_insert(tdata->ht, &p->sibling, parent_cp->sibling);

			parent_cp->children[pindex - 1] = sibling_cp;
			parent_cp->children[pindex] = c;

			for (i = c->no_keys-1; i >= 0; i--) c->keys[i+1] = c->keys[i];
			for (i = c->no_keys; i >= 0; i--) c->children[i+1] = c->children[i];
			if (!c->leaf) {
				KEY_COPY(c->keys[0], parent_cp->keys[pindex-1]);
				c->children[0] = sibling_cp->children[sibling_cp->no_keys];
				parent_cp->keys[pindex-1] = sibling_cp->keys[sibling_cp->no_keys-1];
			} else {
				KEY_COPY(c->keys[0], sibling_cp->keys[sibling_cp->no_keys-1]);
				c->children[1] = sibling_cp->children[sibling_cp->no_keys];
				parent_cp->keys[pindex-1] = sibling_cp->keys[sibling_cp->no_keys-2];
			}
			sibling_cp->no_keys--;
			c->no_keys++;
			sibling_cp->sibling = c;
			*borrowed_from_left_sibling = 1;
			return parent_cp;
		}
	}

	//> Right sibling next.
	if (pindex < p->no_keys) {
		sibling = p->children[pindex + 1];
		*sibling_right = sibling;
		ht_insert(tdata->ht, &p->children[pindex+1], sibling);
		if (sibling->no_keys > BTREE_ORDER) {
			sibling_cp = btree_node_new_copy(sibling);
			parent_cp = btree_node_new_copy(p);
			for (i=0; i <= sibling_cp->no_keys; i++)
				ht_insert(tdata->ht, &sibling->children[i], sibling_cp->children[i]);
			for (i=0; i <= parent_cp->no_keys; i++)
				ht_insert(tdata->ht, &p->children[i], parent_cp->children[i]);
			ht_insert(tdata->ht, &sibling->sibling, sibling_cp->sibling);
			ht_insert(tdata->ht, &p->sibling, parent_cp->sibling);

			parent_cp->children[pindex] = c;
			parent_cp->children[pindex+1] = sibling_cp;

			if (!c->leaf) {
				KEY_COPY(c->keys[c->no_keys], parent_cp->keys[pindex]);
				c->children[c->no_keys+1] = sibling_cp->children[0];
				parent_cp->keys[pindex] = sibling_cp->keys[0];
			} else {
				KEY_COPY(c->keys[c->no_keys], sibling_cp->keys[0]);
				c->children[c->no_keys+1] = sibling_cp->children[1];
				parent_cp->keys[pindex] = c->keys[c->no_keys];
			}
			for (i=0; i < sibling_cp->no_keys-1; i++)
				KEY_COPY(sibling_cp->keys[i], sibling_cp->keys[i+1]);
			for (i=0; i < sibling_cp->no_keys; i++)
				sibling_cp->children[i] = sibling_cp->children[i+1];
			sibling_cp->no_keys--;
			c->no_keys++;
			c->sibling = sibling_cp;
			return parent_cp;
		}
	}

	//> Could not borrow for either of the two siblings.
	return NULL;
}

btree_node_t *btree_delete_with_copy(map_key_t key,
                             btree_node_t **node_stack, int *node_stack_indexes,
                             int stack_top,
                             btree_node_t **tree_cp_root,
                             int *connection_point_stack_index,
                             btree_node_t **to_modify_sibling,
                             btree_node_t **new_sibling, tdata_t *tdata)
{
	btree_node_t *parent;
	btree_node_t *cur = NULL, *cur_cp = NULL, *cur_cp_prev;
	btree_node_t *conn_point, *new_parent;
	int index, i;
	int parent_index;
	int merged_with_left_sibling = 0, borrowed_from_left_sibling = 0;

	*tree_cp_root = NULL;

	while (1) {
		cur = node_stack[stack_top];

		//> We reached root which contains only one key.
		if (stack_top == 0 && cur->no_keys == 1) break;

		index = node_stack_indexes[stack_top];
		if (merged_with_left_sibling) index--;

		//> Copy current node
		cur_cp = btree_node_new_copy(cur);
		for (i=0; i <= cur_cp->no_keys; i++)
			ht_insert(tdata->ht, &cur->children[i], cur_cp->children[i]);
		ht_insert(tdata->ht, &cur->sibling, cur_cp->sibling);

		//> Get the sibling pointer to be modified
		if (cur_cp->leaf) {
			*to_modify_sibling = find_left_sibling(node_stack, node_stack_indexes,
			                                       stack_top, tdata);
			*new_sibling = cur_cp;
		}

		//> Connect copied node with the rest of the copied tree.
		if (*tree_cp_root) cur_cp->children[index] = *tree_cp_root;
		*tree_cp_root = cur_cp;

		//> Delete the key from the current node.
		btree_node_delete_index(cur_cp, index);

		//> Root can be less than half-full.
		if (stack_top == 0) break;

		//> If current node is at least half-full, we are done.
		if (cur_cp->no_keys >= BTREE_ORDER) break;

		//> First try to borrow keys from siblings
		btree_node_t *sibling_left, *sibling_right;
		parent = node_stack[stack_top-1];
		parent_index = node_stack_indexes[stack_top-1];
		new_parent = btree_borrow_keys_with_copies(cur_cp, parent, parent_index, tdata,
		                                           &sibling_left, &sibling_right,
		                                           &borrowed_from_left_sibling);

		//> Update the sibling pointer to be modified
		if (borrowed_from_left_sibling && cur_cp->leaf) {
			node_stack_indexes[stack_top-1]--; // We want left sibling's sibling :-)
			*to_modify_sibling = find_left_sibling(node_stack, node_stack_indexes,
			                                       stack_top, tdata);
			node_stack_indexes[stack_top-1]++; // Fix it in case it is used elsewhere
			*new_sibling = new_parent->children[node_stack_indexes[stack_top-1]-1];
		}
		if (new_parent != NULL) {
			*tree_cp_root = new_parent;
			stack_top--;
			break;
		}

		//> If everything has failed, merge nodes
		*tree_cp_root = btree_merge_with_copy(cur_cp, parent, parent_index,
		                                      &merged_with_left_sibling, tdata,
		                                      sibling_left, sibling_right);
		if (merged_with_left_sibling && cur_cp->leaf) {
			node_stack_indexes[stack_top-1]--; // We want left sibling's sibling :-)
			*to_modify_sibling = find_left_sibling(node_stack, node_stack_indexes,
			                                       stack_top, tdata);
			node_stack_indexes[stack_top-1]++; // Fix it in case it is used elsewhere
			*new_sibling = *tree_cp_root;
		}

		//> Move one level up
		stack_top--;
	}

	*connection_point_stack_index = stack_top - 1;
	conn_point = stack_top <= 0 ? NULL : node_stack[stack_top-1];
	return conn_point;
}

int btree_delete(btree_t *btree, map_key_t key, tdata_t *tdata)
{
	tm_begin_ret_t status;
	btree_node_t *node_stack[20], *n;
	btree_node_t *connection_point, *tree_cp_root;
	btree_node_t *to_modify_sibling, *new_sibling;
	int node_stack_indexes[20], stack_top, index, retries = -1;
	int connection_point_stack_index;

try_from_scratch:

	ht_reset(tdata->ht);
	to_modify_sibling = new_sibling = NULL;

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&btree->lock);
		btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
		if (stack_top < 0) {
			pthread_spin_unlock(&btree->lock);
			return 0;
		}
		n = node_stack[stack_top];
		index = node_stack_indexes[stack_top];
		if (index >= n->no_keys || KEY_CMP(n->keys[index], key) != 0) {
			pthread_spin_unlock(&btree->lock);
			return 0;
		}
		connection_point = btree_delete_with_copy(key,
		                                  node_stack, node_stack_indexes, stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index,
		                                  &to_modify_sibling, &new_sibling, tdata);
		if (connection_point == NULL) {
			btree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
		}
		pthread_spin_unlock(&btree->lock);
		return 1;
	}

	//> Asynchronized traversal. If key is not there we can safely return.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
	if (stack_top < 0) return 0;
	n = node_stack[stack_top];
	index = node_stack_indexes[stack_top];
	if (index >= n->no_keys || KEY_CMP(n->keys[index], key) != 0) return 0;

	connection_point = btree_delete_with_copy(key,
	                                  node_stack, node_stack_indexes, stack_top,
	                                  &tree_cp_root,
	                                  &connection_point_stack_index,
	                                  &to_modify_sibling, &new_sibling, tdata);

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) goto try_from_scratch;
	while (btree->lock != LOCK_FREE) ;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (btree->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		//> Validate copy
		if (stack_top < 0 && btree->root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && btree->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		int i;
		btree_node_t *n1, *n2;
		for (i=0; i < stack_top; i++) {
			n1 = node_stack[i];
			index = node_stack_indexes[i];
			n2 = n1->children[index];
			if (n2 != node_stack[i+1])
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				btree_node_t **np = tdata->ht->entries[i][j];
				btree_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (connection_point == NULL) {
			btree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
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

int btree_update(btree_t *btree, map_key_t key, void *val, tdata_t *tdata)
{
	tm_begin_ret_t status;
	btree_node_t *node_stack[20];
	btree_node_t *connection_point, *tree_cp_root;
	btree_node_t *to_modify_sibling, *new_sibling;
	int node_stack_indexes[20], stack_top, index;
	int retries = -1;
	int connection_point_stack_index;
	int op_is_insert = -1, ret;

try_from_scratch:

	ht_reset(tdata->ht);
	to_modify_sibling = new_sibling = NULL;

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&btree->lock);
		btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
		if (op_is_insert == -1) {
			if (stack_top < 0)
				op_is_insert = 1;
			else if (node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
			         KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0)
				op_is_insert = 0;
			else
				op_is_insert = 1;
		}
		if (op_is_insert && stack_top >= 0 && node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
		        KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0) {
			pthread_spin_unlock(&btree->lock);
			return 0;
		} else if (!op_is_insert && (stack_top < 0 || 
		            node_stack_indexes[stack_top] >= node_stack[stack_top]->no_keys ||
		            KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) != 0)) {
			pthread_spin_unlock(&btree->lock);
			return 2;
		}
		if (op_is_insert) {
			connection_point = btree_insert_with_copy(key, val, 
			                                  node_stack, node_stack_indexes, stack_top,
			                                  &tree_cp_root,
			                                  &connection_point_stack_index,
			                                  &to_modify_sibling, &new_sibling, tdata);
			ret = 1;
		} else {
			connection_point = btree_delete_with_copy(key,
			                                  node_stack, node_stack_indexes, stack_top,
			                                  &tree_cp_root,
			                                  &connection_point_stack_index,
			                                  &to_modify_sibling, &new_sibling, tdata);
			ret = 3;
		}
		if (connection_point == NULL) {
			btree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
		}
		if (to_modify_sibling != NULL) to_modify_sibling->sibling = new_sibling;
		pthread_spin_unlock(&btree->lock);
		return ret;
	}

	//> Asynchronized traversal. If key is there we can safely return.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes, &stack_top);
	if (op_is_insert == -1) {
		if (stack_top < 0)
			op_is_insert = 1;
		else if (node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
		         KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0)
			op_is_insert = 0;
		else
			op_is_insert = 1;
	}
	if (op_is_insert && stack_top >= 0 && node_stack_indexes[stack_top] < 2 * BTREE_ORDER &&
	        KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0)
		return 0;
	else if (!op_is_insert && (stack_top < 0 || 
	            node_stack_indexes[stack_top] >= node_stack[stack_top]->no_keys ||
	            KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) != 0))
		return 2;

	if (op_is_insert) {
		connection_point = btree_insert_with_copy(key, val, 
		                                  node_stack, node_stack_indexes, stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index,
		                                  &to_modify_sibling, &new_sibling, tdata);
		ret = 1;
	} else {
		connection_point = btree_delete_with_copy(key,
		                                  node_stack, node_stack_indexes, stack_top,
		                                  &tree_cp_root,
		                                  &connection_point_stack_index,
		                                  &to_modify_sibling, &new_sibling, tdata);
		ret = 3;
	}

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) goto try_from_scratch;
	while (btree->lock != LOCK_FREE) ;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (btree->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		//> Validate copy
		if (stack_top < 0 && btree->root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && btree->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		int i;
		btree_node_t *n1, *n2;
		for (i=0; i < stack_top; i++) {
			n1 = node_stack[i];
			index = node_stack_indexes[i];
			n2 = n1->children[index];
			if (n2 != node_stack[i+1])
				TX_ABORT(ABORT_VALIDATION_FAILURE);
		}
		int j;
		for (i=0; i < HT_LEN; i++) {
			for (j=0; j < tdata->ht->bucket_next_index[i]; j+=2) {
				btree_node_t **np = tdata->ht->entries[i][j];
				btree_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (connection_point == NULL) {
			btree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
		}
		if (to_modify_sibling != NULL) to_modify_sibling->sibling = new_sibling;
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
/* MAP interface implementation                                               */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(btree_node_t));
	return btree_new();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(btree_node_t));
	tdata_t *tdata = tdata_new(tid);
	return tdata;
}

void map_tdata_print(void *tdata)
{
	tdata_print(tdata);
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	tdata_add(d1, d2, dst);
}

int map_lookup(void *map, void *tdata, map_key_t key)
{
	return btree_lookup(map, key);
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	return btree_rquery(map, key1, key2);
}

int map_insert(void *map, void *tdata, map_key_t key, void *value)
{
	return btree_insert(map, key, value, tdata);
}

int map_delete(void *map, void *tdata, map_key_t key)
{
	return btree_delete(map, key, tdata);
}

int map_update(void *map, void *tdata, map_key_t key, void *value)
{
	return btree_update(map, key, value, tdata);
}

void map_print(void *map)
{
	btree_print(map);
}

int map_validate(void *map)
{
	return btree_validate_helper(map);
}

char *map_name()
{
	char *str;
	XMALLOC(str, 30);
	sprintf(str, "btree-rcu-htm ( BTREE_ORDER: %d )", BTREE_ORDER);
	return str;
}
