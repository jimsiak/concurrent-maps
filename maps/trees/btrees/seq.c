#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "alloc.h"
#include "../../key/key.h"
#include "btree.h"
#include "validate.h"
#include "print.h"

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm/htm.h"
#endif

static int btree_traverse(btree_t *btree, map_key_t key,
                          btree_node_t **_leaf, int *_index)
{
	int index;
	btree_node_t *n = btree->root;

	//> Empty tree.
	if (!n) return 0;

	while (!n->leaf) {
		index = btree_node_search(n, key);
		if (index < n->no_keys && KEY_CMP(n->keys[index], key) == 0) index++;
		n = n->children[index];
	}
	index = btree_node_search(n, key);

	*_leaf = n;
	*_index = index;
	return 1;
}

static int btree_lookup(btree_t *btree, map_key_t key)
{
	int index;
	btree_node_t *leaf;

	if (btree_traverse(btree, key, &leaf, &index) == 0)
		return 0;
	else
		return (KEY_CMP(leaf->keys[index], key) == 0);
}

static __thread map_key_t rquery_result[1000];

static int btree_rquery(btree_t *btree, map_key_t key1, map_key_t key2, int *len)
{
	int index, i, nkeys;
	btree_node_t *leaf, *n;

	*len = 0;

	if (btree_traverse(btree, key1, &leaf, &index) == 0)
		return 0;

	if (KEY_CMP(leaf->keys[index], key2) > 0)
		return 0;
	
	nkeys = 0;
	n = leaf;
	while (n != NULL) {
		for (i = index; i < n->no_keys && KEY_CMP(n->keys[i], key2) <= 0; i++) {
			KEY_COPY(rquery_result[nkeys], n->keys[i]);
			nkeys++;
		}
		if (i < n->no_keys && KEY_CMP(n->keys[i], key2) >= 0)
			break;
		n = n->sibling;
		index = 0;
	}

	*len = nkeys;
	return 1;
}

static void btree_traverse_stack(btree_t *btree, map_key_t key,
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
		if (index < n->no_keys && KEY_CMP(n->keys[index], key) == 0) index++;
		node_stack[++(*node_stack_top)] = n;
		node_stack_indexes[*node_stack_top] = index;
		n = n->children[index];
	}
	index = btree_node_search(n, key);
	node_stack[++(*node_stack_top)] = n;
	node_stack_indexes[*node_stack_top] = index;
}

static int btree_do_insert(btree_t *btree, map_key_t key, void *val,
                           btree_node_t **node_stack, int *node_stack_indexes,
                           int node_stack_top)
{
	btree_node_t *n;
	int index;

	//> Empty tree case.
	if (node_stack_top == -1) {
		n = btree_node_new(1);
		btree_node_insert_index(n, 0, key, val);
		btree->root = n;
		return 1;
	}

	n = node_stack[node_stack_top];
	index = node_stack_indexes[node_stack_top];

	//> Case of a not full leaf.
	if (n->no_keys < 2 * BTREE_ORDER) {
		btree_node_insert_index(n, index, key, val);
		return 1;
	}

	//> Case of full leaf.
	btree_node_t *rnode = btree_leaf_split(n, index, key, val);

	btree_node_t *internal;
	int internal_index;
	map_key_t key_to_add;
	void *ptr_to_add = rnode;

	KEY_COPY(key_to_add, rnode->keys[0]);

	while (1) {
		node_stack_top--;

		//> We surpassed the root. New root needs to be created.
		if (node_stack_top < 0) {
			btree->root = btree_node_new(0);
			btree_node_insert_index(btree->root, 0, key_to_add, ptr_to_add);
			btree->root->children[0] = n;
			break;
		}

		internal = node_stack[node_stack_top];
		internal_index = node_stack_indexes[node_stack_top];

		//> Internal node not full.
		if (internal->no_keys < 2 * BTREE_ORDER) {
			btree_node_insert_index(internal, internal_index, key_to_add, ptr_to_add);
			break;
		}

		//> Internal node full.
		rnode = btree_internal_split(internal, internal_index, key_to_add, ptr_to_add,
		                             &key_to_add);
		ptr_to_add = rnode;
		n = internal;
	}

	return 1;
}

static int btree_insert(btree_t *btree, map_key_t key, void *val)
{
	btree_node_t *node_stack[20];
	int node_stack_indexes[20], node_stack_top = -1;

	//> Route to the appropriate leaf.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes,
	                     &node_stack_top);

	//> Key already in the tree.
	int index = node_stack_indexes[node_stack_top];
	btree_node_t *n = node_stack[node_stack_top];
	if (node_stack_top >= 0 && index < 2 * BTREE_ORDER && KEY_CMP(key, n->keys[index]) == 0)
		return 0;
	//> Key not in the tree.
	return btree_do_insert(btree, key, val, node_stack, node_stack_indexes, node_stack_top);
}

/**
 * c = current
 * p = parent
 * pindex = parent_index
 * Returns: the index of the key to be deleted from the parent node.
 **/
static int btree_merge(btree_node_t *c, btree_node_t *p, int pindex)
{
	int i, sibling_index;
	btree_node_t *sibling;

	//> Left sibling first.
	if (pindex > 0) {
		sibling = p->children[pindex - 1];
		sibling_index = sibling->no_keys;

		if (!c->leaf) {
			KEY_COPY(sibling->keys[sibling_index], p->keys[pindex - 1]);
			sibling->children[sibling_index+1] = c->children[0];
			sibling_index++;
		}
		for (i=0; i < c->no_keys; i++) {
			KEY_COPY(sibling->keys[sibling_index], c->keys[i]);
			sibling->children[sibling_index + 1] = c->children[i + 1];
			sibling_index++;
		}

		sibling->sibling = c->sibling;
		sibling->no_keys = sibling_index;
		return (pindex - 1);
	}

	//> Right sibling then.
	if (pindex < p->no_keys) {
		sibling = p->children[pindex + 1];
		sibling_index = c->no_keys;

		if (!c->leaf) {
			KEY_COPY(c->keys[sibling_index], p->keys[pindex]);
			c->children[sibling_index+1] = sibling->children[0];
			sibling_index++;
		}
		for (i=0; i < sibling->no_keys; i++) {
			KEY_COPY(c->keys[sibling_index], sibling->keys[i]);
			c->children[sibling_index + 1] = sibling->children[i + 1];
			sibling_index++;
		}

		c->sibling = sibling->sibling;
		c->no_keys = sibling_index;
		return pindex;
	}

	//> Unreachable code.
	assert(0);
	return -1;
}

/**
 * c = current
 * p = parent
 * pindex = parent_index
 * Returns: 1 if borrowing was successful, 0 otherwise.
 **/
static int btree_borrow_keys(btree_node_t *c, btree_node_t *p, int pindex)
{
	int i;
	btree_node_t *sibling;

	//> Left sibling first.
	if (pindex > 0) {
		sibling = p->children[pindex - 1];
		if (sibling->no_keys > BTREE_ORDER) {
			for (i = c->no_keys-1; i >= 0; i--)
				KEY_COPY(c->keys[i+1], c->keys[i]);
			for (i = c->no_keys; i >= 0; i--) c->children[i+1] = c->children[i];
			if (!c->leaf) {
				if (KEY_CMP(c->keys[0], p->keys[pindex-1]) == 0)
					KEY_COPY(c->keys[0], sibling->keys[sibling->no_keys-1]);
				else
					KEY_COPY(c->keys[0], p->keys[pindex-1]);
				c->children[0] = sibling->children[sibling->no_keys];
				KEY_COPY(p->keys[pindex-1], sibling->keys[sibling->no_keys-1]);
			} else {
				KEY_COPY(c->keys[0], sibling->keys[sibling->no_keys-1]);
				c->children[1] = sibling->children[sibling->no_keys];
				KEY_COPY(p->keys[pindex-1], c->keys[0]);
			}
			sibling->no_keys--;
			c->no_keys++;
			return 1;
		}
	}

	//> Right sibling next.
	if (pindex < p->no_keys) {
		sibling = p->children[pindex + 1];
		if (sibling->no_keys > BTREE_ORDER) {
			if (!c->leaf) {
				KEY_COPY(c->keys[c->no_keys], p->keys[pindex]);
				c->children[c->no_keys+1] = sibling->children[0];
				KEY_COPY(p->keys[pindex], sibling->keys[0]);
			} else {
				KEY_COPY(c->keys[c->no_keys], sibling->keys[0]);
				c->children[c->no_keys+1] = sibling->children[1];
				KEY_COPY(p->keys[pindex], sibling->keys[1]);
			}
			for (i=0; i < sibling->no_keys-1; i++)
				KEY_COPY(sibling->keys[i], sibling->keys[i+1]);
			for (i=0; i < sibling->no_keys; i++)
				sibling->children[i] = sibling->children[i+1];
			sibling->no_keys--;
			c->no_keys++;
			return 1;
		}
	}

	//> Could not borrow for either of the two siblings.
	return 0;
}

static int btree_do_delete(btree_t *btree, map_key_t key, btree_node_t **node_stack,
                           int *node_stack_indexes, int node_stack_top)
{
	btree_node_t *n = node_stack[node_stack_top];
	int index = node_stack_indexes[node_stack_top];
	btree_node_t *cur = node_stack[node_stack_top];
	int cur_index = node_stack_indexes[node_stack_top];
	btree_node_t *parent;
	int parent_index;
	while (1) {
		//> We reached root which contains only one key.
		if (node_stack_top == 0 && cur->no_keys == 1) {
			btree->root = cur->children[0];
			break;
		}

		//> Delete the key from the current node.
		btree_node_delete_index(cur, cur_index);

		//> Root can be less than half-full.
		if (node_stack_top == 0) break;

		//> If current node is at least half-full, we are done.
		if (cur->no_keys >= BTREE_ORDER)
			break;

		//> First try to borrow keys from siblings
		parent = node_stack[node_stack_top-1];
		parent_index = node_stack_indexes[node_stack_top-1];
		if (btree_borrow_keys(cur, parent, parent_index))
			break;

		//> If everything has failed, merge nodes
		cur_index = btree_merge(cur, parent, parent_index);

		//> Move one level up
		cur = node_stack[--node_stack_top];
	}
	return 1;
}

static int btree_delete(btree_t *btree, map_key_t key)
{
	int index;
	btree_node_t *n;
	btree_node_t *node_stack[20];
	int node_stack_indexes[20];
	int node_stack_top = -1;

	//> Route to the appropriate leaf.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes,
	                     &node_stack_top);

	//> Empty tree case.
	if (node_stack_top == -1) return 0;
	//> Key not in the tree.
	n = node_stack[node_stack_top];
	index = node_stack_indexes[node_stack_top];
	if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0) return 0;

	return btree_do_delete(btree, key, node_stack, node_stack_indexes,
	                       node_stack_top);
}

static int btree_update(btree_t *btree, map_key_t key, void *val)
{
	btree_node_t *node_stack[20];
	int node_stack_indexes[20], node_stack_top = -1;
	int op_is_insert = -1;

	//> Route to the appropriate leaf.
	btree_traverse_stack(btree, key, node_stack, node_stack_indexes,
	                     &node_stack_top);

	//> Empty tree case.
	if (node_stack_top == -1) {
		op_is_insert = 1;
	} else {
		int index = node_stack_indexes[node_stack_top];
		btree_node_t *n = node_stack[node_stack_top];
		if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0) op_is_insert = 1;
		else if (index < 2 * BTREE_ORDER && KEY_CMP(key, n->keys[index]) == 0) op_is_insert = 0;
	}
	

	if (op_is_insert)
		return btree_do_insert(btree, key, val, node_stack, node_stack_indexes,
		                       node_stack_top);
	else
		return btree_do_delete(btree, key, node_stack, node_stack_indexes,
		                       node_stack_top) + 2;
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(btree_node_t));
	return btree_new();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(btree_node_t));
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
	return;
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
	pthread_spin_lock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((btree_t *)map)->lock);
#	endif

	ret = btree_lookup(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((btree_t *)map)->lock);
#	endif

	return ret; 
}

int map_rquery(void *map, void *thread_data, map_key_t key1, map_key_t key2)
{
	int ret = 0, nkeys;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((btree_t *)map)->lock);
#	endif

	ret = btree_rquery(map, key1, key2, &nkeys);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((btree_t *)map)->lock);
#	endif

	return ret; 
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((btree_t *)map)->lock);
#	endif

	ret = btree_insert(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((btree_t *)map)->lock);
#	endif
	return ret;
}

int map_delete(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((btree_t *)map)->lock);
#	endif

	ret = btree_delete(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((btree_t *)map)->lock);
#	endif

	return ret;
}

int map_update(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((btree_t *)map)->lock);
#	endif

	ret = btree_update(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((btree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((btree_t *)map)->lock);
#	endif
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
#	if defined(SYNC_CG_SPINLOCK)
	return "btree-cg-lock";
#	elif defined(SYNC_CG_HTM)
	return "btree-cg-htm";
#	else
	return "btree-sequential";
#	endif
}

void map_print(void *map)
{
	btree_print(map);
}
