#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#include "alloc.h"
#include "htm/htm.h"
#include "ht.h"
#include "../../../map.h"
#include "../../../rcu-htm/tdata.h"
#include "../../../key/key.h"

#define ABTREE_DEGREE_MAX 16
#define ABTREE_DEGREE_MIN 8
#define MAX_HEIGHT 20

typedef struct abtree_node_s {
	char leaf,
	     marked,
	     tag;
	int no_keys;

	map_key_t keys[ABTREE_DEGREE_MAX];
	__attribute__((aligned(16))) void *children[ABTREE_DEGREE_MAX + 1];
} abtree_node_t;

typedef struct {
	abtree_node_t *root;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t lock;
#	endif
} abtree_t;

static __thread void *nalloc;

/**
 * Creates a new abtree_node_t
 **/
static abtree_node_t *abtree_node_new(char leaf)
{
	abtree_node_t *ret = nalloc_alloc_node(nalloc);
	ret->no_keys = 0;
	ret->leaf = leaf;
	return ret;
}

static int abtree_node_search(abtree_node_t *n, map_key_t key)
{
	int i = 0;
	while (i < n->no_keys && KEY_CMP(key, n->keys[i]) > 0) i++;
	return i;
}

static abtree_node_t *abtree_node_get_child(abtree_node_t *n, map_key_t key)
{
	int index = abtree_node_search(n, key);
	if (index < n->no_keys && KEY_CMP(n->keys[index], key) == 0) index++;
	return n->children[index];
}

static void abtree_node_delete_index(abtree_node_t *n, int index)
{
	int i;
	assert(index < n->no_keys);
	for (i=index+1; i < n->no_keys; i++) {
		KEY_COPY(n->keys[i-1], n->keys[i]);
		n->children[i] = n->children[i+1];
	}
	n->no_keys--;
}

static void abtree_node_print(abtree_node_t *n)
{
	int i;

	printf("abtree_node: [");
	if (!n) {
		printf("]\n");
		return;
	}

	for (i=0; i < n->no_keys; i++)
		KEY_PRINT(n->keys[i], " ", " |");
	printf("]");
	printf("%s - %s\n", n->leaf ? " LEAF" : "", n->tag ? " TAGGED" : "");
}

static abtree_t *abtree_new()
{
	abtree_t *ret;
	XMALLOC(ret, 1);
	ret->root = NULL;
#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spin_init(&ret->lock, PTHREAD_PROCESS_SHARED);
#	endif
	return ret;
}

/**
 * Insert 'key' in position 'index' of node 'n'.
 * 'ptr' is either a pointer to data record in the case of a leaf 
 * or a pointer to a child abtree_node_t in the case of an internal node.
 **/
static void abtree_node_insert_index(abtree_node_t *n, int index, map_key_t key,
                                    void *ptr)
{
	int i;

	for (i=n->no_keys; i > index; i--) {
		KEY_COPY(n->keys[i], n->keys[i-1]);
		n->children[i+1] = n->children[i];
	}
	KEY_COPY(n->keys[index], key);
	n->children[index+1] = ptr;

	n->no_keys++;
}

static map_key_t abtree_node_get_max_key(abtree_node_t *n)
{
	int i;
	map_key_t max;
	KEY_COPY(max, n->keys[0]);
	for (i=1; i < n->no_keys; i++)
		if (KEY_CMP(n->keys[i], max) > 0) KEY_COPY(max, n->keys[i]);
	return max;
}

static int abtree_lookup(abtree_t *abtree, map_key_t key)
{
	int index;
	abtree_node_t *n = abtree->root;

	//> Empty tree.
	if (!n) return 0;

	while (!n->leaf) {
		index = abtree_node_search(n, key);
		if (index < n->no_keys && KEY_CMP(n->keys[index], key)) index++;
		n = n->children[index];
	}
	index = abtree_node_search(n, key);
	return (KEY_CMP(n->keys[index], key) == 0);
}

static void abtree_traverse_stack(abtree_t *abtree, map_key_t key,
                          abtree_node_t **node_stack, int *node_stack_indexes,
                          int *node_stack_top)
{
	int index;
	abtree_node_t *n;

	*node_stack_top = -1;
	n = abtree->root;
	if (!n) return;

	while (!n->leaf) {
		index = abtree_node_search(n, key);
		if (index < n->no_keys && KEY_CMP(n->keys[index], key) == 0) index++;
		node_stack[++(*node_stack_top)] = n;
		node_stack_indexes[*node_stack_top] = index;
		n = n->children[index];
	}
	index = abtree_node_search(n, key);
	node_stack[++(*node_stack_top)] = n;
	node_stack_indexes[*node_stack_top] = index;
}

static void abtree_join_parent_with_child(abtree_node_t *p, int pindex,
                                          abtree_node_t *l)
{
	int new_no_keys = p->no_keys + l->no_keys;
	int i;

	//> copy keys first
	int first_key_to_shift = pindex;
	int last_key_to_shift = p->no_keys - 1;
	int index = new_no_keys - 1;
	for (i=last_key_to_shift; i >= first_key_to_shift; i--)
		KEY_COPY(p->keys[index--], p->keys[i]);
	for (i=0; i < l->no_keys; i++)
		KEY_COPY(p->keys[i + first_key_to_shift], l->keys[i]);

	//> copy pointers then
	int first_ptr_to_shift = pindex;
	int last_ptr_to_shift = p->no_keys;
	index = new_no_keys;
	for (i=last_ptr_to_shift; i >= first_ptr_to_shift; i--)
		p->children[index--] = p->children[i];
	for (i=0; i <= l->no_keys; i++)
		p->children[i + first_ptr_to_shift] = l->children[i];

	p->no_keys = new_no_keys;
}

static void abtree_split_parent_and_child(abtree_node_t *p, int pindex,
                                          abtree_node_t *l)
{
	int i, k1 = 0, k2 = 0;
	map_key_t keys[ABTREE_DEGREE_MAX * 2];
	void *ptrs[ABTREE_DEGREE_MAX * 2 + 1];

	//> Get all keys
	for (i=0; i < pindex; i++)          KEY_COPY(keys[k1++], p->keys[i]);
	for (i=0; i < l->no_keys; i++)      KEY_COPY(keys[k1++], l->keys[i]);
	for (i=pindex; i < p->no_keys; i++) KEY_COPY(keys[k1++], p->keys[i]);
	//> Get all pointers
	for (i=0; i < pindex; i++)             ptrs[k2++] = p->children[i];
	for (i=0; i <= l->no_keys; i++)        ptrs[k2++] = l->children[i];
	for (i=pindex+1; i <= p->no_keys; i++) ptrs[k2++] = p->children[i];

	k1 = k2 = 0;
	int sz = p->no_keys + l->no_keys;
	int leftsz = sz / 2;
	int rightsz = sz - leftsz - 1;

	//> Create new left node
	abtree_node_t *new_left = abtree_node_new(0);
	for (i=0; i < leftsz; i++) {
		KEY_COPY(new_left->keys[i], keys[k1++]);
		new_left->children[i] = ptrs[k2++];
	}
	new_left->children[leftsz] = ptrs[k2++];
	new_left->tag = 0;
	new_left->no_keys = leftsz;

	//> Fix the parent
	KEY_COPY(p->keys[0], keys[k1++]);
	p->children[0] = new_left;
	p->children[1] = l;
	// FIXME tag parent here?? or outside this function??
	p->no_keys = 1;

	//> Fix the old l node which now becomes the right node
	for (i=0; i < rightsz; i++) {
		KEY_COPY(l->keys[i], keys[k1++]);
		l->children[i] = ptrs[k2++];
	}
	l->children[rightsz] = ptrs[k2++];
	l->tag = 0;
	l->no_keys = rightsz;
}

static void abtree_join_siblings(abtree_node_t *p, abtree_node_t *l,
                                 abtree_node_t *s, int lindex, int sindex)
{
	int left_index, right_index;
	abtree_node_t *left, *right;
	int i, k1, k2;

	left_index  = lindex < sindex ? lindex : sindex;
	right_index = lindex < sindex ? sindex : lindex;
	left  = p->children[left_index];
	right = p->children[right_index];

	//> Move all keys to the left node
	k1 = left->no_keys;
	k2 = left->no_keys + 1;
	if (!left->leaf) KEY_COPY(left->keys[k1++], p->keys[left_index]);
	for (i=0; i < right->no_keys; i++)
		KEY_COPY(left->keys[k1++], right->keys[i]);
	for (i = (left->leaf) ? 1 : 0; i < right->no_keys; i++)
		left->children[k2++] = right->children[i];
	left->children[k2++] = right->children[right->no_keys];
	left->tag = 0;
	left->no_keys = k1;
	
	//> Fix the parent
	for (i=left_index + 1; i < p->no_keys; i++) {
		KEY_COPY(p->keys[i-1], p->keys[i]);
		p->children[i] = p->children[i+1];
	}
	p->tag = 0;
	p->no_keys--;
}

static void abtree_redistribute_sibling_keys(abtree_node_t *p, abtree_node_t *l,
                                             abtree_node_t *s, int lindex,
                                             int sindex)
{
	map_key_t keys[ABTREE_DEGREE_MAX * 2];
	void *ptrs[ABTREE_DEGREE_MAX * 2 + 1];
	int left_index, right_index;
	abtree_node_t *left, *right;
	int i, k1 = 0, k2 = 0, total_keys, left_keys, right_keys;

	left_index  = lindex < sindex ? lindex : sindex;
	right_index = lindex < sindex ? sindex : lindex;
	left  = p->children[left_index];
	right = p->children[right_index];

	//> Gather all keys in keys array
	for (i=0; i < left->no_keys; i++) {
		KEY_COPY(keys[k1++], left->keys[i]);
		ptrs[k2++] = left->children[i];
	}
	ptrs[k2++] = left->children[left->no_keys];
	if (!left->leaf) KEY_COPY(keys[k1++], p->keys[left_index]);
	for (i=0; i < right->no_keys; i++)
		KEY_COPY(keys[k1++], right->keys[i]);
	for (i = (left->leaf) ? 1 : 0; i < right->no_keys; i++)
		ptrs[k2++] = right->children[i];
	ptrs[k2++] = right->children[right->no_keys];

	//> Calculate new number of keys in left and right
	total_keys = k1;
	left_keys = k1 / 2;
	right_keys = total_keys - left_keys;
	if (!left->leaf) right_keys--; // If not leaf one key goes to parent

	//> Fix left
	k1 = k2 = 0;
	for (i=0; i < left_keys; i++) {
		KEY_COPY(left->keys[i], keys[k1++]);
		left->children[i] = ptrs[k2++];
	}
	left->children[left_keys] = ptrs[k2++];
	left->no_keys = left_keys;

	//> Fix parent
	KEY_COPY(p->keys[left_index], keys[k1]);
	if (!left->leaf) k1++; 

	//> Fix right
	for (i=0; i < right_keys; i++)
		KEY_COPY(right->keys[i], keys[k1++]);
	for (i = (left->leaf) ? 1 : 0; i < right_keys; i++)
		right->children[i] = ptrs[k2++];
	right->children[right_keys] = ptrs[k2++];
	right->no_keys = right_keys;
}

static void abtree_rebalance(abtree_t *abtree, abtree_node_t **node_stack,
                             int *node_stack_indexes, int stack_top,
                             int *should_rebalance)
{
	abtree_node_t *gp, *p, *l, *s;
	int i = 0, pindex, sindex;

	*should_rebalance = 0;

	//> Root is a leaf, so nothing needs to be done
	if (node_stack[0]->leaf) return;

	gp = NULL;
	p  = node_stack[i++];
	pindex = node_stack_indexes[i-1];
	l  = node_stack[i++];
	while (!l->leaf && !l->tag && l->no_keys >= ABTREE_DEGREE_MIN) {
		gp = p;
		p = l;
		pindex = node_stack_indexes[i-1];
		l = node_stack[i++];
	}

	//> No violation to fix
	if (!l->tag && l->no_keys >= ABTREE_DEGREE_MIN) return;

	if (l->tag) {
		if (p->no_keys + l->no_keys <= ABTREE_DEGREE_MAX) {
			//> Join l with its parent
			abtree_join_parent_with_child(p, pindex, l);
		} else {
			//> Split child and parent
			abtree_split_parent_and_child(p, pindex, l);
			p->tag = (gp != NULL); //> Tag parent if not root
			*should_rebalance = 1;
		}
	} else if (l->no_keys < ABTREE_DEGREE_MIN) {
		sindex = pindex ? pindex - 1 : pindex + 1;
		s = p->children[sindex];
		if (s->tag) {
			//> FIXME
		} else {
			if (l->no_keys + s->no_keys + 1 <= ABTREE_DEGREE_MAX) {
				//> Join l and s
				abtree_join_siblings(p, l, s, pindex, sindex);
				*should_rebalance = (gp != NULL && p->no_keys < ABTREE_DEGREE_MIN);
			} else {
				//> Redistribute keys between s and l
				abtree_redistribute_sibling_keys(p, l, s, pindex, sindex);
			}
		}
	} else {
		assert(0);
	}
}

/**
 * Splits a leaf node into two leaf nodes which also contain the newly
 * inserted key 'key'.
 **/ 
abtree_node_t *abtree_leaf_split(abtree_node_t *n, int index, map_key_t key, void *ptr)
{
	int i, k=0, first_key_to_move = n->no_keys / 2;
	abtree_node_t *rnode = abtree_node_new(1);

	//> Move half of the keys on the new node.
	for (i=first_key_to_move; i < n->no_keys; i++) {
		KEY_COPY(rnode->keys[k], n->keys[i]);
		rnode->children[k++] = n->children[i];
	}
	rnode->children[k] = n->children[i];

	//> Update number of keys for the two split nodes.
	n->no_keys -= k;
	rnode->no_keys = k;

	//> Insert the new key in the appropriate node.
	if (index < first_key_to_move) abtree_node_insert_index(n, index, key, ptr);
	else   abtree_node_insert_index(rnode, index - first_key_to_move, key, ptr);

	return rnode;
}

static int abtree_do_insert(abtree_t *abtree, map_key_t key, void *val,
                           abtree_node_t **node_stack, int *node_stack_indexes,
                           int *node_stack_top, int *should_rebalance)
{
	abtree_node_t *n;
	int index, pindex;

	*should_rebalance = 0;

	//> Empty tree case.
	if (*node_stack_top == -1) {
		n = abtree_node_new(1);
		abtree_node_insert_index(n, 0, key, val);
		abtree->root = n;
		return 1;
	}

	n = node_stack[*node_stack_top];
	index = node_stack_indexes[*node_stack_top];

	//> Case of a not full leaf.
	if (n->no_keys < ABTREE_DEGREE_MAX) {
		abtree_node_insert_index(n, index, key, val);
		return 1;
	}

	//> Case of a not full leaf.
	abtree_node_t *rnode = abtree_leaf_split(n, index, key, val);
	abtree_node_t *parent_new = abtree_node_new(0);
	abtree_node_insert_index(parent_new, 0, rnode->keys[0], rnode);
	parent_new->children[0] = n;
	parent_new->tag = 1;

	//> We surpassed the root. New root needs to be created.
	if (*node_stack_top == 0) {
		abtree->root = parent_new;
		parent_new->tag = 0;
	} else {
		abtree_node_t *p = node_stack[*node_stack_top - 1];
		pindex = node_stack_indexes[*node_stack_top - 1];
		p->children[pindex] = parent_new;
	}

	//> Fix the node_stack for the rebalancing
	pindex = (KEY_CMP(key, parent_new->keys[0]) < 0) ? 0 : 1;
	node_stack[*node_stack_top] = parent_new;
	node_stack_indexes[*node_stack_top] = pindex;
	node_stack[++(*node_stack_top)] = parent_new->children[pindex];
	*should_rebalance = 1;
	return 1;
}

static int abtree_insert(abtree_t *abtree, map_key_t key, void *val)
{
	abtree_node_t *node_stack[MAX_HEIGHT];
	int node_stack_indexes[MAX_HEIGHT], node_stack_top = -1;
	int should_rebalance, ret;

	//> Route to the appropriate leaf.
	abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes,
	                     &node_stack_top);
	int index = node_stack_indexes[node_stack_top];
	abtree_node_t *n = node_stack[node_stack_top];
	//> Key already in the tree.
	if (node_stack_top >= 0 && index < ABTREE_DEGREE_MAX &&
	                           KEY_CMP(key, n->keys[index]) == 0)
		return 0;
	//> Key not in the tree.
	ret = abtree_do_insert(abtree, key, val, node_stack, node_stack_indexes,
	                       &node_stack_top, &should_rebalance);
	while (should_rebalance)
		abtree_rebalance(abtree, node_stack, node_stack_indexes, node_stack_top,
		                 &should_rebalance);
	return ret;
}

static int abtree_do_delete(abtree_t *abtree, map_key_t key, abtree_node_t **node_stack,
                           int *node_stack_indexes, int node_stack_top,
                           int *should_rebalance)
{
	abtree_node_t *n = node_stack[node_stack_top];
	int index = node_stack_indexes[node_stack_top];
	abtree_node_t *cur = node_stack[node_stack_top];
	int cur_index = node_stack_indexes[node_stack_top];
	abtree_node_t *parent;
	int parent_index;

	abtree_node_delete_index(cur, cur_index);
	*should_rebalance = cur->no_keys < ABTREE_DEGREE_MIN;
	return 1;
}

static int abtree_delete(abtree_t *abtree, map_key_t key)
{
	int index;
	abtree_node_t *n;
	abtree_node_t *node_stack[MAX_HEIGHT];
	int node_stack_indexes[MAX_HEIGHT];
	int node_stack_top = -1;
	int should_rebalance, ret;

	//> Route to the appropriate leaf.
	abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes,
	                     &node_stack_top);

	//> Empty tree case.
	if (node_stack_top == -1) return 0;
	n = node_stack[node_stack_top];
	index = node_stack_indexes[node_stack_top];
	//> Key not in the tree.
	if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0)
		return 0;
	//> Key in the tree.
	ret = abtree_do_delete(abtree, key, node_stack, node_stack_indexes,
	                       node_stack_top, &should_rebalance);
	while (should_rebalance)
		abtree_rebalance(abtree, node_stack, node_stack_indexes, node_stack_top,
		                 &should_rebalance);
	return ret;
}

static int abtree_update(abtree_t *abtree, map_key_t key, void *val)
{
	abtree_node_t *node_stack[MAX_HEIGHT];
	int node_stack_indexes[MAX_HEIGHT], node_stack_top = -1;
	int op_is_insert = -1, should_rebalance, ret;

	//> Route to the appropriate leaf.
	abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes,
	                      &node_stack_top);

	//> Empty tree case.
	if (node_stack_top == -1) {
		op_is_insert = 1;
	} else {
		int index = node_stack_indexes[node_stack_top];
		abtree_node_t *n = node_stack[node_stack_top];
		if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0)
			op_is_insert = 1;
		else if (index < ABTREE_DEGREE_MAX && KEY_CMP(key, n->keys[index]) == 0)
			op_is_insert = 0;
	}
	

	if (op_is_insert)
		ret = abtree_do_insert(abtree, key, val, node_stack, node_stack_indexes,
		                       &node_stack_top, &should_rebalance);
	else
		ret = abtree_do_delete(abtree, key, node_stack, node_stack_indexes,
		                       node_stack_top, &should_rebalance) + 2;
	while (should_rebalance)
		abtree_rebalance(abtree, node_stack, node_stack_indexes, node_stack_top,
		                 &should_rebalance);
	return ret;
}

static void abtree_print_rec(abtree_node_t *root, int level)
{
	int i;

	printf("[LVL %4d]: ", level);
	fflush(stdout);
	abtree_node_print(root);

	if (!root || root->leaf) return;

	for (i=0; i < root->no_keys; i++)
		abtree_print_rec(root->children[i], level + 1);
	if (root->no_keys > 0)
		abtree_print_rec(root->children[root->no_keys], level + 1);
}

static void abtree_print(abtree_t *abtree)
{
	if (!abtree) {
		printf("Empty tree\n");
		return;
	}

	abtree_print_rec(abtree->root, 0);
}

int bst_violations, total_nodes, total_keys, leaf_keys;
int null_children_violations;
int not_full_nodes;
int leaves_level;
int leaves_at_same_level;
static void abtree_node_validate(abtree_node_t *n, map_key_t min, map_key_t max,
                                 abtree_t *abtree)
{
	int i;
	map_key_t cur_min;
	KEY_COPY(cur_min, n->keys[0]);

	if (n != abtree->root && n->no_keys < ABTREE_DEGREE_MIN)
		not_full_nodes++;

	for (i=1; i < n->no_keys; i++)
		if (KEY_CMP(n->keys[i], cur_min) <= 0)
			bst_violations++;

	if (KEY_CMP(n->keys[0], min) < 0 || KEY_CMP(n->keys[n->no_keys-1], max) >= 0)
		bst_violations++;

	if (!n->leaf)
		for (i=0; i <= n->no_keys; i++)
			if (!n->children[i])
				null_children_violations++;
}

static void abtree_validate_rec(abtree_node_t *root, map_key_t min, map_key_t max,
                                abtree_t *abtree, int level)
{
	int i;

	if (!root) return;

	total_nodes++;
	total_keys += root->no_keys;

	abtree_node_validate(root, min, max, abtree);
	
	if (root->leaf) {
		if (leaves_level == -1)
			leaves_level = level;
		else if (level != leaves_level)
				leaves_at_same_level = 0;
		leaf_keys += root->no_keys;
		return;
	}

	for (i=0; i <= root->no_keys; i++)
		abtree_validate_rec(root->children[i],
		                   i == 0 ? min : root->keys[i-1],
		                   i == root->no_keys ? max : root->keys[i],
		                   abtree, level+1);
}

static int abtree_validate_helper(abtree_t *abtree)
{
	int check_bst = 0, check_abtree_properties = 0;
	bst_violations = 0;
	total_nodes = 0;
	total_keys = leaf_keys = 0;
	null_children_violations = 0;
	not_full_nodes = 0;
	leaves_level = -1;
	leaves_at_same_level = 1;

	abtree_validate_rec(abtree->root, MIN_KEY, MAX_KEY, abtree, 0);

	check_bst = (bst_violations == 0);
	check_abtree_properties = (null_children_violations == 0) &&
	                         (not_full_nodes == 0) &&
	                         (leaves_at_same_level == 1);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  BTREE Violation: %s\n",
	       check_abtree_properties ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- NULL Children Violation: %s\n",
	       (null_children_violations == 0) ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- Not-full Nodes: %s\n",
	       (not_full_nodes == 0) ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- Leaves at same level: %s [ Level %d ]\n",
	       (leaves_at_same_level == 1) ? "Yes [OK]" : "No [ERROR]", leaves_level);
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
	printf("\n");

	return check_bst && check_abtree_properties;
}

/******************************************************************************/
/* Red-Black tree interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(abtree_node_t));
	return abtree_new();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(abtree_node_t));
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
	pthread_spin_lock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((abtree_t *)map)->lock);
#	endif

	ret = abtree_lookup(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((abtree_t *)map)->lock);
#	endif

	return ret; 
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	return 0;
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((abtree_t *)map)->lock);
#	endif

	ret = abtree_insert(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((abtree_t *)map)->lock);
#	endif
	return ret;
}

int map_delete(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((abtree_t *)map)->lock);
#	endif

	ret = abtree_delete(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((abtree_t *)map)->lock);
#	endif

	return ret;
}

int map_update(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((abtree_t *)map)->lock);
#	endif

	ret = abtree_update(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((abtree_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((abtree_t *)map)->lock);
#	endif
	return ret;
}

int map_validate(void *map)
{
	int ret = 0;
	ret = abtree_validate_helper(map);
	return ret;
}

char *map_name()
{
#	if defined(SYNC_CG_SPINLOCK)
	return "abtree-cg-lock";
#	elif defined(SYNC_CG_HTM)
	return "abtree-cg-htm";
#	else
	return "abtree-sequential";
#	endif
}
