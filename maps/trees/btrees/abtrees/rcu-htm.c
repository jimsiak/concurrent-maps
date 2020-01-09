#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h> //> pthread_spinlock_t

#include "alloc.h"
#include "htm/htm.h"
#include "ht.h"
#include "../../../map.h"
#include "../../../rcu-htm/tdata.h"
#include "../../../key/key.h"

#define ABTREE_DEGREE_MAX 16
#define ABTREE_DEGREE_MIN 6
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
	pthread_spinlock_t lock;
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

static abtree_node_t *abtree_node_new_copy(abtree_node_t *n)
{
	abtree_node_t *ret = abtree_node_new(n->leaf);
	memcpy(ret, n, sizeof(*n));
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

static int abtree_node_get_index(abtree_node_t *n, map_key_t key)
{
	int index = abtree_node_search(n, key);
	if (index < n->no_keys && KEY_CMP(n->keys[index], key) == 0) index++;
	return index;
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

	for (i=0; i < n->no_keys; i++) KEY_PRINT(n->keys[i], " ", " |");
	printf("]");
	printf("%s - %s\n", n->leaf ? " LEAF" : "", n->tag ? " TAGGED" : "");
}

static abtree_t *abtree_new()
{
	abtree_t *ret;
	XMALLOC(ret, 1);
	ret->root = NULL;
	pthread_spin_init(&ret->lock, PTHREAD_PROCESS_SHARED);
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
		if (index < n->no_keys && KEY_CMP(n->keys[index], key) == 0) index++;
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

static abtree_node_t *abtree_join_parent_with_child(abtree_node_t *p, int pindex,
                                                    abtree_node_t *l, tdata_t *tdata)
{
	int new_no_keys = p->no_keys + l->no_keys;
	int i;
	abtree_node_t *p_cp = abtree_node_new(0);
	int k1 = 0, k2 = 0;

	//> copy p keys and children until pindex first
	for (i=0; i < pindex; i++) {
		KEY_COPY(p_cp->keys[k1++], p->keys[i]);
		p_cp->children[k2++] = p->children[i];
		if (tdata) ht_insert(tdata->ht, &p->children[i], p_cp->children[k2-1]);
	}

	//> copy l keys and children then
	for (i=0; i < l->no_keys; i++) {
		KEY_COPY(p_cp->keys[k1++], l->keys[i]);
		p_cp->children[k2++] = l->children[i];
		if (tdata) ht_insert(tdata->ht, &l->children[i], p_cp->children[k2-1]);
	}
	p_cp->children[k2++] = l->children[l->no_keys];
	if (tdata) ht_insert(tdata->ht, &l->children[l->no_keys], p_cp->children[k2-1]);

	// finally copy the rest of p
	for (i=pindex; i < p->no_keys; i++)
		KEY_COPY(p_cp->keys[k1++], p->keys[i]);
	for (i=pindex+1; i < p->no_keys; i++) {
		p_cp->children[k2++] = p->children[i];
		if (tdata) ht_insert(tdata->ht, &p->children[i], p_cp->children[k2-1]);
	}
	p_cp->children[k2] = p->children[p->no_keys];
	if (tdata) ht_insert(tdata->ht, &p->children[p->no_keys], p_cp->children[k2]);

	p_cp->no_keys = new_no_keys;
	p_cp->tag = 0;
	p_cp->leaf = 0;
	return p_cp;
}

static abtree_node_t *abtree_split_parent_and_child(abtree_node_t *p, int pindex,
                                                    abtree_node_t *l, tdata_t *tdata)
{
	int i, k1 = 0, k2 = 0;
	map_key_t keys[ABTREE_DEGREE_MAX * 2];
	void *ptrs[ABTREE_DEGREE_MAX * 2 + 1];

	//> Get all keys
	for (i=0; i < pindex; i++)          KEY_COPY(keys[k1++], p->keys[i]);
	for (i=0; i < l->no_keys; i++)      KEY_COPY(keys[k1++], l->keys[i]);
	for (i=pindex; i < p->no_keys; i++) KEY_COPY(keys[k1++], p->keys[i]);
	//> Get all pointers
	for (i=0; i < pindex; i++) {
		ptrs[k2++] = p->children[i];
		if (tdata) ht_insert(tdata->ht, &p->children[i], ptrs[k2-1]);
	}
	for (i=0; i <= l->no_keys; i++) {
		ptrs[k2++] = l->children[i];
		if (tdata) ht_insert(tdata->ht, &l->children[i], ptrs[k2-1]);
	}
	for (i=pindex+1; i <= p->no_keys; i++) {
		ptrs[k2++] = p->children[i];
		if (tdata) ht_insert(tdata->ht, &p->children[i], ptrs[k2-1]);
	}

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

	//> save the key for the parent
	map_key_t pkey;
	KEY_COPY(pkey, keys[k1++]);

	//> Create the new right node
	abtree_node_t *new_right = abtree_node_new(0);
	for (i=0; i < rightsz; i++) {
		KEY_COPY(new_right->keys[i], keys[k1++]);
		new_right->children[i] = ptrs[k2++];
	}
	new_right->children[rightsz] = ptrs[k2++];
	new_right->tag = 0;
	new_right->no_keys = rightsz;

	//> Create the new parent
	abtree_node_t *newp = abtree_node_new(0);
	KEY_COPY(newp->keys[0], pkey);
	newp->children[0] = new_left;
	newp->children[1] = new_right;
	// FIXME tag parent here?? or outside this function??
	newp->no_keys = 1;
	return newp;
}

static abtree_node_t *abtree_join_siblings(abtree_node_t *p, abtree_node_t *l,
                                           abtree_node_t *s, int lindex, int sindex,
                                           tdata_t *tdata)
{
	int left_index, right_index;
	abtree_node_t *left, *right;
	int i, k1, k2;

	left_index  = lindex < sindex ? lindex : sindex;
	right_index = lindex < sindex ? sindex : lindex;
	left  = p->children[left_index];
	right = p->children[right_index];
	if (tdata) ht_insert(tdata->ht, &p->children[left_index], left);
	if (tdata) ht_insert(tdata->ht, &p->children[right_index], right);

	//> Create the new node
	abtree_node_t *new_node = abtree_node_new(left->leaf);
	k1 = k2 = 0;
	for (i=0; i < left->no_keys; i++) {
		KEY_COPY(new_node->keys[k1++], left->keys[i]);
		new_node->children[k2++] = left->children[i];
		if (tdata) ht_insert(tdata->ht, &left->children[i], new_node->children[k2-1]);
	}
	new_node->children[k2++] = left->children[left->no_keys];
	if (tdata) ht_insert(tdata->ht, &left->children[left->no_keys], new_node->children[k2-1]);

	if (!left->leaf) KEY_COPY(new_node->keys[k1++], p->keys[left_index]);
	for (i=0; i < right->no_keys; i++)
		KEY_COPY(new_node->keys[k1++], right->keys[i]);
	for (i = (left->leaf) ? 1 : 0; i < right->no_keys; i++) {
		new_node->children[k2++] = right->children[i];
		if (tdata) ht_insert(tdata->ht, &right->children[i], new_node->children[k2-1]);
	}
	new_node->children[k2++] = right->children[right->no_keys];
	if (tdata) ht_insert(tdata->ht, &right->children[right->no_keys], new_node->children[k2-1]);
	new_node->tag = 0;
	new_node->no_keys = k1;

	//> Create the new parent
	abtree_node_t *newp = abtree_node_new(0);
	for (i=0; i < left_index; i++) {
		KEY_COPY(newp->keys[i], p->keys[i]);
		newp->children[i] = p->children[i];
		if (tdata) ht_insert(tdata->ht, &p->children[i], newp->children[i]);
	}
	newp->children[left_index] = new_node;
	//> Fix the parent
	for (i=left_index + 1; i < p->no_keys; i++) {
		KEY_COPY(newp->keys[i-1], p->keys[i]);
		newp->children[i] = p->children[i+1];
		if (tdata) ht_insert(tdata->ht, &p->children[i+1], newp->children[i]);
	}
	newp->tag = 0;
	newp->no_keys = p->no_keys - 1;
	return newp;
}

static abtree_node_t *abtree_redistribute_sibling_keys(abtree_node_t *p, abtree_node_t *l,
                                                       abtree_node_t *s, int lindex,
                                                       int sindex, tdata_t *tdata)
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
	if (tdata) ht_insert(tdata->ht, &p->children[left_index], left);
	if (tdata) ht_insert(tdata->ht, &p->children[right_index], right);

	//> Gather all keys in keys array
	for (i=0; i < left->no_keys; i++) {
		KEY_COPY(keys[k1++], left->keys[i]);
		ptrs[k2++] = left->children[i];
		if (tdata) ht_insert(tdata->ht, &left->children[i], ptrs[k2-1]);
	}
	ptrs[k2++] = left->children[left->no_keys];
	if (tdata) ht_insert(tdata->ht, &left->children[left->no_keys], ptrs[k2-1]);
	if (!left->leaf) KEY_COPY(keys[k1++], p->keys[left_index]);
	for (i=0; i < right->no_keys; i++)
		KEY_COPY(keys[k1++], right->keys[i]);
	for (i = (left->leaf) ? 1 : 0; i < right->no_keys; i++) {
		ptrs[k2++] = right->children[i];
		if (tdata) ht_insert(tdata->ht, &right->children[i], ptrs[k2-1]);
	}
	ptrs[k2++] = right->children[right->no_keys];
	if (tdata) ht_insert(tdata->ht, &right->children[right->no_keys], ptrs[k2-1]);

	//> Calculate new number of keys in left and right
	total_keys = k1;
	left_keys = k1 / 2;
	right_keys = total_keys - left_keys;
	if (!left->leaf) right_keys--; // If not leaf one key goes to parent

	//> Fix left
	abtree_node_t *new_left = abtree_node_new(left->leaf);
	k1 = k2 = 0;
	for (i=0; i < left_keys; i++) {
		KEY_COPY(new_left->keys[i], keys[k1++]);
		new_left->children[i] = ptrs[k2++];
	}
	new_left->children[left_keys] = ptrs[k2++];
	new_left->no_keys = left_keys;

	//> Keep parents key
	map_key_t pkey;
	KEY_COPY(pkey, keys[k1]);
	if (!left->leaf) k1++; 

	//> Fix right
	abtree_node_t *new_right = abtree_node_new(right->leaf);
	for (i=0; i < right_keys; i++)
		KEY_COPY(new_right->keys[i], keys[k1++]);
	for (i = (new_left->leaf) ? 1 : 0; i < right_keys; i++)
		new_right->children[i] = ptrs[k2++];
	new_right->children[right_keys] = ptrs[k2++];
	new_right->no_keys = right_keys;

	//> Fix parent
	abtree_node_t *newp = abtree_node_new(0);
	for (i=0; i < p->no_keys; i++) {
		KEY_COPY(newp->keys[i], p->keys[i]);
		newp->children[i] = p->children[i];
		if (tdata) ht_insert(tdata->ht, &p->children[i], newp->children[i]);
	}
	newp->children[p->no_keys] = p->children[p->no_keys];
	if (tdata) ht_insert(tdata->ht, &p->children[p->no_keys], newp->children[p->no_keys]);
	newp->no_keys = p->no_keys;
	KEY_COPY(newp->keys[left_index], pkey);
	newp->children[left_index] = new_left;
	newp->children[right_index] = new_right;
	return newp;
}

static void abtree_rebalance(abtree_t *abtree, map_key_t key, int *should_rebalance)
{
	abtree_node_t *gp, *p, *l, *s;
	int i = 0, gpindex, pindex, sindex;
	abtree_node_t *copy = NULL;

	*should_rebalance = 0;

	//> Root is a leaf, so nothing needs to be done
	if (abtree->root->leaf) return;

	gp = NULL;
	gpindex = -1;
	p = abtree->root;
	pindex = abtree_node_get_index(p, key);
	l = p->children[pindex];
	while (!l->leaf && !l->tag && l->no_keys >= ABTREE_DEGREE_MIN) {
		gp = p;
		gpindex = pindex;
		p = l;
		pindex = abtree_node_get_index(p, key);
		l = p->children[pindex];
	}

	//> No violation to fix
	if (!l->tag && l->no_keys >= ABTREE_DEGREE_MIN) return;

	if (l->tag) {
		if (p->no_keys + l->no_keys <= ABTREE_DEGREE_MAX) {
			//> Join l with its parent
			copy = abtree_join_parent_with_child(p, pindex, l, NULL);
		} else {
			//> Split child and parent
			copy = abtree_split_parent_and_child(p, pindex, l, NULL);
			copy->tag = (gp != NULL); //> Tag parent if not root
			*should_rebalance = copy->tag;
		}
	} else if (l->no_keys < ABTREE_DEGREE_MIN) {
		sindex = pindex ? pindex - 1 : pindex + 1;
		s = p->children[sindex];
		if (s->tag) {
			//> FIXME
		} else {
			if (l->no_keys + s->no_keys + 1 <= ABTREE_DEGREE_MAX) {
				//> Join l and s
				copy = abtree_join_siblings(p, l, s, pindex, sindex, NULL);
				*should_rebalance = (gp != NULL && copy->no_keys < ABTREE_DEGREE_MIN);
			} else {
				//> Redistribute keys between s and l
				copy = abtree_redistribute_sibling_keys(p, l, s, pindex, sindex, NULL);
			}
		}
	} else {
		assert(0);
	}

	if (copy) {
		if (gp == NULL) abtree->root = copy;
		else            gp->children[gpindex] = copy;
	}
}

static void abtree_traverse_for_rebalance(abtree_t *abtree, map_key_t key,
                    int *should_rebalance, abtree_node_t **node_stack,
					int *node_stack_indexes, int *stack_top)
{
	abtree_node_t *gp, *p, *l, *s;
	int i = 0, gpindex, pindex, sindex;

	*stack_top = -1;
	*should_rebalance = 0;

	if (abtree->root->leaf) return;

	gp = NULL;
	gpindex = -1;
	p = abtree->root;
	pindex = abtree_node_get_index(p, key);
	node_stack[++(*stack_top)] = p;
	node_stack_indexes[*stack_top] = pindex;
	l = p->children[pindex];
	while (!l->leaf && !l->tag && l->no_keys >= ABTREE_DEGREE_MIN) {
		gp = p;
		gpindex = pindex;
		p = l;
		pindex = abtree_node_get_index(p, key);
		node_stack[++(*stack_top)] = p;
		node_stack_indexes[*stack_top] = pindex;
		l = p->children[pindex];
	}
	node_stack[++(*stack_top)] = l;

	//> No violation to fix
	if (!l->tag && l->no_keys >= ABTREE_DEGREE_MIN) return;

	*should_rebalance = 1;
}

static abtree_node_t *abtree_rebalance_with_copy(abtree_t *abtree, map_key_t key,
                                   abtree_node_t **node_stack,
                                   int *node_stack_indexes, int stack_top,
                                   int *should_rebalance, abtree_node_t **copy,
                                   int *connection_point_stack_index,
                                   tdata_t *tdata)
{
	abtree_node_t *gp, *p, *l, *s;
	int gpindex, pindex, sindex;

	*should_rebalance = 0;
	*copy = NULL;
	*connection_point_stack_index = -1;
	gp = (stack_top >= 2) ? node_stack[stack_top-2] : NULL;
	gpindex = (stack_top >= 2) ? node_stack_indexes[stack_top-2] : -1;
	p  = node_stack[stack_top-1];
	pindex = node_stack_indexes[stack_top-1];
	l  = node_stack[stack_top];

	if (l->tag) {
		if (p->no_keys + l->no_keys <= ABTREE_DEGREE_MAX) {
			//> Join l with its parent
			*copy = abtree_join_parent_with_child(p, pindex, l, tdata);
		} else {
			//> Split child and parent
			*copy = abtree_split_parent_and_child(p, pindex, l, tdata);
			(*copy)->tag = (gp != NULL); //> Tag parent if not root
			*should_rebalance = (*copy)->tag;
		}
	} else if (l->no_keys < ABTREE_DEGREE_MIN) {
		sindex = pindex ? pindex - 1 : pindex + 1;
		s = p->children[sindex];
		ht_insert(tdata->ht, &p->children[sindex], s);
		if (s->tag) {
			//> FIXME
		} else {
			if (l->no_keys + s->no_keys + 1 <= ABTREE_DEGREE_MAX) {
				//> Join l and s
				*copy = abtree_join_siblings(p, l, s, pindex, sindex, tdata);
				*should_rebalance = (gp != NULL && (*copy)->no_keys < ABTREE_DEGREE_MIN);
			} else {
				//> Redistribute keys between s and l
				*copy = abtree_redistribute_sibling_keys(p, l, s, pindex, sindex, tdata);
			}
		}
	} else {
		assert(0);
	}

	*connection_point_stack_index = stack_top - 2;
	return stack_top > 1 ? node_stack[stack_top-2] : NULL;
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

	//> Case of a full leaf.
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

static int abtree_insert(abtree_t *abtree, map_key_t key, void *val, void *tdata)
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
		abtree_rebalance(abtree, key, &should_rebalance);
	return ret;
}

static abtree_node_t *abtree_do_delete_with_copy(abtree_t *abtree, map_key_t key, 
                           abtree_node_t **node_stack,
                           int *node_stack_indexes, int node_stack_top,
                           int *should_rebalance, abtree_node_t **copy,
                           int *connection_point_stack_index)
{
	abtree_node_t *n_cp = abtree_node_new_copy(node_stack[node_stack_top]);
	int index = node_stack_indexes[node_stack_top];

	abtree_node_delete_index(n_cp, index);
	*should_rebalance = n_cp->no_keys < ABTREE_DEGREE_MIN;
	*copy = n_cp;
	*connection_point_stack_index = node_stack_top - 1;
	return node_stack_top > 0 ? node_stack[node_stack_top-1] : NULL;
}

static int abtree_delete(abtree_t *abtree, map_key_t key, void *tdata)
{
	int index;
	abtree_node_t *n, *tree_cp_root, *connection_point;
	abtree_node_t *node_stack[MAX_HEIGHT];
	int node_stack_indexes[MAX_HEIGHT];
	int node_stack_top = -1, should_rebalance, connection_point_stack_index;

	//> Route to the appropriate leaf.
	abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes,
	                     &node_stack_top);

	//> Empty tree case.
	if (node_stack_top == -1)
		return 0;
	n = node_stack[node_stack_top];
	index = node_stack_indexes[node_stack_top];
	//> Key not in the tree.
	if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0)
		return 0;
	//> Key in the tree.
	connection_point = abtree_do_delete_with_copy(abtree, key, node_stack,
	                                         node_stack_indexes, node_stack_top,
	                                         &should_rebalance, &tree_cp_root,
	                                         &connection_point_stack_index);
	//> Install copy
	if (connection_point == NULL) {
		abtree->root = tree_cp_root;
	} else {
		int index = node_stack_indexes[connection_point_stack_index];
		connection_point->children[index] = tree_cp_root;
	}

	while (should_rebalance)
		abtree_rebalance(abtree, key, &should_rebalance);
	return 1;
}

static abtree_node_t *abtree_do_insert_with_copy(abtree_t *abtree,
                           map_key_t key, void *val,
                           abtree_node_t **node_stack, int *node_stack_indexes,
                           int node_stack_top, int *should_rebalance,
						   abtree_node_t **copy,
                           int *connection_point_stack_index)
{
	abtree_node_t *n_cp;
	int index, pindex;

	*should_rebalance = 0;

	//> Empty tree case.
	if (node_stack_top == -1) {
		n_cp = abtree_node_new(1);
		abtree_node_insert_index(n_cp, 0, key, val);
		*copy = n_cp;
		*connection_point_stack_index = -1;
		return NULL;
	}

	n_cp = abtree_node_new_copy(node_stack[node_stack_top]);
	index = node_stack_indexes[node_stack_top];
	if (n_cp->no_keys < ABTREE_DEGREE_MAX) {
		//> Case of a not full leaf.
		abtree_node_insert_index(n_cp, index, key, val);
		*copy = n_cp;
	} else {
		//> Case of a full leaf.
		abtree_node_t *rnode = abtree_leaf_split(n_cp, index, key, val);
		abtree_node_t *parent_new = abtree_node_new(0);
		abtree_node_insert_index(parent_new, 0, rnode->keys[0], rnode);
		parent_new->children[0] = n_cp;
		parent_new->tag = node_stack_top > 0; //> Not tagged if root
	
		*should_rebalance = 1;
		*copy = parent_new;
	}
	*connection_point_stack_index = node_stack_top - 1;
	return node_stack_top > 0 ? node_stack[node_stack_top-1] : NULL;
}

static int abtree_update(abtree_t *abtree, map_key_t key, void *val, tdata_t *tdata)
{
	tm_begin_ret_t status;
	abtree_node_t *node_stack[MAX_HEIGHT];
	int node_stack_indexes[MAX_HEIGHT], stack_top = -1;
	int op_is_insert = -1, should_rebalance, ret;
	int connection_point_stack_index, index;
	int retries = -1;
	abtree_node_t *tree_cp_root, *connection_point;

try_from_scratch:

	ht_reset(tdata->ht);

	if (++retries >= TX_NUM_RETRIES) {
		tdata->lacqs++;
		pthread_spin_lock(&abtree->lock);
		abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes, &stack_top);
		if (stack_top == -1) {
			op_is_insert = 1;
		} else {
			index = node_stack_indexes[stack_top];
			abtree_node_t *n = node_stack[stack_top];
			if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0)
				op_is_insert = 1;
			else if (index < ABTREE_DEGREE_MAX && KEY_CMP(key, n->keys[index]) == 0)
				op_is_insert = 0;
		}

		if (op_is_insert && stack_top >= 0 &&
		    node_stack_indexes[stack_top] < ABTREE_DEGREE_MAX &&
		    KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0) {
			pthread_spin_unlock(&abtree->lock);
			return 0;
		} else if (!op_is_insert && (stack_top < 0 ||
		            node_stack_indexes[stack_top] >= node_stack[stack_top]->no_keys ||
					KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) != 0)) {
			pthread_spin_unlock(&abtree->lock);
			return 2;
		}

		if (op_is_insert) {
			connection_point = abtree_do_insert_with_copy(abtree, key, val, node_stack,
			                       node_stack_indexes, stack_top,
			                       &should_rebalance, &tree_cp_root,
			                       &connection_point_stack_index);
			ret = 1;
		} else {
			connection_point = abtree_do_delete_with_copy(abtree, key,
			                       node_stack, node_stack_indexes,
			                       stack_top, &should_rebalance,
			                       &tree_cp_root, &connection_point_stack_index);
			ret = 3;
		}
		if (connection_point == NULL) {
			abtree->root = tree_cp_root;
		} else {
			index = node_stack_indexes[connection_point_stack_index];
			connection_point->children[index] = tree_cp_root;
		}

		//> FIXME is this "correct" (performance-wise) to be here??
		while (should_rebalance)
			abtree_rebalance(abtree, key, &should_rebalance);
		pthread_spin_unlock(&abtree->lock);
		return ret;
	}

	//> Asynchronized traversal. 
	abtree_traverse_stack(abtree, key, node_stack, node_stack_indexes, &stack_top);
	if (stack_top == -1) {
		op_is_insert = 1;
	} else {
		index = node_stack_indexes[stack_top];
		abtree_node_t *n = node_stack[stack_top];
		if (index >= n->no_keys || KEY_CMP(key, n->keys[index]) != 0)
			op_is_insert = 1;
		else if (index < ABTREE_DEGREE_MAX && KEY_CMP(key, n->keys[index]) == 0)
			op_is_insert = 0;
	}
	if (op_is_insert && stack_top >= 0 &&
	    node_stack_indexes[stack_top] < ABTREE_DEGREE_MAX &&
	    KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) == 0) {
		return 0;
	} else if (!op_is_insert && (stack_top < 0 ||
	            node_stack_indexes[stack_top] >= node_stack[stack_top]->no_keys ||
				KEY_CMP(node_stack[stack_top]->keys[node_stack_indexes[stack_top]], key) != 0)) {
		pthread_spin_unlock(&abtree->lock);
		return 2;
	}

	if (op_is_insert) {
		connection_point = abtree_do_insert_with_copy(abtree, key, val, node_stack,
		                       node_stack_indexes, stack_top,
		                       &should_rebalance, &tree_cp_root,
		                       &connection_point_stack_index);
		ret = 1;
	} else {
		connection_point = abtree_do_delete_with_copy(abtree, key,
		                       node_stack, node_stack_indexes,
		                       stack_top, &should_rebalance,
		                       &tree_cp_root, &connection_point_stack_index);
		ret = 3;
	}

	int validation_retries = -1;
validate_and_connect_copy:

	if (++validation_retries >= TX_NUM_RETRIES) goto try_from_scratch;
	while (abtree->lock != LOCK_FREE) ;

	tdata->tx_starts++;
	status = TX_BEGIN(0);
	if (status == TM_BEGIN_SUCCESS) {
		if (abtree->lock != LOCK_FREE)
			TX_ABORT(ABORT_GL_TAKEN);

		//> Validate copy
		if (stack_top < 0 && abtree->root != NULL)
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		if (stack_top >= 0 && abtree->root != node_stack[0])
			TX_ABORT(ABORT_VALIDATION_FAILURE);
		int i;
		abtree_node_t *n1, *n2;
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
				abtree_node_t **np = tdata->ht->entries[i][j];
				abtree_node_t  *n  = tdata->ht->entries[i][j+1];
				if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
			}
		}

		// Now let's 'commit' the tree copy onto the original tree.
		if (connection_point == NULL) {
			abtree->root = tree_cp_root;
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

	//> FIXME
	while (should_rebalance) {

		ht_reset(tdata->ht);

		abtree_traverse_for_rebalance(abtree, key, &should_rebalance, node_stack,
                              node_stack_indexes, &stack_top);
		if (!should_rebalance) break;
		connection_point = abtree_rebalance_with_copy(abtree, key,
		                           node_stack, node_stack_indexes,
		                           stack_top, &should_rebalance, &tree_cp_root,
		                           &connection_point_stack_index, tdata);
	
		while (1) {
			status = TX_BEGIN(0);
			if (status == TM_BEGIN_SUCCESS) {
				if (abtree->lock != LOCK_FREE)
					TX_ABORT(ABORT_GL_TAKEN);

				//> Validate copy
				if (stack_top < 0 && abtree->root != NULL)
					TX_ABORT(ABORT_VALIDATION_FAILURE);
				if (stack_top >= 0 && abtree->root != node_stack[0])
					TX_ABORT(ABORT_VALIDATION_FAILURE);
				int i;
				abtree_node_t *n1, *n2;
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
						abtree_node_t **np = tdata->ht->entries[i][j];
						abtree_node_t  *n  = tdata->ht->entries[i][j+1];
						if (*np != n) TX_ABORT(ABORT_VALIDATION_FAILURE);
					}
				}

				if (tree_cp_root != NULL) {
					if (connection_point == NULL) {
						abtree->root = tree_cp_root;
					} else {
						index = node_stack_indexes[connection_point_stack_index];
						connection_point->children[index] = tree_cp_root;
					}
				}

				TX_END(0);
				break;
			} else {
				tdata->tx_aborts++;
				if (ABORT_IS_EXPLICIT(status) && 
				    ABORT_CODE(status) == ABORT_VALIDATION_FAILURE) {
					tdata->tx_aborts_explicit_validation++;
					should_rebalance = 1;
					break;
				}
			}
		}
	}

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
	map_key_t cur_min = n->keys[0];

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
		if (leaves_level == -1)         leaves_level = level;
		else if (level != leaves_level) leaves_at_same_level = 0;
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
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(abtree_node_t));
	return abtree_new();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(abtree_node_t));
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

int map_lookup(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = abtree_lookup(map, key);
	return ret; 
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	return 0;
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = abtree_insert(map, key, value, thread_data);
	return ret;
}

int map_delete(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = abtree_delete(map, key, thread_data);
	return ret;
}

int map_update(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = abtree_update(map, key, value, thread_data);
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
	return "abtree-rcu-htm";
}
