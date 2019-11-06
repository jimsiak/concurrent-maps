#include <stdio.h>
#include <stdlib.h>
#include "stack.h"
#include "ca-locks.h"
#include "tdata.h"
#include "../key/key.h"

static int ca_lookup(ca_t *ca, map_key_t key, ca_tdata_t *tdata)
{
	int ret = 0;
	base_node_t *bnode;
	route_node_t *parent, *gparent;

	while (1) {
		bnode = _get_base_node(ca, &parent, &gparent, key);
		ca_node_base_lock(bnode);
		if (!bnode->valid) {
			ca_node_base_unlock(bnode);
			continue;
		}
		ret = seq_ds_lookup(bnode->root, key);
		ca_adapt_if_needed(ca, bnode, parent, gparent, tdata);
		ca_node_base_unlock(bnode);
		return ret;
	}
}

static int ca_insert(ca_t *ca, map_key_t key, void *value, ca_tdata_t *tdata)
{
	int ret = 0;
	base_node_t *bnode;
	route_node_t *parent, *gparent;

	while (1) {
		bnode = _get_base_node(ca, &parent, &gparent, key);
		ca_node_base_lock(bnode);
		if (!bnode->valid) {
			ca_node_base_unlock(bnode);
			continue;
		}
		ret = seq_ds_insert(bnode->root, key, value);
		ca_adapt_if_needed(ca, bnode, parent, gparent, tdata);
		ca_node_base_unlock(bnode);
		return ret;
	}
}

static int ca_update(ca_t *ca, map_key_t key, void *value, ca_tdata_t *tdata)
{
	int ret = 0;
	base_node_t *bnode;
	route_node_t *parent, *gparent;

	while (1) {
		bnode = _get_base_node(ca, &parent, &gparent, key);
		ca_node_base_lock(bnode);
		if (!bnode->valid) {
			ca_node_base_unlock(bnode);
			continue;
		}
		ret = seq_ds_update(bnode->root, key, value);
		ca_adapt_if_needed(ca, bnode, parent, gparent, tdata);
		ca_node_base_unlock(bnode);
		return ret;
	}
}

static int ca_delete(ca_t *ca, map_key_t key, ca_tdata_t *tdata)
{
	int ret = 0;
	base_node_t *bnode;
	route_node_t *parent, *gparent;

	while (1) {
		bnode = _get_base_node(ca, &parent, &gparent, key);
		ca_node_base_lock(bnode);
		if (!bnode->valid) {
			ca_node_base_unlock(bnode);
			continue;
		}
		ret = seq_ds_delete(bnode->root, key);
		ca_adapt_if_needed(ca, bnode, parent, gparent, tdata);
		ca_node_base_unlock(bnode);
		return ret;
	}
}

static __thread stack_t access_path;
static __thread base_node_t *rquery_bnodes[100];

/**
 * For a range query of [key1, key2] returns (locked) all the base nodes involved.
 * Returns the number of base nodes, or -1 if some of the base nodes was invalid.
 */
static int _rquery_get_base_nodes(ca_t *ca, map_key_t key1, map_key_t key2)
{
	base_node_t *bnode;
	route_node_t *rnode;
	void *curr, *prev;
	int nbase_nodes = 0, i;

	stack_reset(&access_path);

	_get_base_node_stack(ca, &access_path, key1);
	curr = stack_pop(&access_path);
	prev = curr;

	while (curr != NULL) {
		if (ca_node_is_route(curr)) {
			rnode = curr;
			if (prev != rnode->left && prev != rnode->right) {
				//> None of the two children have been examined, go to left
				curr = rnode->left;
				stack_push(&access_path, rnode);
			} else if (rnode->left == prev) {
				//> Previous examined node was its left child, go to right
				curr = rnode->right;
				stack_push(&access_path, rnode);
			} else {
				//> Both children have already been examined, go upwards
				prev = curr;
				curr = stack_pop(&access_path);
			}
		} else {
			bnode = curr;
			if (pthread_spin_trylock(&bnode->lock)) goto out_with_valid_error;
			rquery_bnodes[nbase_nodes++] = bnode;
			if (!bnode->valid) goto out_with_valid_error;
			if (bnode->root->root != NULL && seq_ds_max_key(bnode->root) >= key2) break;
			prev = curr;
			curr = stack_pop(&access_path);
		}
	}
	return nbase_nodes;

out_with_valid_error:
	for (i=0; i < nbase_nodes; i++)
		ca_node_base_unlock(rquery_bnodes[i]);
	return -1;
}

static int ca_rquery(ca_t *ca, map_key_t key1, map_key_t key2, int *nkeys,
                     ca_tdata_t *tdata)
{
	int nbase_nodes, i;

	do {
		nbase_nodes = _rquery_get_base_nodes(ca, key1, key2);
	} while (nbase_nodes == -1);

	for (i=0; i < nbase_nodes; i++)
		ca_node_base_unlock(rquery_bnodes[i]);

	return (nbase_nodes > 0);
}

static int bst_violations;
static int total_nodes, route_nodes, base_nodes, invalid_nodes;
static int total_keys, base_keys;
static int max_depth, min_depth;
static int max_seq_ds_size, min_seq_ds_size;

static void ca_validate_rec(void *root, map_key_t min, map_key_t max,
                                  int depth)
{
	route_node_t *rnode;
	base_node_t *bnode;
	int sz;

	total_nodes++;
	total_keys++;

	if (ca_node_is_route(root)) {
		rnode = root;
		route_nodes++;
		invalid_nodes += (rnode->valid == 0);
		ca_validate_rec(rnode->left, min, rnode->key, depth+1);
		ca_validate_rec(rnode->right, rnode->key, max, depth+1);
	} else {
		bnode = root;
		base_nodes++;
		invalid_nodes += (bnode->valid == 0);
		//> FIXME don't access bnode->root->root directly below
		if (bnode->root->root != NULL && seq_ds_max_key(bnode->root) > max) bst_violations++;
		if (bnode->root->root != NULL && seq_ds_min_key(bnode->root) < min) bst_violations++;
		if (depth < min_depth) min_depth = depth;
		if (depth > max_depth) max_depth = depth;
		sz = seq_ds_size(bnode->root);
		base_keys += sz;
		if (sz < min_seq_ds_size) min_seq_ds_size = sz;
		if (sz > max_seq_ds_size) max_seq_ds_size = sz;
	}
}

int ca_validate_helper(ca_t *ca)
{
	int check_bst = 0;
	bst_violations = 0;
	total_nodes = route_nodes = base_nodes = invalid_nodes = 0;
	total_keys = base_keys = 0;
	min_depth = 100000;
	max_depth = -1;
	min_seq_ds_size = 9999999;
	max_seq_ds_size = -1;

	if (ca->root) ca_validate_rec(ca->root, MIN_KEY, MAX_KEY, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Invalid nodes: %d %s\n", invalid_nodes,
	       invalid_nodes == 0 ? "[OK]" : "[ERROR]");
	printf("  Tree size: %8d ( %8d route / %8d base )\n",
	       total_nodes, route_nodes, base_nodes);
	printf("  Number of keys: %8d total / %8d in base nodes\n", total_keys,
	                                                            base_keys);
	printf("  Depth (min/max): %d / %d\n", min_depth, max_depth);
	printf("  Sequential Data Structures Sizes (min/max): %d / %d\n",
	          min_seq_ds_size, max_seq_ds_size);
	printf("\n");

	return check_bst;
}

/******************************************************************************/
/*     Map interface implementation                                           */
/******************************************************************************/
void *map_new()
{
	printf("Size of CA node is %lu (route) and %lu (base)\n",
	        sizeof(route_node_t), sizeof(base_node_t));
	return ca_new();
}

void *map_tdata_new(int tid)
{
	nalloc_internal = nalloc_thread_init(tid, sizeof(treap_node_internal_t));
	nalloc_external = nalloc_thread_init(tid, sizeof(treap_node_external_t));
	nalloc_route = nalloc_thread_init(tid, sizeof(route_node_t));
	nalloc_base = nalloc_thread_init(tid, sizeof(base_node_t));
	return ca_tdata_new(tid);
}

void map_tdata_print(void *thread_data)
{
	ca_tdata_print(thread_data);
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	ca_tdata_add(d1, d2, dst);
}

int map_lookup(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = ca_lookup(map, key, thread_data);
	return ret; 
}

int map_rquery(void *map, void *thread_data, map_key_t key1, map_key_t key2)
{
	int ret = 0, nkeys;
	ret = ca_rquery(map, key1, key2, &nkeys, thread_data);
	return ret; 
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = ca_insert(map, key, value, thread_data);
	return ret;
}

int map_delete(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = ca_delete(map, key, thread_data);
	return ret;
}

int map_update(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = ca_update(map, key, value, thread_data);
	return ret;
}

int map_validate(void *map)
{
	int ret = 0;
	ret = ca_validate_helper(map);
	return ret;
}

char *map_name()
{
	return "ca-locks(" seq_ds_name ")";
}
