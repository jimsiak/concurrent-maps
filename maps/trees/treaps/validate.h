#ifndef _VALIDATE_H_
#define _VALIDATE_H_

#include <stdio.h>
#include <limits.h> //> ULLONG_MAX
#include "treap.h"
#include "../../key/key.h"

static int bst_violations, heap_violations;
static int total_nodes, internal_nodes, external_nodes;
static int total_keys, leaf_keys;
static int max_depth, min_depth;

/**
 * Validates the following:
 * 1. Keys inside node are sorted.
 * 2. Keys inside node are higher than min and less than or equal to max.
 **/
static void treap_node_external_validate(treap_node_external_t *n,
                                         map_key_t min, map_key_t max)
{
	int i;
	for (i=1; i < n->nr_keys; i++)
		if (KEY_CMP(n->keys[i-1], n->keys[i]) >= 0)
			bst_violations++;
	if (KEY_CMP(n->keys[0], min) < 0 || KEY_CMP(n->keys[n->nr_keys-1], max) > 0)
		bst_violations++;
	leaf_keys += n->nr_keys;
	total_keys += n->nr_keys;
}

static void treap_validate_rec(void *root, map_key_t min, map_key_t max,
                               unsigned long long max_priority, int depth)
{
	treap_node_internal_t *internal;
	total_nodes++;

	if (treap_node_is_internal(root)) {
		internal_nodes++;
		total_keys++;

		internal = root;
		if (internal->weight > max_priority) {
			heap_violations++;
			printf("internal->weight: %llu, max_priority: %llu\n", internal->weight, max_priority);
		}
		if (KEY_CMP(internal->key, min) < 0 || KEY_CMP(internal->key, max) > 0)
			bst_violations++;
		treap_validate_rec(internal->left, min, internal->key, internal->weight, depth+1);
		treap_validate_rec(internal->right, internal->key, max, internal->weight, depth+1);
	} else {
		external_nodes++;
		treap_node_external_validate(root, min, max);
		if (depth < min_depth) min_depth = depth;
		if (depth > max_depth) max_depth = depth;
	}
}

static int treap_validate_helper(treap_t *treap)
{
	int check_bst = 0, check_heap = 0;
	bst_violations = 0;
	heap_violations = 0;
	total_nodes = 0;
	internal_nodes = 0;
	external_nodes = 0;
	total_keys = 0;
	leaf_keys = 0;
	min_depth = 100000;
	max_depth = -1;

	if (treap->root) treap_validate_rec(treap->root, MIN_KEY, MAX_KEY, ULLONG_MAX, 0);

	check_bst = (bst_violations == 0);
	check_heap = (heap_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  HEAP Violation: %s\n",
	       check_heap ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d ( %8d internal / %8d external )\n",
	       total_nodes, internal_nodes, external_nodes);
	printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
	printf("  Depth (min/max): %d / %d\n", min_depth, max_depth);
	printf("\n");

	return check_bst && check_heap;
}

#endif /* _VALIDATE_H_ */
