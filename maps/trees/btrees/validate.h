#ifndef _VALIDATE_H_
#define _VALIDATE_H_

#include <stdio.h>
#include "btree.h"
#include "../../key/key.h"

static int bst_violations, total_nodes, total_keys, leaf_keys;
static int null_children_violations, empty_internal_violations;
static int not_full_nodes;
static int leaves_level, leaves_at_same_level, leaves_empty;
static int wrong_sibling_pointers;

/**
 * Validates the following:
 * 1. Keys inside node are sorted (and there are no duplicates).
 * 2. Keys inside node in the range (min, max].
 * 3. Node is at least half-full.
 * 4. Internal nodes do not have null children.
 **/
static void btree_node_validate(btree_node_t *n, btree_t *btree,
                                map_key_t min, map_key_t max)
{
	int i;

	//> Some b-trees (e.g., blink lock-based by Yao et. al) may allow empty leaves
	if (n->no_keys == 0) {
		if (n->leaf) leaves_empty++;
		else empty_internal_violations++;
		return;
	}

	//> 1. Check that keys inside node are sorted (ascending order) and unique
	for (i=1; i < n->no_keys; i++)
		if (KEY_CMP(n->keys[i], n->keys[i-1]) <= 0)
			bst_violations++;

	//> 2. Check that keys inside node are in the range (min, max]
	if (KEY_CMP(n->keys[0], min) < 0 || KEY_CMP(n->keys[n->no_keys-1], max) > 0)
		bst_violations++;

	//> 3. Check that the node is at least half-full
	//>    NOTE: In some implementations this is not an error
	if (n != btree->root && n->no_keys < BTREE_ORDER)
		not_full_nodes++;

	//> 4. Check that the node does not have null children.
	//>    (Only if we are on an internal node)
	if (!n->leaf)
		for (i=0; i <= n->no_keys; i++)
			if (!n->children[i])
				null_children_violations++;
}

static btree_node_t *btree_validate_rec(btree_node_t *root, btree_t *btree,
                                        map_key_t min, map_key_t max, int level)
{
	btree_node_t *sib;
	int i;

	if (!root) return NULL;

	total_nodes++;
	total_keys += root->no_keys;

	btree_node_validate(root, btree, min, max);
	
	if (root->leaf) {
		if (leaves_level == -1) leaves_level = level;
		else if (level != leaves_level) leaves_at_same_level = 0;
		leaf_keys += root->no_keys;
		return root->sibling;
	}

	for (i=0; i <= root->no_keys; i++) {
		sib = btree_validate_rec(root->children[i], btree,
		                         i == 0 ? min : root->keys[i-1],
		                         i == root->no_keys ? max : root->keys[i],
		                         level+1);
		if (i < root->no_keys && sib != NULL && sib != root->children[i+1])
			wrong_sibling_pointers++;
	}
	return NULL;
}

static int btree_validate_helper(btree_t *btree)
{
	int check_bst = 0, check_btree_properties = 0;
	bst_violations = 0;
	total_nodes = 0;
	total_keys = leaf_keys = 0;
	null_children_violations = 0;
	empty_internal_violations = 0;
	not_full_nodes = 0;
	leaves_level = -1;
	leaves_at_same_level = 1;
	leaves_empty = 0;
	wrong_sibling_pointers = 0;

	btree_validate_rec(btree->root, btree, MIN_KEY, MAX_KEY, 0);

	check_bst = (bst_violations == 0);
	check_btree_properties = (null_children_violations == 0) &&
#ifndef NOT_FULL_NODES_ALLOWED
	                         (not_full_nodes == 0) &&
#endif
	                         (leaves_at_same_level == 1) &&
	                         (wrong_sibling_pointers == 0) &&
	                         (empty_internal_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  BTREE Violation: %s\n",
	       check_btree_properties ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- NULL Children Violation: %s\n",
	       (null_children_violations == 0) ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- Not-full Nodes: %s\n",
#ifndef NOT_FULL_NODES_ALLOWED
	       (not_full_nodes == 0) ? "No [OK]" : "Yes [ERROR]");
#else
	       (not_full_nodes == 0) ? "No [OK]" : "Yes [ALLOWED]");
#endif
	printf("  |-- Leaves at same level: %s [ Level %d ]\n",
	       (leaves_at_same_level == 1) ? "Yes [OK]" : "No [ERROR]", leaves_level);
	printf("  |-- Wrong sibling pointers: %d [%s]\n", wrong_sibling_pointers,
	       (wrong_sibling_pointers == 0) ? "OK" : "ERROR");
	printf("  |-- Empty Internal nodes: %d [%s]\n", empty_internal_violations,
	       (empty_internal_violations == 0) ? "OK" : "ERROR");
	printf("  Number of Empty Leaf nodes: %d\n", leaves_empty);
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
	printf("\n");

	return check_bst && check_btree_properties;
}

#endif /* _VALIDATE_H_ */
