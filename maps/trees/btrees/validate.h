#ifndef _VALIDATE_H_
#define _VALIDATE_H_

#include <stdio.h>
#include "btree.h"
#include "../../key/key.h"

static int bst_violations, total_nodes, total_keys, leaf_keys;
static int null_children_violations;
static int not_full_nodes;
static int leaves_level;
static int leaves_at_same_level;
static int wrong_sibling_pointers;

/**
 * Validates the following:
 * 1. Keys inside node are sorted.
 * 2. Keys inside node are higher than min and less than or equal to max.
 * 3. Node is at least half-full.
 * 4. Internal nodes do not have null children.
 **/
static void btree_node_validate(btree_node_t *n, int min, int max, btree_t *btree)
{
	int i;
	map_key_t cur_min;

	KEY_COPY(cur_min, n->keys[0]);

	if (n != btree->root && n->no_keys < BTREE_ORDER)
		not_full_nodes++;

	for (i=1; i < n->no_keys; i++) {
		if (KEY_CMP(n->keys[i], cur_min) <= 0) {
			bst_violations++;
		}
	}

	if (KEY_CMP(n->keys[0], min) < 0 || KEY_CMP(n->keys[n->no_keys-1], max) > 0) {
		bst_violations++;
	}

	if (!n->leaf)
		for (i=0; i <= n->no_keys; i++)
			if (!n->children[i])
				null_children_violations++;
}

static btree_node_t *btree_validate_rec(btree_node_t *root, int min, int max,
                                        btree_t *btree, int level)
{
	btree_node_t *sib;
	int i;

	if (!root) return NULL;

	total_nodes++;
	total_keys += root->no_keys;

	btree_node_validate(root, min, max, btree);
	
	if (root->leaf) {
		if (leaves_level == -1) leaves_level = level;
		else if (level != leaves_level) leaves_at_same_level = 0;
		leaf_keys += root->no_keys;
		return root->sibling;
	}

	for (i=0; i <= root->no_keys; i++) {
		sib = btree_validate_rec(root->children[i],
		                         i == 0 ? min : root->keys[i-1],
		                         i == root->no_keys ? max : root->keys[i],
		                         btree, level+1);
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
	not_full_nodes = 0;
	leaves_level = -1;
	leaves_at_same_level = 1;
	wrong_sibling_pointers = 0;

	btree_validate_rec(btree->root, -1, MAX_KEY, btree, 0);

	check_bst = (bst_violations == 0);
	check_btree_properties = (null_children_violations == 0) &&
	                         (not_full_nodes == 0) &&
	                         (leaves_at_same_level == 1) &&
	                         (wrong_sibling_pointers == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  BTREE Violation: %s\n",
	       check_btree_properties ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- NULL Children Violation: %s\n",
	       (null_children_violations == 0) ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- Not-full Nodes: %s\n",
	       (not_full_nodes == 0) ? "No [OK]" : "Yes [ERROR]");
	printf("  |-- Leaves at same level: %s [ Level %d ]\n",
	       (leaves_at_same_level == 1) ? "Yes [OK]" : "No [ERROR]", leaves_level);
	printf("  |-- Wrong sibling pointers: %d [%s]\n", wrong_sibling_pointers,
	       (wrong_sibling_pointers == 0) ? "OK" : "ERROR");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Number of keys: %8d total / %8d in leaves\n", total_keys, leaf_keys);
	printf("\n");

	return check_bst && check_btree_properties;
}

#endif /* _VALIDATE_H_ */
