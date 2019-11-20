#ifndef _AVL_VALIDATE_H_
#define _AVL_VALIDATE_H_

#include "avl.h"

static int total_paths;
static int min_path_len, max_path_len;
static int total_nodes, marked_nodes;
#ifdef NODE_HAS_PARENT
static int parent_errors;
#endif
#ifdef NODE_HAS_LOCK
static int locked_nodes;
#endif
static int avl_violations, bst_violations;
static int _avl_validate_rec(avl_node_t *root, int _depth,
                             map_key_t min, map_key_t max)
{
	avl_node_t *left, *right;

	if (!root) return -1;

	if (root->data == MARKED_NODE) marked_nodes++;
#	ifdef NODE_HAS_LOCK
	if (root->lock != 1) locked_nodes++;
#	endif

	left = root->left;
	right = root->right;

#	ifdef NODE_HAS_PARENT
	if (left && left->parent != root) parent_errors++;
	if (right && right->parent != root) parent_errors++;
#	endif

	total_nodes++;
	_depth++;

	/* BST violation? */
	if (KEY_CMP(root->key, min) <= 0) bst_violations++;
#	ifdef BST_EXTERNAL
	if (KEY_CMP(root->key, max) > 0) bst_violations++;
#	else
	if (KEY_CMP(root->key, max) >= 0) bst_violations++;
#	endif

	/* We found a path (a node with at least one sentinel child). */
	if (!left || !right) {
		total_paths++;
		if (_depth <= min_path_len) min_path_len = _depth;
		if (_depth >= max_path_len) max_path_len = _depth;
	}

	/* Check subtrees. */
	int lheight = -1, rheight = -1;
	if (left)  lheight = _avl_validate_rec(left, _depth, min, root->key);
	if (right) rheight = _avl_validate_rec(right, _depth, root->key, max);

	/* AVL violation? */
	if (abs(lheight - rheight) > 1) avl_violations++;

	return MAX(lheight, rheight) + 1;
}

static inline int _avl_validate_helper(avl_node_t *root)
{
	int check_avl = 0, check_bst = 0;
	int check = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	marked_nodes = 0;
	avl_violations = 0;
	bst_violations = 0;
#	ifdef NODE_HAS_LOCK
	locked_nodes = 0;
#	endif
#	ifdef NODE_HAS_PARENT
	parent_errors = 0;
#	endif

	_avl_validate_rec(root, 0, MIN_KEY, MAX_KEY);

	check_avl = (avl_violations == 0);
	check_bst = (bst_violations == 0);
	check = (check_avl && check_bst);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Valid AVL Tree: %s\n",
	       check ? "Yes [OK]" : "No [ERROR]");
	printf("  AVL Violation: %s\n",
	       check_avl ? "No [OK]" : "Yes [ERROR]");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Total nodes: %d ( %d Unmarked / %d Marked )\n", total_nodes,
	                             total_nodes - marked_nodes, marked_nodes);
#	ifdef NODE_HAS_PARENT
	printf("  Parent errors: %d\n", parent_errors);
#	endif
#	ifdef NODE_HAS_LOCK
	printf("  Locked nodes: %d\n", locked_nodes);
#	endif
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check;
}

#endif /* _AVL_VALIDATE_H_ */
