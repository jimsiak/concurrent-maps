#ifndef _PRINT_H_
#define _PRINT_H_

#include "bst.h"
#include "../../key/key.h"

static void bst_print_rec(bst_node_t *root, int level)
{
	int i;

	if (root) bst_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++) printf("|--");

	if (!root) {
		printf("|~\n");
		return;
	}

	KEY_PRINT(root->key, "", "\n");

	bst_print_rec(root->left, level + 1);
}

static void bst_print(bst_t *bst)
{
	if (bst->root == NULL) printf("[empty]");
	else                   bst_print_rec(bst->root, 0);
	printf("\n");
}

#endif /* _PRINT_H_ */
