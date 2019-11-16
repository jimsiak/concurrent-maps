#ifndef _PRINT_H_
#define _PRINT_H_

#include <stdio.h>
#include "btree.h"

static void btree_node_print(btree_node_t *n)
{
	int i;

	printf("btree_node: [");
	if (!n) {
		printf("]\n");
		return;
	}

	for (i=0; i < n->no_keys; i++)
		KEY_PRINT(n->keys[i], " ", " |");
#ifdef HIGHKEY_PER_NODE
	KEY_PRINT(n->highkey, " highkey = ", " ");
#endif
	printf("]");
	printf("%s\n", n->leaf ? " LEAF" : "");
}

void btree_print_rec(btree_node_t *root, int level)
{
	int i;

	printf("[LVL %4d]: ", level);
	fflush(stdout);
	btree_node_print(root);

	if (!root || root->leaf) return;

	for (i=0; i < root->no_keys; i++)
		btree_print_rec(root->children[i], level + 1);
	if (root->no_keys > 0)
		btree_print_rec(root->children[root->no_keys], level + 1);
}

void btree_print(btree_t *btree)
{
	if (!btree) printf("Empty tree\n");
	else btree_print_rec(btree->root, 0);
}

#endif /* _PRINT_H_ */
