#ifndef _SL_PRINT_H_
#define _SL_PRINT_H_

#include <stdio.h>
#include "sl_types.h"

static void _sl_print(sl_t *sl)
{
	int i;
	sl_node_t *curr;

	int level_nodes[MAX_LEVEL]; /* DEBUG */

	for (i=0; i < MAX_LEVEL; i++) {
		level_nodes[i] = 0;
//		printf("LEVEL[%2d]: ", i);
		for (curr = sl->head; curr; curr = curr->next[i]) {
//			printf(" -> %d", curr->key);
			level_nodes[i]++;
		}
//		printf("\n");
	}
//	printf("\n");

	for (i=0; i < MAX_LEVEL; i++)
		printf("NR_NODES[%d] = %d\n", i, level_nodes[i]);
}

#endif /* _SL_PRINT_H_ */
