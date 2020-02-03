#ifndef _SL_VALIDATE_H_
#define _SL_VALIDATE_H_

#include <stdio.h>

#include "../key/key.h"
#include "sl_types.h"

static int _sl_validate_helper(sl_t *sl)
{
	map_key_t max = MIN_KEY;
	int total_nodes = -2; /* Don't count sentinel nodes. */
	int order_ok = 1;
	sl_node_t *curr;

	for (curr = sl->head; curr; curr = curr->next[0]) {
		total_nodes++;
		if (KEY_CMP(curr->key, max) < 0)
			order_ok = 0;
		KEY_COPY(max, curr->key);
	}

	printf("Validation:\n");
	printf("=======================\n");
	printf("  Total nodes: %d\n", total_nodes);
	printf("  Keys order: %s\n", order_ok ? "OK" : "ERROR");
	printf("\n");

	return order_ok;
}

#endif /* _SL_VALIDATE_H_ */
