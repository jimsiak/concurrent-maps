/**
 * This is a concurrent map implementation that uses the skiplist from
 * the following paper:
 * "A Simple Optimistic skip-list Algorithm", Herlihy, Lev, Lunchangco, Shavit
 * 
 * The implementation is heavily based on the implementation provided by ASCYLIB:
 *   https://github.com/LPD-EPFL/ASCYLIB
 *
 **/

#include <stdio.h>
#include <assert.h>

#include "../key/key.h"

#define SL_HERLIHY
#define LOCK_PER_NODE
#define LEVEL_PER_NODE
#include "sl_types.h"
#include "sl_random.h"
#include "sl_validate.h"
#include "sl_thread_data.h"

static inline int find_node(sl_t *sl, map_key_t key, sl_node_t *preds[],
                                                     sl_node_t *succs[])
{
	int i, found;
	sl_node_t *pred, *curr;

	found = -1;
	pred = sl->head;

	for (i=MAX_LEVEL-1; i >= 0; i--) {
		curr = pred->next[i];
		while (KEY_CMP(curr->key, key) < 0) {
			pred = curr;
			curr = pred->next[i];
		}
		if (preds)
			preds[i] = pred;
		succs[i] = curr;
		if (found == -1 && KEY_CMP(key, curr->key) == 0)
			found = i;
	}
	return found;
}

static inline sl_node_t *find_node_left(sl_t *sl, map_key_t key)
{
	int i;
	sl_node_t *pred, *curr, *nd = NULL;
	
	pred = sl->head;
	
	for (i = MAX_LEVEL - 1; i >= 0; i--) {
		curr = pred->next[i];
		while (KEY_CMP(curr->key, key) < 0) {
			pred = curr;
			curr = pred->next[i];
		}

		if (KEY_CMP(key, curr->key) == 0) {
			nd = curr;
			break;
		}
	}
	return nd;
}

static int _sl_lookup(sl_t *sl, map_key_t key)
{
	sl_node_t *node = find_node_left(sl, key);
	if (node && !node->marked && node->fully_linked) return 1;
	return 0;
}

static inline void unlock_levels(sl_t *sl, sl_node_t *preds[], int highest_locked)
{
	int i;
	sl_node_t *old = NULL;

	for (i=0; i <= highest_locked; i++) {
		if (old != preds[i])
			UNLOCK_NODE(preds[i]);
		old = preds[i];
	}
}

static int _sl_insert(sl_t *sl, map_key_t key, void *value, sl_node_t **new_node,
                      sl_thread_data_t *tdata)
{
	sl_node_t *succs[MAX_LEVEL], *preds[MAX_LEVEL];
	sl_node_t *prev_pred, *pred, *succ;
	sl_node_t *node_found;
	int i, toplevel, found, highest_locked, valid;

	toplevel = get_rand_level(tdata);
	new_node[0]->toplevel = toplevel;

	while (1) {
		found = find_node(sl, key, preds, succs);
		if (found != -1) {
			node_found = succs[found];
			if (!node_found->marked) {
				while (!node_found->fully_linked)
					;
				return 0;
			}
			continue;
		}

		highest_locked = -1;
		prev_pred = NULL;
		valid = 1;

		for (i=0; valid && i < toplevel; i++) {
			pred = preds[i];
			succ = succs[i];
			if (pred != prev_pred) {
				LOCK_NODE(pred);
				highest_locked = i;
				prev_pred = pred;
			}

			valid = (!pred->marked && !succ->marked && (pred->next[i] == succ));
		}

		if (!valid) {
			unlock_levels(sl, preds, highest_locked);
			continue;
		}

		for (i=0; i < toplevel; i++)
			new_node[0]->next[i] = succs[i];

		__sync_synchronize(); /* Works! (sync asm instruction, hw barrier) */
//		asm volatile("":::"memory"); /* Doesn't work :) */

		for (i=0; i < toplevel; i++)
			preds[i]->next[i] = new_node[0];

		new_node[0]->fully_linked = 1;
		unlock_levels(sl, preds, highest_locked);
		return 1;
	}

	/* Unreachable */
	assert(0);
	return -1;
}

static inline int ok_to_delete(sl_node_t *node, int found)
{
	return (node->fully_linked && (node->toplevel - 1 == found) && !node->marked);
}

static int _sl_delete(sl_t *sl, map_key_t key, sl_node_t **node_to_delete)
{
	sl_node_t *succs[MAX_LEVEL], *preds[MAX_LEVEL];
	sl_node_t *pred, *succ, *prev_pred;
	int i, is_marked, toplevel, highest_locked, valid, found;

	is_marked = 0;
	toplevel = -1;

	while (1) {
		found = find_node(sl, key, preds, succs);

		if (is_marked || (found != -1 && ok_to_delete(succs[found], found))) {
			if (!is_marked) {
				node_to_delete[0] = succs[found];
				LOCK_NODE(node_to_delete[0]);
				toplevel = node_to_delete[0]->toplevel;

				if (node_to_delete[0]->marked) {
					UNLOCK_NODE(node_to_delete[0]);
					return 0;
				}

				node_to_delete[0]->marked = 1;
				is_marked = 1;
			}

			highest_locked = -1;
			prev_pred = NULL;
			valid = 1;
			for (i=0; valid && i < toplevel; i++) {
				pred = preds[i];
				succ = succs[i];
				if (pred != prev_pred) {
					LOCK_NODE(pred);
					highest_locked = i;
					prev_pred = pred;
				}

				valid = (!pred->marked && pred->next[i] == succ);
			}

			if (!valid) {
				unlock_levels(sl, preds, highest_locked);
				continue;
			}

			for (i=toplevel-1; i >= 0; i--)
				preds[i]->next[i] = node_to_delete[0]->next[i];

			UNLOCK_NODE(node_to_delete[0]);
			unlock_levels(sl, preds, highest_locked);
			return 1;
		} else {
			return 0;
		}
	}

	/* Unreachable. */
	assert(0);
	return -1;
}

/******************************************************************************/
/*         Map interface implementation                                       */
/******************************************************************************/
void *map_new()
{
	return _sl_new();
}

void *map_tdata_new(int tid)
{
	return sl_thread_data_new(tid);
}

void map_tdata_print(void *thread_data)
{
	sl_thread_data_print(thread_data);
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	sl_thread_data_add(d1, d2, dst);
}

int map_lookup(void *sl, void *thread_data, map_key_t key)
{
	int ret = 0;
	sl_thread_data_t *tdata = thread_data;

	ret = _sl_lookup(sl, key);

	return ret;
}

int map_rquery(void *sl, void *thread_data, map_key_t key1, map_key_t key2)
{
	return 0;
}

int map_insert(void *sl, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	sl_node_t *new_node[1];
	new_node[0] = _sl_node_new(key, value);
	sl_thread_data_t *tdata = thread_data;

	ret = _sl_insert(sl, key, value, new_node, thread_data);

	if (!ret)
		_sl_node_free(new_node[0]);

	return ret;
}

int map_delete(void *sl, void *thread_data, map_key_t key)
{
	int ret = 0;
	sl_node_t *node_to_delete[1] = { NULL };
	sl_thread_data_t *tdata = thread_data;

	ret = _sl_delete(sl, key, node_to_delete);

	return ret;
}

int map_update(void *sl, void *thread_data, map_key_t key, void *value)
{
	return 0;
}

int map_validate(void *sl)
{
	int ret;
	ret = _sl_validate_helper(sl);
	return ret;
}

void *map_name()
{
	return "skip_list_herlihy";
}
