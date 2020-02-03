/**
 * This is a concurrent map implementation that uses the skiplist from
 * the following paper:
 * "Concurrent Maintenance of Skip Lists", Pugh
 * 
 * The implementation is heavily based on the implementation provided by ASCYLIB:
 *   https://github.com/LPD-EPFL/ASCYLIB
 *
 **/

#include <stdio.h>
#include <assert.h>

#include "../key/key.h"

#define LOCK_PER_NODE
#define LEVEL_PER_NODE
#include "sl_types.h"
#include "sl_random.h"
#include "sl_validate.h"
#include "sl_thread_data.h"

static int _sl_lookup(sl_t *sl, map_key_t key)
{
	int i;
	sl_node_t *succ, *pred;

	succ = NULL;
	pred = sl->head;

	for (i = MAX_LEVEL-1; i >= 0; i--) {
		succ = pred->next[i];
		while (KEY_CMP(succ->key, key) < 0) {
			pred = succ;
			succ = succ->next[i];
		}

		if (KEY_CMP(succ->key, key) == 0)
			return 1;
	}

	return 0;
}

static inline sl_node_t *get_lock(sl_node_t *pred, map_key_t key, int level)
{
	sl_node_t *succ = pred->next[level];
	
	while (KEY_CMP(succ->key, key) < 0) {
		pred = succ;
		succ = succ->next[level];
	}

	LOCK_NODE(pred);
	succ = pred->next[level];
	while (KEY_CMP(succ->key, key) < 0) {
		UNLOCK_NODE(pred);
		pred = succ;
		LOCK_NODE(pred);
		succ = pred->next[level];
	}

	return pred;
}

static int _sl_insert(sl_t *sl, map_key_t key, void *value, sl_node_t **new_node,
                      sl_thread_data_t *tdata)
{
	int i, toplevel;
	sl_node_t *update[MAX_LEVEL];
	sl_node_t *succ, *pred;

	pred = sl->head;
	for (i = MAX_LEVEL-1; i >= 0; i--) {
		succ = pred->next[i];
		while (KEY_CMP(succ->key, key) < 0) {
			pred = succ;
			succ = succ->next[i];
		}

		if (KEY_CMP(succ->key, key) == 0)
			return 0;
		
		update[i] = pred;
	}

	toplevel = get_rand_level(tdata);

	pred = get_lock(pred, key, 0);
	if (KEY_CMP(pred->next[0]->key, key) == 0) {
		UNLOCK_NODE(pred);
		return 0;
	}

	sl_node_t *n = new_node[0];
	n->toplevel = toplevel;
	LOCK_NODE(n);
	n->next[0] = pred->next[0];

	__sync_synchronize();

	pred->next[0] = n;
	UNLOCK_NODE(pred);

	for (i = 1; i < n->toplevel; i++) {
		pred = get_lock(update[i], key, i);
		n->next[i] = pred->next[i];
		__sync_synchronize();
		pred->next[i] = n;
		UNLOCK_NODE(pred);
	}

	UNLOCK_NODE(n);
	return 1;
}

static int _sl_delete(sl_t *sl, map_key_t key, sl_node_t **node_to_delete)
{
	int i, is_garbage;
	sl_node_t *update[MAX_LEVEL];
	sl_node_t *succ, *pred;

	succ = NULL;
	pred = sl->head;

	for (i = MAX_LEVEL - 1; i >= 0; i--) {
		succ = pred->next[i];
		while (KEY_CMP(succ->key, key) < 0) {
			pred = succ;
			succ = succ->next[i];
		}
		update[i] = pred;
	}

	succ = pred;

	while (1) { 
		succ = succ->next[0];
		if (KEY_CMP(succ->key, key) > 0)
			return 0;
		
		LOCK_NODE(succ);
		is_garbage = (KEY_CMP(succ->key, succ->next[0]->key) > 0);
		if (is_garbage || KEY_CMP(succ->key, key) != 0)
			UNLOCK_NODE(succ);
		else
			break;
	}

	for (i = succ->toplevel - 1; i >= 0; i--) {
		pred = get_lock(update[i], key, i);
		pred->next[i] = succ->next[i];
		succ->next[i] = pred;	/* pointer reversal! :-) */
		UNLOCK_NODE(pred);
	}  

	UNLOCK_NODE(succ);

	return 1;
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
	void *ret = sl_thread_data_new(tid);
	return ret;
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
	return "skip_list_pugh";
}
