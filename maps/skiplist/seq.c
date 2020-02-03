#include <stdio.h>
#include <stdlib.h> /* rand() */

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm/htm.h"
#	if !defined(TX_NUM_RETRIES)
#		define TX_NUM_RETRIES 20
#	endif
#endif

#include "../key/key.h"

#include "sl_random.h"
#include "sl_types.h"
#include "sl_validate.h"
#include "sl_thread_data.h"

static int _sl_lookup(sl_t *sl, map_key_t key)
{
	int i;
	sl_node_t *curr = sl->head;

	for (i = MAX_LEVEL - 1; i >= 0; i--)
		while (KEY_CMP(curr->next[i]->key, key) < 0)
			curr = curr->next[i];

	return (KEY_CMP(key, curr->next[0]->key) == 0);
}

static __thread map_key_t rquery_result[10000];

int _sl_rquery(sl_t *sl, map_key_t key1, map_key_t key2)
{
	int i, nkeys = 0;
	sl_node_t *curr = sl->head;

	for (i = MAX_LEVEL - 1; i >= 0; i--)
		while (KEY_CMP(curr->next[i]->key, key1))
			curr = curr->next[i];

	curr = curr->next[0];
	while (KEY_CMP(curr->key, key2) <= 0) {
		KEY_COPY(rquery_result[nkeys++], curr->key);
		curr = curr->next[0];
	}

	return 1;
}

static sl_node_t *_sl_traverse(sl_t *sl, map_key_t key,
                               sl_node_t *currs_saved[MAX_LEVEL])
{
	int i;
	sl_node_t *curr = sl->head;

	for (i = MAX_LEVEL - 1; i >= 0; i--) {
		while (KEY_CMP(curr->next[i]->key, key) < 0)
			curr = curr->next[i];
		currs_saved[i] = curr;
	}

	return curr;
}

static void _do_insert(sl_node_t *n, sl_node_t *currs_saved[MAX_LEVEL],
                       sl_thread_data_t *tdata)
{
	int i, l;
	l = get_rand_level(tdata);
	for (i=0; i < l; i++) {
		n->next[i] = currs_saved[i]->next[i];
		currs_saved[i]->next[i] = n;
	}
}

static int _sl_insert(sl_t *sl, map_key_t key, void *value, sl_node_t **new_node,
                      sl_thread_data_t *tdata)
{
	sl_node_t *curr, *currs_saved[MAX_LEVEL];

	curr = _sl_traverse(sl, key, currs_saved);
	if (KEY_CMP(key, curr->next[0]->key) == 0) return 0;
	_do_insert(new_node[0], currs_saved, tdata);
	return 1;
}

static void _do_delete(map_key_t key, sl_node_t *currs_saved[MAX_LEVEL])
{
	int i;
	for (i=0; i < MAX_LEVEL && currs_saved[i]; i++) {
		if (KEY_CMP(currs_saved[i]->next[i]->key, key) == 0)
			currs_saved[i]->next[i] = currs_saved[i]->next[i]->next[i];
	}
}

static int _sl_delete(sl_t *sl, map_key_t key, sl_node_t **node_to_delete)
{
	sl_node_t *curr, *currs_saved[MAX_LEVEL];

	curr = _sl_traverse(sl, key, currs_saved);
	if (KEY_CMP(key, curr->next[0]->key) != 0) return 0;
	_do_delete(key, currs_saved);
	return 1;
}

static int _sl_update(sl_t *sl, map_key_t key, void *value, sl_node_t **new_node,
                      sl_node_t **node_to_delete, sl_thread_data_t *tdata)
{
	sl_node_t *curr, *currs_saved[MAX_LEVEL];

	curr = _sl_traverse(sl, key, currs_saved);

	if (KEY_CMP(key, curr->next[0]->key) != 0) {
		_do_insert(new_node[0], currs_saved, tdata);
		return 1;
	} else {
		_do_delete(key, currs_saved);
		return 3;
	}
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

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	ret = _sl_lookup(sl, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	return ret;
}

int map_rquery(void *sl, void *thread_data, map_key_t key1, map_key_t key2)
{
	int ret = 0;
	sl_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	ret = _sl_rquery(sl, key1, key2);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	return ret;

}

int map_insert(void *sl, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	sl_node_t *new_node[1];
	new_node[0] = _sl_node_new(key, value);
	sl_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	ret = _sl_insert(sl, key, value, new_node, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	if (!ret)
		_sl_node_free(new_node[0]);

	return ret;
}

int map_delete(void *sl, void *thread_data, map_key_t key)
{
	int ret = 0;
	sl_node_t *node_to_delete[1] = { NULL };
	sl_thread_data_t *tdata = thread_data;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	ret = _sl_delete(sl, key, node_to_delete);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

//	if (ret)
//		_ll_node_free(node_to_delete[0]);

	return ret;
}

int map_update(void *sl, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	sl_node_t *new_node[1];
	sl_node_t *node_to_delete[1] = { NULL };
	sl_thread_data_t *tdata = thread_data;

	new_node[0] = _sl_node_new(key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

	ret = _sl_update(sl, key, value, new_node, node_to_delete, thread_data);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((sl_t *)sl)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(tdata->tx_data, &((sl_t *)sl)->lock);
#	endif

//	if (!ret)
//		_sl_node_free(new_node[0]);

	return ret;

}

int map_validate(void *sl)
{
	int ret;
	ret = _sl_validate_helper(sl);
	return ret;
}

void *map_name()
{
#	if defined(SYNC_CG_SPINLOCK)
	return "skiplist-cg-lock";
#	elif defined(SYNC_CG_HTM)
	return "skiplist-cg-htm";
#	else
	return "skiplist-sequential";
#	endif
}
