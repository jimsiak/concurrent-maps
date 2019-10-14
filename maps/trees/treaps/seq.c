#include <stdio.h>
#include <stdlib.h>
#include "seq.h"
#include "treap.h"
#include "validate.h"

#if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
#	include <pthread.h> //> pthread_spinlock_t
#endif

#if defined(SYNC_CG_HTM)
#	include "htm/htm.h"
#endif

/******************************************************************************/
/*     Map interface implementation                                           */
/******************************************************************************/
void *map_new()
{
	printf("Size of treap node is %lu (internal) and %lu (external)\n",
	        sizeof(treap_node_internal_t), sizeof(treap_node_external_t));
	return treap_new();
}

void *map_tdata_new(int tid)
{
	nalloc_internal = nalloc_thread_init(tid, sizeof(treap_node_internal_t));
	nalloc_external = nalloc_thread_init(tid, sizeof(treap_node_external_t));
#	if defined(SYNC_CG_HTM)
	return tx_thread_data_new(tid);
#	else
	return NULL;
#	endif
}

void map_tdata_print(void *thread_data)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(thread_data);
#	endif
	return;
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(d1, d2, dst);
#	endif
}

int map_lookup(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((treap_t *)map)->lock);
#	endif

	ret = treap_seq_lookup(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((treap_t *)map)->lock);
#	endif

	return ret; 
}

int map_rquery(void *map, void *thread_data, map_key_t key1, map_key_t key2)
{
	int ret = 0, nkeys;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((treap_t *)map)->lock);
#	endif

	ret = treap_seq_rquery(map, key1, key2, &nkeys);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((treap_t *)map)->lock);
#	endif

	return ret; 
}

int map_insert(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((treap_t *)map)->lock);
#	endif

	ret = treap_seq_insert(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((treap_t *)map)->lock);
#	endif
	return ret;
}

int map_delete(void *map, void *thread_data, map_key_t key)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((treap_t *)map)->lock);
#	endif

	ret = treap_seq_delete(map, key);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((treap_t *)map)->lock);
#	endif

	return ret;
}

int map_update(void *map, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_lock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_start(TX_NUM_RETRIES, thread_data, &((treap_t *)map)->lock);
#	endif

	ret = treap_seq_update(map, key, value);

#	if defined(SYNC_CG_SPINLOCK)
	pthread_spin_unlock(&((treap_t *)map)->lock);
#	elif defined(SYNC_CG_HTM)
	tx_end(thread_data, &((treap_t *)map)->lock);
#	endif
	return ret;
}

int map_validate(void *map)
{
	int ret = 0;
	ret = treap_validate_helper(map);
	return ret;
}

char *map_name()
{
#	if defined(SYNC_CG_SPINLOCK)
	return "treap-cg-lock";
#	elif defined(SYNC_CG_HTM)
	return "treap-cg-htm";
#	else
	return "treap-sequential";
#	endif
}
