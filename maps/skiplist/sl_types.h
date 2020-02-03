#ifndef _SL_TYPES_H_
#define _SL_TYPES_H_

#ifdef LOCK_PER_NODE
#	include <pthread.h>
#endif

#include <string.h> /* memset() */
#include <limits.h> /* INT_MIN and INT_MAX */

#include "../key/key.h"
#include "alloc.h" /* XMALLOC() */

#define MAX_LEVEL 13

#ifdef LOCK_PER_NODE
#	define LOCK_NODE(node) (pthread_spin_lock(&(node)->lock))
#	define UNLOCK_NODE(node) (pthread_spin_unlock(&(node)->lock))
#endif

typedef struct sl_node {
	map_key_t key;
	void *value;
	struct sl_node *next[MAX_LEVEL];

#	ifdef SL_HERLIHY
	/* We need volatile here!! Especially with -O3. */
	volatile unsigned char marked;
	volatile unsigned char fully_linked;
#	endif

#	ifdef LOCK_PER_NODE
	pthread_spinlock_t lock;
#	endif

#	ifdef LEVEL_PER_NODE
	int toplevel;
#	endif

} sl_node_t;

typedef struct {
	sl_node_t *head;

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spinlock_t lock;
#	endif

} sl_t;

static sl_node_t *_sl_node_new(map_key_t key, void *value)
{
	sl_node_t *ret;

	XMALLOC(ret, 1);
	KEY_COPY(ret->key, key);
	ret->value = value;
	memset(ret->next, 0, MAX_LEVEL * sizeof(*ret->next));

#	ifdef SL_HERLIHY
	ret->marked = 0;
	ret->fully_linked = 0;
#	endif

#	ifdef LOCK_PER_NODE
	pthread_spin_init(&ret->lock, PTHREAD_PROCESS_SHARED);
#	endif

#	ifdef LEVEL_PER_NODE
	ret->toplevel = 0;
#	endif

	return ret;
}

static void _sl_node_free(sl_node_t *node)
{
	free(node);
}

static sl_t *_sl_new()
{
	int i;
	sl_t *ret;

	XMALLOC(ret, 1);

	ret->head = _sl_node_new(MIN_KEY, NULL);
#	ifdef SL_HERLIHY
	ret->head->fully_linked = 1;
#	endif

#	ifdef LEVEL_PER_NODE
	ret->head->toplevel = MAX_LEVEL;
#	endif

	ret->head->next[0] = _sl_node_new(MAX_KEY, NULL);
#	ifdef SL_HERLIHY
	ret->head->next[0]->fully_linked = 1;
#	endif

#	ifdef LEVEL_PER_NODE
	ret->head->next[0]->toplevel = MAX_LEVEL;
#	endif

	for (i=1; i < MAX_LEVEL; i++) {
		ret->head->next[i] = ret->head->next[0];
	}

#	if defined(SYNC_CG_SPINLOCK) || defined(SYNC_CG_HTM)
	pthread_spin_init(&ret->lock, PTHREAD_PROCESS_SHARED);
#	endif

	printf("Sizeof(sl_node_t) = %lu\n", sizeof(sl_node_t));
	return ret;
}

#endif /* _SL_TYPES_H_ */
