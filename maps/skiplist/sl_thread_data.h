#ifndef _SL_THREAD_DATA_
#define _SL_THREAD_DATA_

#include <stdlib.h> /* drand48_data */

#include "alloc.h" /* XMALLOC() */

#ifdef SYNC_CG_HTM
#	include "htm/htm.h"
#endif

typedef struct {
	int tid;
	struct drand48_data drand_buffer;

#	ifdef SYNC_CG_HTM
	tx_thread_data_t *tx_data;
#	endif
} sl_thread_data_t;

static inline sl_thread_data_t *sl_thread_data_new(int tid)
{
	sl_thread_data_t *ret;

	XMALLOC(ret, 1);
	ret->tid = tid;
	srand48_r(tid, &ret->drand_buffer);

#	ifdef SYNC_CG_HTM
	ret->tx_data = tx_thread_data_new(tid);
#	endif

	return ret;
}

static inline void sl_thread_data_print(sl_thread_data_t *tdata)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_print(tdata->tx_data);
#	endif
}

static inline void sl_thread_data_add(sl_thread_data_t *d1,
                                      sl_thread_data_t *d2,
                                      sl_thread_data_t *dst)
{
#	if defined(SYNC_CG_HTM)
	tx_thread_data_add(d1->tx_data, d2->tx_data, dst->tx_data);
#	endif
}

#endif /* _SL_THREAD_DATA_ */
