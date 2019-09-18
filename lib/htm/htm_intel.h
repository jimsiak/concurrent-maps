#ifndef _HTM_INTEL_H_
#define _HTM_INTEL_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h> /* pthread_spinlock_t */

#include "rtm.h" /* _xbegin() etc. */
#include "alloc.h" /* XMALLOC() */

//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//> Interface for fine-grained HTM implementations
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
typedef unsigned tm_begin_ret_t;
#define LOCK_FREE 1
#define TM_BEGIN_SUCCESS _XBEGIN_STARTED
#define ABORT_VALIDATION_FAILURE 0xee
#define ABORT_GL_TAKEN           0xff
#define ABORT_IS_CONFLICT(status) ((status) & _XABORT_CONFLICT)
#define ABORT_IS_EXPLICIT(status) ((status) & _XABORT_EXPLICIT)
#define ABORT_CODE(status) _XABORT_CODE(status)
#define TX_ABORT(code) _xabort(code)
#define TX_BEGIN(code) _xbegin()
#define TX_END(code)   _xend()
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

#define EXP_THRESHOLD 2
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#define EXP_POW(f,c,m) (MIN((c) * (f), (m)))

enum {
	TX_ABORT_CONFLICT,
	TX_ABORT_CAPACITY,
	TX_ABORT_EXPLICIT,
	TX_ABORT_REST,
	TX_ABORT_REASONS_END
};

typedef struct {
	int tid;
	int tx_starts,
	    tx_commits,
	    tx_aborts,
	    tx_lacqs;

	int tx_aborts_per_reason[TX_ABORT_REASONS_END];

} tx_thread_data_t;

static inline void *tx_thread_data_new(int tid)
{
	tx_thread_data_t *ret;

	XMALLOC(ret, 1);
	memset(ret, 0, sizeof(*ret));
	ret->tid = tid;
	return ret;
}

static inline void tx_thread_data_print(void *thread_data)
{
	int i;

	if (!thread_data)
		return;

	tx_thread_data_t *data = thread_data;

	printf("TXSTATS(TSX): %3d %12d %12d %12d (", data->tid,
	       data->tx_starts, data->tx_commits, data->tx_aborts);

	for (i=0; i < TX_ABORT_REASONS_END; i++)
		printf(" %12d", data->tx_aborts_per_reason[i]);
	printf(" )");
	printf(" %12d\n", data->tx_lacqs);
}

static inline void tx_thread_data_add(void *d1, void *d2, void *dst)
{
	int i;
	tx_thread_data_t *data1 = d1, *data2 = d2, *dest = dst;

	dest->tx_starts = data1->tx_starts + data2->tx_starts;
	dest->tx_commits = data1->tx_commits + data2->tx_commits;
	dest->tx_aborts = data1->tx_aborts + data2->tx_aborts;
	dest->tx_lacqs = data1->tx_lacqs + data2->tx_lacqs;

	for (i=0; i < TX_ABORT_REASONS_END; i++)
		dest->tx_aborts_per_reason[i] = data1->tx_aborts_per_reason[i] +
		                                data2->tx_aborts_per_reason[i];
}

static inline int tx_start(int num_retries, void *thread_data,
                            pthread_spinlock_t *fallback_lock)
{
	int status = 0;
	int aborts = num_retries;
	tx_thread_data_t *tdata = thread_data;

	while (1) {
		/* Avoid lemming effect. */
		while (*fallback_lock == 0)
			;

		tdata->tx_starts++;

		status = _xbegin();
		if (_XBEGIN_STARTED == (unsigned)status) {
			if (*fallback_lock == 0)
				_xabort(0xff);
			return num_retries - aborts;
		}
	
		/* Abort comes here. */
		tdata->tx_aborts++;

		if (status & _XABORT_CAPACITY) {
			tdata->tx_aborts_per_reason[TX_ABORT_CAPACITY]++;
		} else if (status & _XABORT_CONFLICT) {
			tdata->tx_aborts_per_reason[TX_ABORT_CONFLICT]++;
		} else if (status & _XABORT_EXPLICIT) {
			tdata->tx_aborts_per_reason[TX_ABORT_EXPLICIT]++;
		} else {
			tdata->tx_aborts_per_reason[TX_ABORT_REST]++;
		}

		if (--aborts <= 0) {
			pthread_spin_lock(fallback_lock);
			return num_retries - aborts;
		}
	}

	/* Unreachable. */
	return -1;
}

static inline int tx_end(void *thread_data, pthread_spinlock_t *fallback_lock)
{
	tx_thread_data_t *tdata = thread_data;

	if (*fallback_lock == 1) {
		_xend();
		tdata->tx_commits++;
		return 0;
	} else {
		pthread_spin_unlock(fallback_lock);
		tdata->tx_lacqs++;
		return 1;
	}
}

#endif /* _HTM_INTEL_H_ */
