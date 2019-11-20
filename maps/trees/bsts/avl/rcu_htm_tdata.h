#ifndef _RCU_HTM_TDATA_H_
#define _RCU_HTM_TDATA_H_

typedef struct {
	int tid;
	long long unsigned tx_starts, tx_aborts, 
	                   tx_aborts_explicit_validation, lacqs;
	ht_t *ht;
} tdata_t;

static inline tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	ret->tid = tid;
	ret->tx_starts = 0;
	ret->tx_aborts = 0;
	ret->tx_aborts_explicit_validation = 0;
	ret->lacqs = 0;
	ret->ht = ht_new();
	return ret;
}

static inline void tdata_print(tdata_t *tdata)
{
	printf("TID %3d: %llu %llu %llu ( %llu )\n", tdata->tid, tdata->tx_starts,
	      tdata->tx_aborts, tdata->tx_aborts_explicit_validation, tdata->lacqs);
}

static inline void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	dst->tx_starts = d1->tx_starts + d2->tx_starts;
	dst->tx_aborts = d1->tx_aborts + d2->tx_aborts;
	dst->tx_aborts_explicit_validation = d1->tx_aborts_explicit_validation +
	                                     d2->tx_aborts_explicit_validation;
	dst->lacqs = d1->lacqs + d2->lacqs;
}

#endif /* _RCU_HTM_TDATA_H_ */
