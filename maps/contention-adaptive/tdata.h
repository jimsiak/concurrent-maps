#ifndef _TDATA_H_
#define _TDATA_H_

#include <string.h>
#include "alloc.h"

typedef struct {
	int tid;
	int joins;
	int splits;
} ca_tdata_t;

ca_tdata_t *ca_tdata_new(int tid)
{
	ca_tdata_t *tdata;
	XMALLOC(tdata, 1);
	memset(tdata, 0, sizeof(*tdata));
	tdata->tid = tid;
	return tdata;
}

void ca_tdata_print(ca_tdata_t *tdata)
{
	printf("%3d %5d %5d\n", tdata->tid, tdata->joins, tdata->splits);
}

void ca_tdata_add(ca_tdata_t *d1, ca_tdata_t *d2, ca_tdata_t *dst)
{
	dst->joins = d1->joins + d2->joins;
	dst->splits = d1->splits + d2->splits;
}

#endif /* _TDATA_H_ */
