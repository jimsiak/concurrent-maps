#ifndef _SL_RANDOM_H_
#define _SL_RANDOM_H_

#include <stdlib.h>
#include "sl_types.h"
#include "sl_thread_data.h"

static inline int get_rand_level(sl_thread_data_t *tdata)
{
	long int drand_res;
	int i, level = 1, ret;

	for (i=0; i < MAX_LEVEL - 1; i++) {
		lrand48_r(&tdata->drand_buffer, &drand_res);
		ret = (int) (drand_res % 100);
		if (ret < 50)
			level++;
		else
			break;
	}
	return level;
}

#endif /* _SL_RANDOM_H_ */
