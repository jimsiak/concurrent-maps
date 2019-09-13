#ifndef _HTM_H_
#define _HTM_H_

#include "arch.h"

#if !defined(TX_NUM_RETRIES)
#	define TX_NUM_RETRIES 20
#endif

#if defined __POWERPC64__
#	include "htm_power.h"
#else
#	include "htm_intel.h"
#endif

#endif /* _HTM_H_ */
