#ifndef _KEY_H_
#define _KEY_H_

#if defined(MAP_KEY_TYPE_INT)
#	include "key_int.h"
#elif defined (MAP_KEY_TYPE_BIG_INT)
#	include "key_big_int.h"
#elif defined (MAP_KEY_TYPE_STR)
#	ifndef STR_KEY_SZ
#		define STR_KEY_SZ 50
#	endif
#	include "key_str.h"
#else
#	error "No key type defined..."
#endif

#endif /* _KEY_H_ */
