#ifndef _LOG_H_
#define _LOG_H_

#include <stdio.h>
#include <stdarg.h>

typedef enum {
	DEBUG = 0,
	INFO,
	WARNING,
	ERROR,
	CRITICAL
}log_level_t;

//> Default LOG_LVL
#ifndef LOG_LVL
#	define LOG_LVL INFO
#endif

#define PRINT_MSG_PREFIX(lvl) \
//	printf("%10s - %15s() ==> ", #lvl, __func__)
//	printf("%10s - %s:%04d - %s ==> ", #lvl, __FILE__, __LINE__, __func__)

#define log_print(lvl, fmt, ...) \
	do { \
		if ((lvl) >= LOG_LVL) { \
			PRINT_MSG_PREFIX(lvl); \
			printf((fmt), ##__VA_ARGS__); \
		} \
	} while(0) \

#if (DEBUG >= LOG_LVL)
#	define log_debug(fmt, ...) log_print(DEBUG, (fmt), ##__VA_ARGS__)
#else
#	define log_debug(fmt, ...)
#endif

#if (INFO >= LOG_LVL)
#	define log_info(fmt, ...) log_print(INFO, (fmt), ##__VA_ARGS__)
#else
#	define log_info(fmt, ...)
#endif

#if (WARNING >= LOG_LVL)
#	define log_warning(fmt, ...) log_print(WARNING, (fmt), ##__VA_ARGS__)
#else
#	define log_warning(fmt, ...)
#endif

#if (ERROR >= LOG_LVL)
#	define log_error(fmt, ...) log_print(ERROR, (fmt), ##__VA_ARGS__)
#else
#	define log_error(fmt, ...)
#endif

#if (CRITICAL >= LOG_LVL)
#	define log_critical(fmt, ...) log_print(CRITICAL, (fmt), ##__VA_ARGS__)
#else
#	define log_critical(fmt, ...)
#endif

#endif /* _LOG_H_ */
