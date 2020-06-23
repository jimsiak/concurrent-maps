#ifndef _UTILS_H_
#define _UTILS_H_

#define MIN(a, b) ({ \
	__typeof__ (a) _a = (a); \
	__typeof__ (b) _b = (b); \
	_a < _b ? _a : _b; \
})

#define MAX(a, b) ({ \
	__typeof__ (a) _a = (a); \
	__typeof__ (b) _b = (b); \
	_a > _b ? _a : _b; \
})

#define ABS(a) ({ \
	__typeof__ (a) _a = (a); \
	_a >= 0 ? _a : -a; \
})

#endif /* _UTILS_H_ */
