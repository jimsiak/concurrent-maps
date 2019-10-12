#ifndef _STACK_H_
#define _STACK_H_

#include <assert.h>

#define STACK_LENGTH 50

typedef struct {
	void *elems[STACK_LENGTH];
	int nr_elems;
} stack_t;

static inline void stack_push(stack_t *stack, void *elem)
{
	assert(stack->nr_elems < STACK_LENGTH);
	stack->elems[stack->nr_elems++] = elem;
}

static inline void *stack_pop(stack_t *stack)
{
	return (stack->nr_elems == 0) ? NULL : stack->elems[--stack->nr_elems];
}

static inline void stack_reset(stack_t *stack)
{
	stack->nr_elems = 0;
}

static inline int stack_size(stack_t *stack)
{
	return stack->nr_elems;
}

#endif /* _STACK_H_ */
