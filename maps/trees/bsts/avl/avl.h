#ifndef _AVL_T_
#define _AVL_T_

#define NODE_HAS_HEIGHT
#include "../bst.h"

typedef bst_node_t avl_node_t;
typedef bst_t avl_t;

#define MAX(a,b) ( (a) >= (b) ? (a) : (b) )

//> Used as `data` for marked nodes (zombie nodes)
#define MARKED_NODE ((void *)0xffffLLU)

avl_t *avl_new()
{
	return _bst_new_helper();
}

#endif /* _AVL_T_ */
