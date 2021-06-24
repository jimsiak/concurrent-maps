#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "../../map.h"
#include "../../key/key.h"
#include "alloc.h"
#include "arch.h"

#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)
#define CAS_BOOL(a,b,c) __sync_bool_compare_and_swap(a,b,c)
#define CAS_U32(a,b,c) __sync_val_compare_and_swap(a,b,c)

//> Encoded in the operation pointer
#define STATE_OP_NONE 0
#define STATE_OP_MARK 1
#define STATE_OP_CHILDCAS 2
#define STATE_OP_RELOCATE 3

//> In the relocate_op struct
#define STATE_OP_ONGOING 0
#define STATE_OP_SUCCESSFUL 1
#define STATE_OP_FAILED 2

//> States for the result of a search operation
#define FOUND 0x0
#define NOT_FOUND_L 0x1
#define NOT_FOUND_R 0x2
#define ABORT 0x3

//> Helper Macros
#define GETFLAG(op)    ((uint64_t)(op) & 3)
#define FLAG(op, flag) ((operation_t *)((((uint64_t)(op)) & 0xfffffffffffffffc) | (flag)))
#define UNFLAG(op)     ((operation_t *)(((uint64_t)(op)) & 0xfffffffffffffffc))
#define ISNULL(node)   (((node) == NULL) || (((uint64_t)node) & 1))
#define SETNULL(node)  ((node_t *)((((uint64_t)node) & 0xfffffffffffffffe) | 1))

static __thread void *nalloc;

typedef __attribute__((aligned(64))) union operation_t operation_t;
typedef __attribute__((aligned(64))) struct node_t node_t;

typedef struct child_cas_op_t {
	char is_left;
	node_t *expected,
	       *update;
} child_cas_op_t;

typedef struct relocate_op_t {
	int state; // initialize to ONGOING every time a relocate operation is created
	node_t *dest;
	operation_t *dest_op;
	int remove_key;
	int replace_key;
	void *remove_value;
	void *replace_value;
} relocate_op_t;

union operation_t {
	child_cas_op_t child_cas_op;
	relocate_op_t relocate_op;
    char padding[CACHE_LINE_SIZE];
};

struct node_t {
	int key; 
    void *value;
	operation_t *op;
	node_t *left,
	       *right;
};

//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
//> Per-thread data statistics
typedef struct {
	int tid;
	unsigned long long retries[3]; // 0 -> bst_find(), 1 -> inserts, 2 -> deletes
} tdata_t;
tdata_t *tdata_new(int tid)
{
	tdata_t *td;
	XMALLOC(td, 1);
	memset(td, 0, sizeof(*td));
	td->tid = tid;
	return td;
}
void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	int i;
	for (i=0; i < 3; i++) dst->retries[i] = d1->retries[i] + d2->retries[i];
}
void tdata_print(tdata_t *td)
{
	int i;
	printf("TID %3d:", td->tid);
	for (i=0; i < 3; i++)
		printf(" %llu", td->retries[i]);
	printf("\n");
}
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

node_t *create_node(int key, void *value)
{
	node_t *new_node = nalloc_alloc_node(nalloc);
    new_node->key = key;
    new_node->value = value;
    new_node->op = NULL;
	new_node->right = SETNULL(new_node->right);
	new_node->left = SETNULL(new_node->left);
    return new_node;
}

operation_t* alloc_op()
{
    operation_t *new_op;
	XMALLOC(new_op, 1);
	memset(new_op, 0, sizeof(*new_op));
    return new_op;
}

node_t *bst_initialize()
{
	//> Assign minimum key to the root, actual tree will be 
	//> the right subtree of the root.
	node_t *root = create_node(MIN_KEY, NULL);
	return root;
}

/**
 * Changes the appropriate child (i.e., left or right) of node `dest` from
 * `op->expected` to `op->update`
**/
static void help_child_cas(operation_t *op, node_t *dest)
{
	node_t **address = (op->child_cas_op.is_left) ? &(dest->left) :
	                                                &(dest->right);
	(void)CAS_PTR(address, op->child_cas_op.expected, op->child_cas_op.update);
	(void)CAS_PTR(&(dest->op), FLAG(op, STATE_OP_CHILDCAS), FLAG(op, STATE_OP_NONE));
}

static void help_marked(operation_t *pred_op, node_t *pred, node_t *curr)
{
	node_t *new_ref;
	if (ISNULL(curr->left)) {
		if (ISNULL(curr->right)) new_ref = SETNULL(curr);
		else                     new_ref = curr->right;
	} else {
		new_ref = curr->left;
	}

	operation_t *cas_op = alloc_op();
	cas_op->child_cas_op.is_left = (curr == pred->left);
	cas_op->child_cas_op.expected = curr;
	cas_op->child_cas_op.update = new_ref;

	if (CAS_BOOL(&(pred->op), pred_op, FLAG(cas_op, STATE_OP_CHILDCAS)))
		help_child_cas(cas_op, pred);
}

static char help_relocate(operation_t *op, node_t *pred, operation_t *pred_op,
                                           node_t *curr)
{
	int seen_state = op->relocate_op.state;
	if (seen_state == STATE_OP_ONGOING) {
		operation_t *seen_op = CAS_PTR(&(op->relocate_op.dest->op), op->relocate_op.dest_op, FLAG(op, STATE_OP_RELOCATE));
		if (seen_op == op->relocate_op.dest_op || seen_op == FLAG(op, STATE_OP_RELOCATE)) {
			CAS_PTR(&(op->relocate_op.state), STATE_OP_ONGOING, STATE_OP_SUCCESSFUL);
			seen_state = STATE_OP_SUCCESSFUL;
		} else {
			seen_state = CAS_PTR(&(op->relocate_op.state), STATE_OP_ONGOING, STATE_OP_FAILED);
		}
	}

	if (seen_state == STATE_OP_SUCCESSFUL) {
		CAS_BOOL(&(op->relocate_op.dest->key), op->relocate_op.remove_key, op->relocate_op.replace_key);
		CAS_BOOL(&(op->relocate_op.dest->value), op->relocate_op.remove_value, op->relocate_op.replace_value);
		CAS_BOOL(&(op->relocate_op.dest->op), FLAG(op, STATE_OP_RELOCATE), FLAG(op, STATE_OP_NONE));
	}

	char result = (seen_state == STATE_OP_SUCCESSFUL);
	if (op->relocate_op.dest == curr) return result;

	CAS_BOOL(&(curr->op), FLAG(op, STATE_OP_RELOCATE), FLAG(op, result ? STATE_OP_MARK : STATE_OP_NONE));
	if (result) {
		if (op->relocate_op.dest == pred)
			pred_op = FLAG(op, STATE_OP_NONE);
		help_marked(pred_op, pred, curr);
	}
	return result;
}

static void help(node_t *pred, operation_t *pred_op,
                 node_t *curr, operation_t *curr_op)
{
	switch (GETFLAG(curr_op)) {
	case STATE_OP_CHILDCAS:
		help_child_cas(UNFLAG(curr_op), curr);
		break;
	case STATE_OP_RELOCATE:
		help_relocate(UNFLAG(curr_op), pred, pred_op, curr);
		break;
	case STATE_OP_MARK:
		help_marked(pred_op, pred, curr);
		break;
	default:
		assert(0);
	}
}

static int bst_find(int k, node_t **pred, operation_t **pred_op,
                           node_t **curr, operation_t **curr_op,
                           node_t *aux_root, node_t *root,
                           tdata_t *tdata)
{
	int result, curr_key;
	node_t *next, *last_right;
	operation_t *last_right_op;

RETRY:
	*pred = NULL; *pred_op = NULL;

	result = NOT_FOUND_R;
	*curr = aux_root;
	*curr_op = (*curr)->op;

	//> Ongoing operation on the root of the tree
	if (GETFLAG(*curr_op) != STATE_OP_NONE) {
		if (aux_root == root) {
			help_child_cas(UNFLAG(*curr_op), *curr);
			tdata->retries[0]++;
			goto RETRY;
		} else {
			return ABORT;
		}
	}

	next = (*curr)->right;
	last_right = *curr;
	last_right_op = *curr_op;

	while (!ISNULL(next)) {
		*pred = *curr;
		*pred_op = *curr_op;
		*curr = next;
		*curr_op = (*curr)->op;

		if (GETFLAG(*curr_op) != STATE_OP_NONE) {
			help(*pred, *pred_op, *curr, *curr_op);
			tdata->retries[0]++;
			goto RETRY;
		}

		curr_key = (*curr)->key;
		if (k < curr_key) {
			result = NOT_FOUND_L;
			next = (*curr)->left;
		} else if (k > curr_key) {
			result = NOT_FOUND_R;
			next = (*curr)->right;
			last_right = *curr;
			last_right_op = *curr_op;
		} else {
			result = FOUND;
			break;
		}
	}
	
	if (result != FOUND && last_right_op != last_right->op) {
		tdata->retries[0]++;
		goto RETRY;
	}

	if ((*curr)->op != *curr_op) {
		tdata->retries[0]++;
		goto RETRY;
	}

	return result;
} 

static int bst_contains(int k, node_t *root, tdata_t *tdata)
{
	node_t *pred, *curr;
	operation_t *pred_op, *curr_op;

    int ret = bst_find(k, &pred, &pred_op, &curr, &curr_op, root, root, tdata);
	return (ret == FOUND);
}

static int do_bst_add(int k, void *v, int result, node_t *root, node_t **new_node,
                      node_t *old, node_t *curr, operation_t *curr_op)
{
	operation_t *cas_op;
	char is_left;

	if (*new_node == NULL) *new_node = create_node(k, v);

	is_left = (result == NOT_FOUND_L);
	if (is_left) old = curr->left;
	else         old = curr->right;

	cas_op = alloc_op();
	cas_op->child_cas_op.is_left = is_left;
	cas_op->child_cas_op.expected = old;
	cas_op->child_cas_op.update = *new_node;

	if (CAS_PTR(&curr->op, curr_op, FLAG(cas_op, STATE_OP_CHILDCAS)) == curr_op) {
		help_child_cas(cas_op, curr);
		return 1;
	}
	return 0;
}

static int bst_add(int k, void *v, node_t *root, tdata_t *tdata)
{
	node_t *pred, *curr, *new_node = NULL, *old = NULL;
	operation_t *pred_op, *curr_op;
	int result = 0;

	while (1) {
		old = NULL;

		result = bst_find(k, &pred, &pred_op, &curr, &curr_op, root, root, tdata);
		if (result == FOUND) return 0;

		if (do_bst_add(k, v, result, root, &new_node, old, curr, curr_op))
			return 1;
		tdata->retries[1]++;
	}
}

static int do_bst_remove(int k, node_t *root, node_t *curr, node_t *pred,
                         operation_t *curr_op, operation_t *pred_op,
                         operation_t **reloc_op, tdata_t *tdata)
{
	int res;
	node_t *replace;
	operation_t *replace_op;

	if (ISNULL(curr->right) || ISNULL(curr->left)) {
		//> Node has less than two children
		if (CAS_BOOL(&(curr->op), curr_op, FLAG(curr_op, STATE_OP_MARK))) {
			help_marked(pred_op, pred, curr);
			return 1;
		}
	} else {
		//> Node has two children
		res = bst_find(k, &pred, &pred_op, &replace, &replace_op, curr, root, tdata);
		if (res == ABORT || curr->op != curr_op)
			return 0;
            
		if (*reloc_op == NULL) *reloc_op = alloc_op(); 
		(*reloc_op)->relocate_op.state = STATE_OP_ONGOING;
		(*reloc_op)->relocate_op.dest = curr;
		(*reloc_op)->relocate_op.dest_op = curr_op;
		(*reloc_op)->relocate_op.remove_key = k;
		(*reloc_op)->relocate_op.remove_value = res;
		(*reloc_op)->relocate_op.replace_key = replace->key;
		(*reloc_op)->relocate_op.replace_value = replace->value;

		if (CAS_BOOL(&(replace->op), replace_op, FLAG(*reloc_op, STATE_OP_RELOCATE)))
			if (help_relocate(*reloc_op, pred, pred_op, replace))
				return 1;
	}
	return 0;
}

static int bst_remove(int k, node_t *root, tdata_t *tdata)
{
	node_t *pred, *curr;
	operation_t *pred_op, *curr_op, *reloc_op = NULL;
    int res;

	while (1) {
        res = bst_find(k, &pred, &pred_op, &curr, &curr_op, root, root, tdata);
		if (res != FOUND) return 0;

		if (do_bst_remove(k, root, curr, pred, curr_op, pred_op, &reloc_op, tdata))
			return 1;
		tdata->retries[2]++;
	}
}

static int bst_update(int k, void *v, node_t *root, tdata_t *tdata)
{
	node_t *pred, *curr, *new_node = NULL, *old;
	operation_t *pred_op, *curr_op, *reloc_op = NULL;
	int res = 0;
	int op_is_insert = -1;

	while (1) {
		old = NULL;

		res = bst_find(k, &pred, &pred_op, &curr, &curr_op, root, root, tdata);
		if (op_is_insert == -1) {
			if (res == FOUND) op_is_insert = 0;
			else              op_is_insert = 1;
		}

		if (op_is_insert) {
			if (res == FOUND)
				return 0;
			if (do_bst_add(k, v, res, root, &new_node, old, curr, curr_op))
				return 1;
			tdata->retries[1]++;
		} else {
			if (res != FOUND)
				return 2;
			if (do_bst_remove(k, root, curr, pred, curr_op, pred_op, &reloc_op, tdata))
				return 3;
			tdata->retries[2]++;
		}
	}
	return 0;
}

static int total_paths, total_nodes, bst_violations;
static int min_path_len, max_path_len;
static void _bst_validate_rec(volatile node_t *root, int _th)
{
	if (ISNULL(root))
		return;

	volatile node_t *left = root->left;
	volatile node_t *right = root->right;

	total_nodes++;
	_th++;

	/* BST violation? */
	if (!ISNULL(left) && left->key >= root->key)
		bst_violations++;
	if (!ISNULL(right) && right->key < root->key)
		bst_violations++;

	/* We found a path (a node with at least one sentinel child). */
	if (ISNULL(left) && ISNULL(right)) {
		total_paths++;

		if (_th <= min_path_len)
			min_path_len = _th;
		if (_th >= max_path_len)
			max_path_len = _th;
	}

	/* Check subtrees. */
	_bst_validate_rec(left, _th);
	_bst_validate_rec(right, _th);
}

static inline int _bst_validate_helper(node_t *root)
{
	int check_bst = 0;
	total_paths = 0;
	min_path_len = 99999999;
	max_path_len = -1;
	total_nodes = 0;
	bst_violations = 0;

	_bst_validate_rec(root->right, 0);

	check_bst = (bst_violations == 0);

	printf("Validation:\n");
	printf("=======================\n");
	printf("  BST Violation: %s\n",
	       check_bst ? "No [OK]" : "Yes [ERROR]");
	printf("  Tree size: %8d\n", total_nodes);
	printf("  Total paths: %d\n", total_paths);
	printf("  Min/max paths length: %d/%d\n", min_path_len, max_path_len);
	printf("\n");

	return check_bst;
}

/*********************    FOR DEBUGGING ONLY    *******************************/
static void bst_print_rec(volatile node_t *root, int level)
{
	int i;

	if (!ISNULL(root))
		bst_print_rec(root->right, level + 1);

	for (i = 0; i < level; i++)
		printf("|--");

	if (ISNULL(root)) {
		printf("NULL\n");
		return;
	}

	printf("%d\n", root->key);

	bst_print_rec(root->left, level + 1);
}
static void bst_print_struct(volatile node_t *n)
{
	if (ISNULL(n))
		printf("[empty]");
	else
		bst_print_rec(n, 0);
	printf("\n");
}
int bst_print(volatile node_t *n) {
	bst_print_struct(n);
	return 0;
}
/******************************************************************************/

/******************************************************************************/
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(node_t));
	return (void *)bst_initialize();
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(node_t));
	tdata_t *td = tdata_new(tid);
	return td;
}

void map_tdata_print(void *thread_data)
{
	tdata_print(thread_data);
	return;
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	tdata_add(d1, d2, dst);
}

int map_lookup(void *bst, void *thread_data, int key)
{
	int ret;
	ret = bst_contains(key, bst, thread_data);
	return ret;
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *bst, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = bst_add(key, value, bst, thread_data);
	return ret;
}

int map_delete(void *bst, void *thread_data, int key)
{
	int ret = 0;
	ret = bst_remove(key, bst, thread_data);
	return ret;
}

int map_update(void *bst, void *thread_data, int key, void *value)
{
	int ret = 0;
	ret = bst_update(key, value, bst, thread_data);
	return ret;
}

int map_validate(void *bst)
{
	int ret = 1;
	ret = _bst_validate_helper(bst);
	return ret;
}

char *map_name()
{
	return "bst_howley";
}
