#include <stdint.h>
#include <stdlib.h>
#include <limits.h>

#include "utils.h"
#include "../../key/key.h"
#include "../../map.h"
#include "bst.h"
#define BST_NATARAJAN
#define BST_EXTERNAL
#include "validate.h"
#include "print.h"

#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

#define CAS_PTR(a,b,c) __sync_val_compare_and_swap(a,b,c)

//> Helper functions
#define GETFLAG(ptr) ((uint64_t)(ptr) & 1)
#define GETTAG(ptr)  ((uint64_t)(ptr) & 2)
#define FLAG(ptr)    ((bst_node_t *)((uint64_t)(ptr) | 1))
#define TAG(ptr)     ((bst_node_t *)((uint64_t)(ptr) | 2))
#define UNTAG(ptr)   ((bst_node_t *)((uint64_t)(ptr) & 0xfffffffffffffffd))
#define UNFLAG(ptr)  ((bst_node_t *)((uint64_t)(ptr) & 0xfffffffffffffffe))
#define ADDRESS(ptr) ((bst_node_t *)((uint64_t)(ptr) & 0xfffffffffffffffc))

typedef struct seek_record_t {
	bst_node_t *ancestor,
	           *successor,
	           *parent,
	           *leaf;
	char padding[CACHE_LINE_SIZE - 4 * sizeof(bst_node_t *)];
} __attribute__((aligned(CACHE_LINE_SIZE))) seek_record_t;

static __thread seek_record_t *seek_record;
static __thread void *nalloc;

static seek_record_t *seek(map_key_t key, bst_node_t *root,
                           int *nr_nodes_traversed)
{
	volatile seek_record_t seek_record_l;
	bst_node_t *parent_field, *current_field, *current;
	bst_node_t *node_s;

	*nr_nodes_traversed = 0;

	node_s = ADDRESS(root->right);
	seek_record_l.ancestor = root;
	seek_record_l.successor = node_s; 
	seek_record_l.parent = node_s;
	seek_record_l.leaf = ADDRESS(node_s->right);

	parent_field = seek_record_l.parent->right;
	current_field = seek_record_l.leaf->right;
	current = ADDRESS(current_field);

	while (current != NULL) {
		(*nr_nodes_traversed)++;

		if (!GETTAG(parent_field)) {
			seek_record_l.ancestor = seek_record_l.parent;
			seek_record_l.successor = seek_record_l.leaf;
		}
		seek_record_l.parent = seek_record_l.leaf;
		seek_record_l.leaf = current;

		parent_field = current_field;
		current_field = (KEY_CMP(key, current->key) <= 0) ? current->left :
		                                                    current->right;
		current = ADDRESS(current_field);
	}
	seek_record->ancestor = seek_record_l.ancestor;
	seek_record->successor = seek_record_l.successor;
	seek_record->parent = seek_record_l.parent;
	seek_record->leaf = seek_record_l.leaf;
	return seek_record;
}

static int bst_search(map_key_t key, bst_node_t *root)
{
	int nr_nodes;
	seek(key, root, &nr_nodes);
	return (KEY_CMP(seek_record->leaf->key, key) == 0);
}

static int bst_cleanup(map_key_t key)
{
	bst_node_t *ancestor, *successor, *parent, *chld, *sibl;
	bst_node_t **succ_addr, **child_addr, **sibling_addr;
	bst_node_t *untagged, *tagged, *res;

	ancestor = seek_record->ancestor;
	successor = seek_record->successor;
	parent = seek_record->parent;

	succ_addr = (KEY_CMP(key, ancestor->key) <= 0) ? &(ancestor->left) :
	                                                 &(ancestor->right);

	if (KEY_CMP(key, parent->key) <= 0) {
		child_addr   = &(parent->left);
		sibling_addr = &(parent->right);
	} else {
		child_addr   = &(parent->right);
		sibling_addr = &(parent->left);
	}

	chld = *child_addr;
	if (!GETFLAG(chld)) {
		chld = *sibling_addr;
		sibling_addr = child_addr;
	}

	do {
		untagged = *sibling_addr;
		tagged = TAG(untagged);
		res = CAS_PTR(sibling_addr,untagged, tagged);
	} while (res != untagged);

	sibl = *sibling_addr;
	if (CAS_PTR(succ_addr, ADDRESS(successor), UNTAG(sibl)) == ADDRESS(successor))
		return 1;

	return 0;
}

static int do_bst_insert(map_key_t key, void *val, uint *created,
                         bst_node_t **new_internal, bst_node_t **new_node)
{
	bst_node_t *parent = seek_record->parent;
	bst_node_t *leaf = seek_record->leaf;
	bst_node_t **child_addr;
	bst_node_t *result, *chld;

	if (KEY_CMP(key, parent->key) <= 0) child_addr = &(parent->left); 
	else                                child_addr = &(parent->right);

	if (*created == 0) {
		*new_internal = bst_node_new(key, 0);
		*new_node = bst_node_new(key,val);
		*created = 1;
	}

	if (KEY_CMP(key, leaf->key) < 0) {
		(*new_internal)->left = *new_node;
		(*new_internal)->right = leaf; 
	} else {
		(*new_internal)->right = *new_node;
		(*new_internal)->left = leaf;
	}

	(*new_internal)->key = (*new_internal)->left->key;

	result = CAS_PTR(child_addr, ADDRESS(leaf), ADDRESS(*new_internal));
	if (result == ADDRESS(leaf)) return 1;

	chld = *child_addr; 
	if ((ADDRESS(chld) == leaf) && (GETFLAG(chld) || GETTAG(chld)))
		bst_cleanup(key);

	return 0;
}

static int bst_insert(map_key_t key, void *val, bst_node_t *root)
{
	int nr_nodes;
	bst_node_t *new_internal = NULL, *new_node = NULL;
	uint created = 0;

	while (1) {
		seek(key, root, &nr_nodes);
		if (KEY_CMP(seek_record->leaf->key, key) == 0) return 0;
		if (do_bst_insert(key, val, &created, &new_internal, &new_node))
			return 1;
	}
}

static int do_bst_remove(map_key_t key, int *injecting, bst_node_t **leaf)
{
	void *val = seek_record->leaf->data;
	bst_node_t *parent = seek_record->parent;
	bst_node_t **child_addr;
	bst_node_t *lf, *result, *chld;

	child_addr = (KEY_CMP(key, parent->key) <= 0) ? &(parent->left) :
	                                                &(parent->right);

	if (*injecting == 1) {
		*leaf = seek_record->leaf;
		if (KEY_CMP((*leaf)->key, key) != 0) return 0;

		lf = ADDRESS(*leaf);
		result = CAS_PTR(child_addr, lf, FLAG(lf));
		if (result == ADDRESS(*leaf)) {
			*injecting = 0;
			if (bst_cleanup(key)) return 1;
		} else {
			if ((ADDRESS(*child_addr) == *leaf) &&
			    (GETFLAG(*child_addr) || GETTAG(*child_addr)))
				bst_cleanup(key);
		}
	} else {
		if (seek_record->leaf != *leaf) return 1; 
		else if (bst_cleanup(key)) return 1;
	}

	return -1;
}


static int bst_remove(map_key_t key, bst_node_t *root)
{
	int nr_nodes, ret, injecting = 1;
	bst_node_t *leaf;

	while (1) {
		seek(key, root, &nr_nodes);
		ret = do_bst_remove(key, &injecting, &leaf);
		if (ret != -1) return ret;
    }
}

static int bst_update(map_key_t key, void *val, bst_node_t *root)
{
	int nr_nodes, ret, injecting = 1;
	bst_node_t *leaf;
	bst_node_t *new_internal = NULL, *new_node = NULL;
	uint created = 0;
	int op_is_insert = -1;

	while (1) {
		seek(key, root, &nr_nodes);
		if (op_is_insert == -1) {
			if (KEY_CMP(seek_record->leaf->key, key) == 0) op_is_insert = 0;
			else                                           op_is_insert = 1;
		}

		if (op_is_insert) {
			if (KEY_CMP(seek_record->leaf->key, key) == 0)
				return 0;
			if (do_bst_insert(key, val, &created, &new_internal, &new_node))
				return 1;
		} else {
			ret = do_bst_remove(key, &injecting, &leaf);
			if (ret != -1) return ret + 2;
		}
    }
}

/******************************************************************************/
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(bst_node_t));
	bst_t *bst = _bst_new_helper();
	bst->root = bst_node_new(MIN_KEY, NULL);
	bst->root->left = bst_node_new(MIN_KEY, NULL);
	bst->root->right = bst_node_new(MIN_KEY, NULL);
	bst->root->right->left = bst_node_new(MIN_KEY, NULL);
	bst->root->right->right = bst_node_new(MIN_KEY, NULL);
	return bst;
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(bst_node_t));
	XMALLOC(seek_record, 1);
	return NULL;
}

void map_tdata_print(void *thread_data)
{
	return;
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
}

int map_lookup(void *bst, void *thread_data, map_key_t key)
{
	int ret;
	ret = bst_search(key, ((bst_t *)bst)->root);
	return ret;
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *bst, void *thread_data, map_key_t key, void *data)
{
	int ret = 0;
	ret = bst_insert(key, data, ((bst_t *)bst)->root);
	return ret;
}

int map_delete(void *bst, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = bst_remove(key, ((bst_t *)bst)->root);
	return ret;
}

int map_update(void *bst, void *thread_data, map_key_t key, void *data)
{
	int ret = 0;
	ret = bst_update(key, data, ((bst_t *)bst)->root);
	return ret;
}

int map_validate(void *bst)
{
	int ret = 1;
	ret = bst_validate(bst);
	return ret;
}

char *map_name()
{
	return "bst_aravind";
}
