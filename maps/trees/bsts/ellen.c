#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>

#include "../../map.h"
#include "../../key/key.h"

#include "arch.h" /* CACHE_LINE_SIZE */

#define CAS_PTR(a, b, c) __sync_val_compare_and_swap(a, b, c)

//> The states a node can have:
//>   we avoid an enum to better control the size of the data structures
#define STATE_CLEAN 0
#define STATE_DFLAG 1
#define STATE_IFLAG 2
#define STATE_MARK 3

typedef struct bst_node_s bst_node_t;
typedef union info_t info_t;

typedef struct iinfo_t {
	bst_node_t* p;
	bst_node_t* new_internal;
	bst_node_t* l;
} iinfo_t;

typedef struct dinfo_t {
	bst_node_t* gp;
	bst_node_t* p;
	bst_node_t* l;
	info_t *pupdate;
} dinfo_t;

union info_t {
	iinfo_t iinfo;
	dinfo_t dinfo;
	uint8_t padding[CACHE_LINE_SIZE];
};

#define NODE_HAS_UPDATE
#define NODE_HAS_ISLEAF
#include "bst.h"
#define BST_EXTERNAL
#define BST_ELLEN
#include "validate.h"
#include "print.h"

typedef struct search_result_t {
	bst_node_t* gp; 
	bst_node_t* p;
	bst_node_t* l;
	info_t *pupdate;
	info_t *gpupdate;
} search_result_t;

//> Per thread variables
static __thread search_result_t last_result;
static __thread void *nalloc;
static __thread void *nalloc_info;

#define GETFLAG(ptr)    (((uint64_t)(ptr)) & 3)
#define FLAG(ptr, flag) ((((uint64_t)(ptr)) & 0xfffffffffffffffc) | flag)
#define UNFLAG(ptr)	    (((uint64_t)(ptr)) & 0xfffffffffffffffc)

static search_result_t *bst_search(map_key_t key, bst_node_t *root)
{
	//> JimSiak, DON'T remove "volatile" from the declaration of `res`
	//> if you do, and compile with -O3 some super aggressive optimization is
	//> performed and operations are lost!!!
	search_result_t volatile *res = &last_result;

	res->l = root;
	while (!res->l->isleaf) {
		res->gp = res->p;
		res->p = res->l;
		res->gpupdate = res->pupdate;
		res->pupdate = res->p->update;
		if (KEY_CMP(key, res->l->key) <= 0) res->l = res->p->left;
		else                                res->l = res->p->right;
	}
	return (search_result_t *)res;
}

//> JimSiak: the original version of ASCYLIB used bst_search for the
//>          lookup operation, but this leads to very slow performance due to
//>          more memory accesses.
static int bst_find(map_key_t key, bst_node_t *root)
{
	bst_node_t *c = root;
	while (!c->isleaf) {
		if (KEY_CMP(key, c->key) <= 0) c = c->left;
		else                           c = c->right;
	}
	return (KEY_CMP(c->key, key) == 0);
}

static info_t *create_iinfo_t(bst_node_t *p, bst_node_t *ni, bst_node_t *l)
{
	info_t *new_info;
	new_info = nalloc_alloc_node(nalloc_info);
	new_info->iinfo.p = p; 
	new_info->iinfo.new_internal = ni; 
	new_info->iinfo.l = l; 
	return new_info;
}

static info_t *create_dinfo_t(bst_node_t *gp, bst_node_t *p, bst_node_t *l,
                              info_t *u)
{
	info_t *new_info;
	new_info = nalloc_alloc_node(nalloc_info);
	new_info->dinfo.gp = gp; 
	new_info->dinfo.p = p; 
	new_info->dinfo.l = l; 
	new_info->dinfo.pupdate = u; 
	return new_info;
}

static void bst_cas_child(bst_node_t *parent, bst_node_t *old, bst_node_t *new)
{
	bst_node_t **ptr;
	//> If the parent contains MIN_KEY, i.e., it is the root of the tree, we
	//> we always go to the its right child. This special case arises due to
	//> the three dummy nodes that make up an empty tree and all contain MIN_KEY
	ptr = &parent->right;
	if (KEY_CMP(parent->key, MIN_KEY) != 0 && KEY_CMP(new->key, parent->key) <= 0)
		ptr = &parent->left;
	bst_node_t *unused = CAS_PTR(ptr, old, new);
}

static void bst_help_insert(info_t *op)
{
	bst_cas_child(op->iinfo.p, op->iinfo.l, op->iinfo.new_internal);
	(void)CAS_PTR(&(op->iinfo.p->update), FLAG(op, STATE_IFLAG),
	                                      FLAG(op, STATE_CLEAN));
}

static void bst_help_marked(info_t *op)
{
	bst_node_t *other;
	other = (op->dinfo.p->right == op->dinfo.l) ? op->dinfo.p->left :
	                                              op->dinfo.p->right;
	bst_cas_child(op->dinfo.gp, op->dinfo.p,other);
	(void)CAS_PTR(&(op->dinfo.gp->update), FLAG(op,STATE_DFLAG),
	                                       FLAG(op,STATE_CLEAN));
}

static void bst_help(info_t *u);

static char bst_help_delete(info_t *op)
{
	info_t *result; 
	result = CAS_PTR(&(op->dinfo.p->update), op->dinfo.pupdate,
	                                         FLAG(op,STATE_MARK));
	if (result == op->dinfo.pupdate || result == (info_t *)FLAG(op, STATE_MARK)) {
		bst_help_marked(op);
		return 1;
	} else {
		bst_help(result);
		(void)CAS_PTR(&(op->dinfo.gp->update), FLAG(op,STATE_DFLAG), FLAG(op,STATE_CLEAN));
		return 0;
	}
}

static void bst_help(info_t *u)
{
	if      (GETFLAG(u) == STATE_IFLAG) bst_help_insert((info_t*)UNFLAG(u));
	else if (GETFLAG(u) == STATE_MARK)  bst_help_marked((info_t*)UNFLAG(u));
	else if (GETFLAG(u) == STATE_DFLAG) bst_help_delete((info_t*)UNFLAG(u)); 
}

static int do_bst_insert(map_key_t key, void *data, bst_node_t **new_node,
                         bst_node_t **new_sibling, bst_node_t **new_internal, 
                         search_result_t *search_result)
{
	info_t *op, *result;

	//> Another operation is currently on parent node. Help it.
	if (GETFLAG(search_result->pupdate) != STATE_CLEAN) {
		bst_help(search_result->pupdate);
		return 0;
	}

	if (*new_node == NULL) {
		*new_node = bst_node_new(key, data, 1); 
		*new_sibling = bst_node_new(search_result->l->key, search_result->l->data, 1);
		*new_internal = bst_node_new(key, 0, 0);
	}
	KEY_COPY((*new_sibling)->key, search_result->l->key);
	(*new_sibling)->data = search_result->l->data;
	(*new_sibling)->isleaf = 1;
	(*new_internal)->data = 0;
	(*new_internal)->isleaf = 0;

	if (KEY_CMP((*new_node)->key, (*new_sibling)->key) <= 0) {
		(*new_internal)->left = *new_node;
		(*new_internal)->right = *new_sibling;
	} else {
		(*new_internal)->left = *new_sibling;
		(*new_internal)->right = *new_node;
	}
	KEY_COPY((*new_internal)->key, (*new_internal)->left->key);
	op = create_iinfo_t(search_result->p, *new_internal, search_result->l);
	result = CAS_PTR(&(search_result->p->update), search_result->pupdate,
	                                              FLAG(op,STATE_IFLAG));
	if (result == search_result->pupdate) {
		bst_help_insert(op);
		return 1;
	} else {
		bst_help(result);
		return 0;
	}
}

static int bst_insert(map_key_t key, void *data,  bst_node_t *root)
{
	bst_node_t *new_internal = NULL, *new_sibling = NULL, *new_node = NULL;
	search_result_t *search_result;

	while(1) {
		search_result = bst_search(key,root);
		if (KEY_CMP(search_result->l->key, key) == 0)
			return 0;
		if (do_bst_insert(key, data, &new_node, &new_sibling, &new_internal,
		                  search_result))
			return 1;
	}
}

static int do_bst_delete(search_result_t *search_result)
{
	info_t *op, *result;
	void *found_data;

	found_data = search_result->l->data;
	if (GETFLAG(search_result->gpupdate) != STATE_CLEAN) {
		bst_help(search_result->gpupdate);
		return 0;
	} 

	if (GETFLAG(search_result->pupdate) != STATE_CLEAN){
		bst_help(search_result->pupdate);
		return 0;
	}

	op = create_dinfo_t(search_result->gp, search_result->p, 
	                    search_result->l, search_result->pupdate);
	result = CAS_PTR(&(search_result->gp->update), search_result->gpupdate,
	                  FLAG(op,STATE_DFLAG));
	if (result == search_result->gpupdate) {
		if (bst_help_delete(op) == 1)
			return 1;
	} else {
		bst_help(result);
	}
	return 0;
}

static int bst_delete(map_key_t key, bst_node_t *root)
{
	search_result_t *search_result;
	while (1) {
		search_result = bst_search(key,root); 
		if (KEY_CMP(search_result->l->key, key) != 0) return 0;
		if (do_bst_delete(search_result)) return 1;
	}
}

static int bst_update(map_key_t key, void *data, bst_node_t *root)
{
	int op_is_insert = -1;
	bst_node_t *new_internal = NULL, *new_sibling = NULL, *new_node = NULL;
	search_result_t* search_result;

	while (1) {
		search_result = bst_search(key,root); 
		if (op_is_insert == -1) {
			if (KEY_CMP(search_result->l->key, key) == 0) op_is_insert = 0;
			else                                          op_is_insert = 1;
		}

		if (op_is_insert) {
			if (KEY_CMP(search_result->l->key, key) == 0)
				return 0;
			if (do_bst_insert(key, data, &new_node, &new_sibling, &new_internal, search_result))
				return 1;
		} else {
			if (KEY_CMP(search_result->l->key, key) != 0)
				return 2;
			if (do_bst_delete(search_result))
				return 3;
		}
	}
	return 0;
}

static unsigned long long bst_size_rec(bst_node_t* node)
{
		if (node->isleaf == 0) {
			return (bst_size_rec((bst_node_t*) node->right) + bst_size_rec((bst_node_t*) node->left));
		} else {
			return 1;
		}
}

static unsigned long long bst_size(bst_node_t* node)
{
	return bst_size_rec(node)-2;
}

/******************************************************************************/
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	printf("Size of tree node is %lu\n", sizeof(bst_node_t));
	bst_t *bst = _bst_new_helper();
	bst->root = bst_node_new(MIN_KEY, 0, 0);
	bst->root->left = bst_node_new(MIN_KEY, 0, 1);
	bst->root->right = bst_node_new(MIN_KEY, 0, 1);
	return bst;
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(bst_node_t));
	nalloc_info = nalloc_thread_init(tid, sizeof(info_t));
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
	ret = bst_find(key, ((bst_t *)bst)->root);
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
	ret = bst_delete(key, ((bst_t *)bst)->root);
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
	return "bst_ellen";
}
