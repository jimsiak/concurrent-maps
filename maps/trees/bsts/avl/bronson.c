/**
 * `avl->root` is an avl_node_t with no key or data whose right child is the root.
 **/
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#include "../../../key/key.h"
#include "../../../map.h"

#define NODE_HAS_PARENT
#define NODE_HAS_LOCK
#define NODE_HAS_VERSION
#include "avl.h"

#include "validate.h"
#include "print.h"

#define INIT_LOCK(lock) pthread_spin_init((lock), PTHREAD_PROCESS_SHARED)
#define LOCK(lock)      pthread_spin_lock((lock))
#define UNLOCK(lock)    pthread_spin_unlock((lock))

//> node->version handling
#define UNLINKED 0x1LL
#define SHRINKING 0x2LL
#define SHRINK_CNT_INC (0x1LL << 2)
#define IS_SHRINKING(version) (version & SHRINKING)

#define LEFT 0
#define RIGHT 1
#define GET_CHILD_DIR(node, dir) (((dir) == LEFT) ? (node)->left : (node)->right)
#define GET_CHILD_KEY(node, key) ( (KEY_CMP((key), (node)->key) < 0) ? \
                                                (node)->left : \
                                                (node)->right )

#define RETRY 0xFF

//#define SW_BARRIER() __sync_synchronize()
#define SW_BARRIER() asm volatile("": : :"memory")

#define BEGIN_SHRINKING(n) (n)->version |= SHRINKING; SW_BARRIER()
#define END_SHRINKING(n) SW_BARRIER(); (n)->version += SHRINK_CNT_INC; (n)->version &= (~SHRINKING)

static __thread void *nalloc;

avl_node_t *avl_node_new(map_key_t key, void *data, int height, long long version,
                         avl_node_t *parent)
{
	avl_node_t *node;
	node = nalloc_alloc_node(nalloc);
	memset(node, 0, sizeof(*node));
	KEY_COPY(node->key, key);
	node->data = data;
	node->height = height;
	node->version = version;
	node->parent = parent;
	INIT_LOCK(&node->lock);
	return node;
}

#define SPIN_CNT 100
void wait_until_not_changing(avl_node_t *n)
{
	volatile int i = 0;
	long long version = n->version;

	if (IS_SHRINKING(version)) {
		while (n->version == version && i < SPIN_CNT) ++i;
		if (i == SPIN_CNT) {
			LOCK(&n->lock);
			UNLOCK(&n->lock);
		}
	}
}

int attempt_get(map_key_t key, avl_node_t *node, int dir, long long version)
{
	avl_node_t *child;
	int next_dir, ret;
	long long child_version;

	while (1) {
		child = GET_CHILD_DIR(node, dir);
		SW_BARRIER();

		//> The node version has changed. Must retry.
		if (node->version != version) return RETRY;

		//> Reached NULL or the node with the specified key.
		if (child == NULL) return 0;
		if (KEY_CMP(key, child->key) == 0) return 1;

		//> Where to go next?
		next_dir = (KEY_CMP(key, child->key) < 0) ? LEFT : RIGHT;

		child_version = child->version;
		if (IS_SHRINKING(child_version)) {
			wait_until_not_changing(child);
		} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
			if (node->version != version) return RETRY;
			ret = attempt_get(key, child, next_dir, child_version);
			if (ret != RETRY) return ret;
		}
	}
}

int _avl_lookup_helper(avl_t *avl, map_key_t key)
{
	int ret;
	ret = attempt_get(key, avl->root, RIGHT, 0);
	assert(ret != RETRY);
	return ret;
}

/*****************************************************************************/
/* Rebalancing functions                                                     */
/*****************************************************************************/
#define UNLINK_REQUIRED -1
#define REBALANCE_REQUIRED -2
#define NOTHING_REQUIRED -3

#define NODE_HEIGHT(node) ((node == NULL) ? 0 : node->height)

static int node_condition(avl_node_t *node)
{
	avl_node_t *nl, *nr;
	int hn, hl, hr, hn_new, balance;

	nl = node->left;
	nr = node->right;
	if ((!nl || !nr) && node->data == MARKED_NODE) return UNLINK_REQUIRED;

	hn = node->height;
	hl = NODE_HEIGHT(nl);
	hr = NODE_HEIGHT(nr);

	hn_new = 1 + MAX(hl, hr);
	balance = hl - hr;
	if (balance < -1 || balance > 1) return REBALANCE_REQUIRED;

	return (hn != hn_new) ? hn_new : NOTHING_REQUIRED;
}
//> `node` must be locked before calling.
static avl_node_t *fix_node_height(avl_node_t *node)
{
	int c = node_condition(node);
	if (c == REBALANCE_REQUIRED || c == UNLINK_REQUIRED) return node;
	if (c == NOTHING_REQUIRED) return NULL;
	node->height = c;
	return node->parent;
}
static avl_node_t *rotate_right(avl_node_t *parent, avl_node_t *n,
                                avl_node_t *nl, int hr, int hll,
                                avl_node_t *nlr, int hlr)
{
	int hn_new, balance_n, balance_l;

	BEGIN_SHRINKING(n);

	n->left = nlr;
	if (nlr) nlr->parent = n;
	nl->right = n;
	n->parent = nl;
	if (parent->left == n) parent->left = nl;
	else                  parent->right = nl;
	nl->parent = parent;

	hn_new = 1 + MAX(hlr, hr);
	n->height = hn_new;
	nl->height = 1 + MAX(hll, hn_new);

	END_SHRINKING(n);

	balance_n = hlr - hr;
	if (balance_n < -1 || balance_n > 1) return n;
	if ((!nlr || hr == 0) && n->data == MARKED_NODE) return n;

	balance_l = hll - hn_new;
	if (balance_l < -1 || balance_l > 1) return nl;
	if (hll == 0 && nl->data == MARKED_NODE) return nl;
	return fix_node_height(parent);
}
avl_node_t *rotate_right_over_left(avl_node_t *parent, avl_node_t *n,
                                   avl_node_t *nl, int hr, int hll,
                                   avl_node_t *nlr, int hlrl)
{
	avl_node_t *nlrl, *nlrr;
	int hlrr, hn_new, hl_new, balance_n, balance_lr;

	nlrl = nlr->left;
	nlrr = nlr->right;
	hlrr = NODE_HEIGHT(nlrr);

	BEGIN_SHRINKING(n);
	BEGIN_SHRINKING(nl);
	
	n->left = nlrr;
	if (nlrr) nlrr->parent = n;
	nl->right = nlrl;
	if (nlrl) nlrl->parent = nl;
	nlr->left = nl;
	nl->parent = nlr;
	nlr->right = n;
	n->parent = nlr;
	if (parent->left == n) parent->left = nlr;
	else                   parent->right = nlr;
	nlr->parent = parent;

	hn_new = 1 + MAX(hlrr, hr);
	n->height = hn_new;
	hl_new = 1 + MAX(hll, hlrl);
	nl->height = hl_new;
	nlr->height = 1 + MAX(hl_new, hn_new);

	END_SHRINKING(n);
	END_SHRINKING(nl);

	balance_n = hlrr - hr;
	if (balance_n < -1 || balance_n > 1) return n;
//	if ((!nlrr || hr == 0) && n->data == MARKED_NODE) return n;

	balance_lr = hl_new - hn_new;
	if (balance_lr < -1 || balance_lr > 1) return nlr;

	return fix_node_height(parent);
}
static avl_node_t *rebalance_left(avl_node_t *parent, avl_node_t *n,
                                  avl_node_t *nr, int hl);
static avl_node_t *rebalance_right(avl_node_t *parent, avl_node_t *n,
                                   avl_node_t *nl, int hr)
{
	avl_node_t *nlr, *ret;
	int hl, hll, hlr, hlrl, balance;

	LOCK(&nl->lock);

	hl = nl->height;
	if (hl - hr <= 1) {
		UNLOCK(&nl->lock);
		return n;
	}

	nlr = nl->right;
	hll = NODE_HEIGHT(nl->left);
	hlr = NODE_HEIGHT(nlr);
	if (hll >= hlr) {
		ret = rotate_right(parent, n, nl, hr, hll, nlr, hlr);
		UNLOCK(&nl->lock);
		return ret;
	}

	LOCK(&nlr->lock);
	hlr = nlr->height;
	if (hll >= hlr) {
		ret = rotate_right(parent, n, nl, hr, hll, nlr, hlr);
		UNLOCK(&nlr->lock);
		UNLOCK(&nl->lock);
		return ret;
	}

	hlrl = NODE_HEIGHT(nlr->left);
	balance = hll - hlrl;
//	if (balance >= -1 && balance <= 1 &&
//	    !((hll == 0 || hlrl == 0) && nl->data == MARKED_NODE)) {
//	if (balance >= -1 && balance <= 1 && !(hll == 0 || hlrl == 0)) {
//	if (1) {
		ret = rotate_right_over_left(parent, n, nl, hr, hll, nlr, hlrl);
		UNLOCK(&nlr->lock);
		UNLOCK(&nl->lock);
		return ret;
//	}
//	UNLOCK(&nlr->lock);
//
//	ret = rebalance_left(n, nl, nlr, hll);
//	UNLOCK(&nl->lock);
//	return ret;
}
static avl_node_t *rotate_left(avl_node_t *parent, avl_node_t *n, int hl,
                               avl_node_t *nr, avl_node_t *nrl, int hrl, int hrr)
{
	int hn_new, balance_n, balance_r;

	BEGIN_SHRINKING(n);

	n->right = nrl;
	if (nrl) nrl->parent = n;
	nr->left = n;
	n->parent = nr;
	if (parent->left == n) parent->left = nr;
	else                   parent->right = nr;
	nr->parent = parent;

	hn_new = 1 + MAX(hl, hrl);
	n->height = hn_new;
	nr->height = 1 + MAX(hn_new, hrr);

	END_SHRINKING(n);

	balance_n = hrl - hl;
	if (balance_n < -1 || balance_n > 1) return n;
	if ((!nrl || hl == 0) && n->data == MARKED_NODE) return n;

	balance_r = hrr - hn_new;
	if (balance_r < -1 || balance_r > 1) return nr;
	if (hrr == 0 && nr->data == MARKED_NODE) return nr;
	return fix_node_height(parent);

}
static avl_node_t *rotate_left_over_right(avl_node_t *parent, avl_node_t *n, int hl,
                                          avl_node_t *nr, avl_node_t *nrl,
                                          int hrr, int hrlr)
{
	avl_node_t *nrll, *nrlr;
	int hrll, hn_new, hr_new, balance_n, balance_rl;

	nrll = nrl->left;
	nrlr = nrl->right;
	hrll = NODE_HEIGHT(nrll);

	BEGIN_SHRINKING(n);
	BEGIN_SHRINKING(nr);
	
	n->right = nrll;
	if (nrll) nrll->parent = n;
	nr->left = nrlr;
	if (nrlr) nrlr->parent = nr;
	nrl->right = nr;
	nr->parent = nrl;
	nrl->left = n;
	n->parent = nrl;
	if (parent->left == n) parent->left = nrl;
	else                   parent->right = nrl;
	nrl->parent = parent;

	hn_new = 1 + MAX(hl, hrll);
	n->height = hn_new;
	hr_new = 1 + MAX(hrlr, hrr);
	nr->height = hr_new;
	nrl->height = 1 + MAX(hn_new, hr_new);

	END_SHRINKING(n);
	END_SHRINKING(nr);

	balance_n = hrll - hl;
	if (balance_n < -1 || balance_n > 1) return n;
//	if ((!nrll || hl == 0) && n->data == MARKED_NODE) return n;

	balance_rl = hr_new - hn_new;
	if (balance_rl < -1 || balance_rl > 1) return nrl;

	return fix_node_height(parent);
}
static avl_node_t *rebalance_left(avl_node_t *parent, avl_node_t *n,
                                  avl_node_t *nr, int hl)
{
	int hr, hrl, hrr, hrlr, balance;
	avl_node_t *nrl, *ret;

	LOCK(&nr->lock);
	hr = nr->height;
	if (hl - hr >= -1) {
		UNLOCK(&nr->lock);
		return n;
	}

	nrl = nr->left;
	hrl = NODE_HEIGHT(nrl);
	hrr = NODE_HEIGHT(nr->right);
	if (hrr >= hrl) {
		ret = rotate_left(parent, n, hl, nr, nrl, hrl, hrr);
		UNLOCK(&nr->lock);
		return ret;
	}

	LOCK(&nrl->lock);
	hrl = nrl->height;
	if (hrr >= hrl) {
		ret = rotate_left(parent, n, hl, nr, nrl, hrl, hrr);
		UNLOCK(&nrl->lock);
		UNLOCK(&nr->lock);
		return ret;
	}

	hrlr = NODE_HEIGHT(nrl->right);
	balance = hrr - hrlr;
//	if (balance >= -1 && balance <= 1 &&
//	    !((hrr == 0 || hrlr == 0) && nr->data == MARKED_NODE)) {
//	if (balance >= -1 && balance <= 1 && !(hrr == 0 || hrlr == 0)) {
//	if (1) {
		ret = rotate_left_over_right(parent, n, hl, nr, nrl, hrr, hrlr);
		UNLOCK(&nrl->lock);
		UNLOCK(&nr->lock);
		return ret;
//	}
//	UNLOCK(&nrl->lock);
//
//	ret = rebalance_right(n, nr, nrl, hrr);
//	UNLOCK(&nr->lock);
//	return ret;
}
static int attempt_node_unlink(avl_node_t *parent, avl_node_t *n)
{
	avl_node_t *l = n->left,
               *r = n->right;
	avl_node_t *splice = (l != NULL) ? l : r;
	avl_node_t *parentl = parent->left,
	           *parentr = parent->right;

	if (parentl != n && parentr != n) return 0;
	if (l != NULL && r != NULL) return 0;
	if (parentl == n) parent->left = splice;
	else              parent->right = splice;
	if (splice) splice->parent = parent;

	n->version = UNLINKED;
	return 1;
}
//> `node` and `parent` must be locked before calling.
static avl_node_t *rebalance_node(avl_node_t *parent, avl_node_t *n)
{
	avl_node_t *nl = n->left, *nr = n->right;
	int hn, hl, hr, hn_new, balance;

	if ((!nl || !nr) && n->data == MARKED_NODE) {
		if (attempt_node_unlink(parent, n)) return fix_node_height(parent);
		else                                return n;
	}

	hn = n->height;
	hl = NODE_HEIGHT(nl);
	hr = NODE_HEIGHT(nr);
	hn_new = 1 + MAX(hl, hr);
	balance = hl - hr;

	if (balance > 1) return rebalance_right(parent, n, nl, hr);
	if (balance < -1) return rebalance_left(parent, n, nr, hl);
	if (hn != hn_new) {
		n->height = hn_new;
		return fix_node_height(parent);
	}
	return NULL;
}
static void fix_height_and_rebalance(avl_node_t *node)
{
	avl_node_t *node_old;
	while (node && node->parent) {
		int condition = node_condition(node);

		if (condition == NOTHING_REQUIRED || node->version == UNLINKED) return;

		if (condition != UNLINK_REQUIRED && condition != REBALANCE_REQUIRED) {
			node_old = node;
			LOCK(&node_old->lock);
			node = fix_node_height(node);
			UNLOCK(&node_old->lock);
			continue;
		}

		avl_node_t *parent = node->parent;
		LOCK(&parent->lock);
		if (parent->version == UNLINKED || node->parent != parent) {
			UNLOCK(&parent->lock);
			continue;
		}
		node_old = node;
		LOCK(&node_old->lock);
		node = rebalance_node(parent, node);
		UNLOCK(&node_old->lock);
		UNLOCK(&parent->lock);
	}
}
/*****************************************************************************/

int attempt_insert(map_key_t key, void *data, avl_node_t *node, int dir,
                   long long version)
{
	LOCK(&node->lock);
	if (node->version != version || GET_CHILD_DIR(node, dir)) {
		UNLOCK(&node->lock);
		return RETRY;
	}

	avl_node_t *new_node = avl_node_new(key, data, 1, 0, node);
	if (dir == LEFT) node->left  = new_node;
	else             node->right = new_node;
	UNLOCK(&node->lock);

	fix_height_and_rebalance(node);

	return 1;
}

int attempt_relink(avl_node_t *node)
{
	int ret;
	LOCK(&node->lock);
	if (node->version == UNLINKED) {
		ret = RETRY;
	} else if (node->data == MARKED_NODE) {
		node->data = NULL;
		ret = 1;
	} else {
		ret = 0;
	}
	UNLOCK(&node->lock);
	return ret;
}

int attempt_put(map_key_t key, void *data, avl_node_t *node, int dir,
                long long version)
{
	avl_node_t *child;
	int next_dir, ret = RETRY;
	long long child_version;

	do {
		child = GET_CHILD_DIR(node, dir);
		SW_BARRIER();

		//> The node version has changed. Must retry.
		if (node->version != version) return RETRY;

		if (child == NULL) {
			ret = attempt_insert(key, data, node, dir, version);
		} else {
			next_dir = (KEY_CMP(key, child->key) < 0) ? LEFT : RIGHT;
			if (KEY_CMP(key, child->key) == 0) {
				ret = attempt_relink(child);
			} else {
				child_version = child->version;
				if (IS_SHRINKING(child_version)) {
					wait_until_not_changing(child);
				} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
					if (node->version != version) return RETRY;
					ret = attempt_put(key, data, child, next_dir, child_version);
				}
			}
		}
	} while (ret == RETRY);

	return ret;
}

int _avl_insert_helper(avl_t *avl, map_key_t key, void *data)
{
	int ret;
	ret = attempt_put(key, data, avl->root, RIGHT, 0);
	assert(ret != RETRY);
	return ret;
}

#define CAN_UNLINK(n) ((n)->left == NULL || (n)->right == NULL)
int attempt_rm_node(avl_node_t *par, avl_node_t *n)
{
	if (n->data == MARKED_NODE) return 0;

	if (!CAN_UNLINK(n)) {
		//> Internal node, just mark it
		LOCK(&n->lock);
		if (n->version == UNLINKED || CAN_UNLINK(n)) {
			UNLOCK(&n->lock);
			return RETRY;
		}
		if (n->data != MARKED_NODE) {
			n->data = MARKED_NODE;
			UNLOCK(&n->lock);
			return 1;
		} else {
			UNLOCK(&n->lock);
			return 0;
		}
	}

	//> External node, can remove it from the tree
	LOCK(&par->lock);
	if (par->version == UNLINKED || n->parent != par) {
		UNLOCK(&par->lock);
		return RETRY;
	}
	LOCK(&n->lock);
	if (n->version == UNLINKED || par->version == UNLINKED || n->parent != par) {
		UNLOCK(&n->lock);
		UNLOCK(&par->lock);
		return RETRY;
	}

	if (n->data == MARKED_NODE) {
		UNLOCK(&n->lock);
		UNLOCK(&par->lock);
		return 0;
	}

	n->data = MARKED_NODE;
	if (CAN_UNLINK(n)) {
		avl_node_t *c = (n->left == NULL) ? n->right : n->left;
		if (par->left == n) par->left = c;
		else                par->right = c;
		if (c != NULL) c->parent = par;
		n->version = UNLINKED;
	}
	UNLOCK(&n->lock);
	UNLOCK(&par->lock);

	fix_height_and_rebalance(par);

	return 1;
}

int attempt_remove(map_key_t key, avl_node_t *node, int dir, long long version)
{
	avl_node_t *child;
	int next_dir, ret = RETRY;
	long long child_version;

	do {
		child = GET_CHILD_DIR(node, dir);
		SW_BARRIER();

		//> The node version has changed. Must retry.
		if (node->version != version) return RETRY;

		if (child == NULL) return 0;

		next_dir = (KEY_CMP(key, child->key) < 0) ? LEFT : RIGHT;
		if (KEY_CMP(key, child->key) == 0) {
			ret = attempt_rm_node(node, child);
		} else {
			child_version = child->version;
			if (IS_SHRINKING(child_version)) {
				wait_until_not_changing(child);
			} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
				if (node->version != version) return RETRY;
				ret = attempt_remove(key, child, next_dir, child_version);
			}
		}
	} while (ret == RETRY);

	return ret;
}

int _avl_delete_helper(avl_t *avl, map_key_t key)
{
	int ret;
	ret = attempt_remove(key, avl->root, RIGHT, 0);
	assert(ret != RETRY);
	return ret;
}

int attempt_update(map_key_t key, void *data, avl_node_t *node, int dir,
                   long long version)
{
	avl_node_t *child;
	int next_dir, ret = RETRY;
	long long child_version;

	do {
		child = GET_CHILD_DIR(node, dir);
		SW_BARRIER();

		//> The node version has changed. Must retry.
		if (node->version != version) return RETRY;

		if (child == NULL) {
			ret = attempt_insert(key, data, node, dir, version);
			break;
		}

		next_dir = (KEY_CMP(key, child->key) < 0) ? LEFT : RIGHT;
		if (KEY_CMP(key, child->key) == 0) {
			if (child->data == MARKED_NODE) {
				ret = attempt_relink(child);
			} else {
				ret = attempt_rm_node(node, child);
				if (ret != RETRY) ret += 2;
			}
		} else {
			child_version = child->version;
			if (IS_SHRINKING(child_version)) {
				wait_until_not_changing(child);
			} else if (child_version != UNLINKED && child == GET_CHILD_DIR(node, dir)) {
				if (node->version != version) return RETRY;
				ret = attempt_update(key, data, child, next_dir, child_version);
			}
		}
	} while (ret == RETRY);

	return ret;
}

int _avl_update_helper(avl_t *avl, map_key_t key, void *data)
{
	int ret;
	ret = attempt_update(key, data, avl->root, RIGHT, 0);
	assert(ret != RETRY);
	return ret;
}

/******************************************************************************/
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{
	avl_t *avl;
	printf("Size of tree node is %lu\n", sizeof(avl_node_t));
	avl = avl_new();
	avl->root = avl_node_new(MAX_KEY, 0, 0, 0, NULL);
	return avl;
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(avl_node_t));
	return NULL;
}

void map_tdata_print(void *thread_data)
{
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
}

int map_lookup(void *avl, void *thread_data, map_key_t key)
{
	int ret;
	ret = _avl_lookup_helper(avl, key);
	return ret;
}

int map_rquery(void *map, void *tdata, map_key_t key1, map_key_t key2)
{
	printf("Range query not yet implemented\n");
	return 0;
}

int map_insert(void *avl, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = _avl_insert_helper(avl, key, value);
	return ret;
}

int map_delete(void *avl, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = _avl_delete_helper(avl, key);
	return ret;
}

int map_update(void *avl, void *thread_data, map_key_t key, void *value)
{
	int ret = 0;
	ret = _avl_update_helper(avl, key, value);
	return ret;
}
int map_validate(void *avl)
{
	int ret = 1;
	ret = _avl_validate_helper(((avl_t *)avl)->root->right);
	return ret;
}

char *map_name()
{
	return "avl_bronson";
}

void map_print(void *map)
{
	avl_print(map);
}
