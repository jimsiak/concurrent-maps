#include <stdint.h>
#include <stdlib.h>
#include <string.h> /* memset() */
#include <limits.h>
#include <pthread.h> /* pthread_spinlock_t ... */

#include "../../../map.h"
#include "../../../key/key.h"
#include "arch.h" /* CACHE_LINE_SIZE */
#include "alloc.h"

typedef pthread_spinlock_t lock_t;
#define INIT_LOCK(lock) pthread_spin_init(lock, PTHREAD_PROCESS_SHARED)
#define LOCK(lock)      pthread_spin_lock(lock)
#define UNLOCK(lock)    pthread_spin_unlock(lock)
#define TRYLOCK(lock)   pthread_spin_trylock(lock)

#define NODE_HAS_PARENT
#define NODE_HAS_SUCC_AND_PRED
#define NODE_HAS_TREE_AND_SUCC_LOCKS
#define NODE_HAS_LR_HEIGHTS
#include "avl.h"
#include "validate.h"
#include "print.h"

static __thread void *nalloc;

enum {
	STATS_IND_INS = 0,
	STATS_IND_DEL = 1,
	STATS_IND_NO
};
typedef struct {
	int tid;
	unsigned long long retries[STATS_IND_NO];
	unsigned long long locks[STATS_IND_NO];
	unsigned long long unlocks[STATS_IND_NO];
} tdata_t;
tdata_t *tdata_new(int tid)
{
	tdata_t *ret;
	XMALLOC(ret, 1);
	memset(ret, 0, sizeof(*ret));
	ret->tid = tid;
	return ret;
}
void tdata_print(tdata_t *tdata)
{
	int i;
	printf("%3d\t |", tdata->tid);
	for (i=0; i < STATS_IND_NO; i++)
		printf("\t%12llu", tdata->retries[i]);
	printf(" |");
	for (i=0; i < STATS_IND_NO; i++)
		printf("\t%12llu", tdata->locks[i]);
	printf(" |");
	for (i=0; i < STATS_IND_NO; i++)
		printf("\t%12llu", tdata->unlocks[i]);
	printf("\n");
}
void tdata_add(tdata_t *d1, tdata_t *d2, tdata_t *dst)
{
	int i;
	for (i=0; i < STATS_IND_NO; i++) {
		dst->retries[i] = d1->retries[i] + d2->retries[i];
		dst->locks[i] = d1->locks[i] + d2->locks[i];
		dst->unlocks[i] = d1->unlocks[i] + d2->unlocks[i];
	}
}

#define MARK(n) ((n)->data = (void *)0xFFLLU)
#define IS_MARKED(n) ((n)->data == (void *)0xFFLLU)

static avl_node_t *search(avl_t *avl, map_key_t k)
{
	avl_node_t *child;
	avl_node_t *n = avl->root;
	map_key_t curr_key;

	while (1) {
		KEY_COPY(curr_key, n->key);
		if (KEY_CMP(curr_key, k) == 0) return n;
		child = (KEY_CMP(curr_key, k) < 0) ? n->right : n->left;
		if (child == NULL) return n;
		n = child;
	}
}

static int _avl_lookup_helper(avl_t *avl, map_key_t k)
{
	avl_node_t *n = search(avl, k);
	while (KEY_CMP(n->key, k) > 0 && KEY_CMP(n->pred->key, k) >= 0) n = n->pred;
	while (KEY_CMP(n->key, k) < 0 && KEY_CMP(n->succ->key, k) <= 0) n = n->succ;
	return (KEY_CMP(n->key, k) == 0 && !IS_MARKED(n));
}

/*****************************************************************************/
/*                                REBALANCING                                */
/*****************************************************************************/
static void update_child(avl_node_t *, avl_node_t *, avl_node_t *);
static avl_node_t *lock_parent(avl_node_t *, tdata_t *);
static void rotate(avl_node_t *child, avl_node_t *n, avl_node_t *parent,
                   char left_rotation)
{
	update_child(parent, n, child);
	n->parent = child;
	if (left_rotation) {
		update_child(n, child, child->left);
		child->left = n;
		n->rheight = child->lheight;
		child->lheight = MAX(n->lheight, n->rheight) + 1;
	} else {
		update_child(n, child, child->right);
		child->right = n;
		n->lheight = child->rheight;
		child->rheight = MAX(n->lheight, n->rheight) + 1;
	}
}
//> Returns 1 if height has changed, 0 otherwise
static char update_height(avl_node_t *child, avl_node_t *node, char is_left)
{
	short int new_height, old_height;
	new_height = (child == NULL) ? 0 : MAX(child->lheight, child->rheight) + 1;
	old_height = is_left ? node->lheight : node->rheight;
	if (old_height != new_height) {
		if (is_left) node->lheight = new_height;
		else         node->rheight = new_height;
	}
	return (old_height != new_height);
}
//> `node` and `child` must be tree locked
static void rebalance(avl_t *avl, avl_node_t *node, avl_node_t *child)
{
	char is_left, updated;
	short int balance, ch_balance;
	avl_node_t *grandchild, *parent = NULL;

	//> At the start of each iteration `node` and `child` are locked (if NULL).
	while (1) {
		//> Rebalance has reached the root.
		if (node == avl->root || node == avl->root->parent)
			goto unlock_and_out;

		is_left = (node->left == child);
		updated = update_height(child, node, is_left);
		if (!child && !node->left && !node->right) {
			node->lheight = 0;
			node->rheight = 0;
			updated = 1;
		}
		balance = node->lheight - node->rheight;
		if (!updated && ABS(balance) < 2) goto unlock_and_out;

		//> `child` is not the appropriate, release the previous one and
		//>  lock the correct one.
		if ((is_left && balance <= -2) || (!is_left && balance >= 2)) {
			if (child) UNLOCK(&child->tree_lock);
			is_left = !is_left;
			child = is_left ? node->left : node->right;
			if (TRYLOCK(&child->tree_lock) != 0) {
				UNLOCK(&node->tree_lock);
				return; // XXX no restart here as in the paper
			}
		}

		if (ABS(balance) >= 2) { // Need to rebalance
			//> First rotation if needed
			ch_balance = (child == NULL) ? 0 : child->lheight - child->rheight;
			if ((is_left && ch_balance < 0) || (!is_left && ch_balance > 0)) {
				grandchild = is_left ? child->right : child->left;
				if (!grandchild) goto unlock_and_out; // JIMSIAK
				if (TRYLOCK(&grandchild->tree_lock) != 0) {
					goto unlock_and_out; // XXX no restart here as in the paper
				}
				rotate(grandchild, child, node, is_left);
				UNLOCK(&child->tree_lock);
				child = grandchild;
			}

			//> Second (or first) rotation
			parent = lock_parent(node, NULL);
			rotate(child, node, parent, !is_left);

			UNLOCK(&node->tree_lock);
			node = parent;
		} else { // No rotations needed but the height has been updated
			if (child) UNLOCK(&child->tree_lock);
			child = node;
			node = lock_parent(node, NULL);
		}
	}

unlock_and_out:
	if (child) UNLOCK(&child->tree_lock);
	if (node) UNLOCK(&node->tree_lock);
	return;
}
/*****************************************************************************/

//> Returs n's parent node, and locks its `tree_lock` as well.
static avl_node_t *lock_parent(avl_node_t *n, tdata_t *tdata)
{
	avl_node_t *p;
	while (1) {
		p = n->parent;
		LOCK(&p->tree_lock);
		if (n->parent == p && !IS_MARKED(p)) return p;
		UNLOCK(&p->tree_lock);
	}
}

static avl_node_t *choose_parent(avl_node_t *p, avl_node_t *s,
                                 avl_node_t *first_cand, tdata_t *tdata)
{
	avl_node_t *candidate = (first_cand == p || first_cand == s) ? first_cand : p;

	while (1) {
		LOCK(&candidate->tree_lock);
		if (candidate == p) {
			if (!candidate->right) return candidate;
			UNLOCK(&candidate->tree_lock);
			candidate = s;
		} else {
			if (!candidate->left) return candidate;
			UNLOCK(&candidate->tree_lock);
			candidate = p;
		}
	}
}

// `p` is tree locked when called
static void insert_to_tree(avl_t *avl, avl_node_t *p, avl_node_t *n, tdata_t *tdata)
{
	n->parent = p;
	if (KEY_CMP(p->key, n->key) < 0) {
		p->right = n;
		p->rheight = 1;
	} else {
		p->left = n;
		p->lheight = 1;
	}
	rebalance(avl, lock_parent(p, NULL), p);
}

static int _avl_insert_helper(avl_t *avl, map_key_t k, void *v, tdata_t *tdata)
{
	avl_node_t *node, *p, *s, *new, *parent;

	while(1) {
		node = search(avl, k);
		p = (KEY_CMP(node->key, k) >= 0) ? node->pred : node;
		LOCK(&p->succ_lock);
		s = p->succ;

		if (KEY_CMP(k, p->key) > 0 && KEY_CMP(k, s->key) <= 0 && !IS_MARKED(p)) {
			//> Key already in the tree
			if (KEY_CMP(s->key, k) == 0) {
				UNLOCK(&p->succ_lock);
				return 0;
			}
			//> Ordering insertion
			new = avl_node_new(k, v);
			parent = choose_parent(p, s, node, tdata);
			new->succ = s;
			new->pred = p;
			new->parent = parent;
			s->pred = new;
			p->succ = new;
			UNLOCK(&p->succ_lock);
			//> Tree Layout insertion
			insert_to_tree(avl, parent, new, tdata);
			return 1;
		}
		UNLOCK(&p->succ_lock);
	}
}

//> Acquires the required `tree_lock` locks.
//> - If `n` has 0 or 1 child: `n->parent`, `n` and `n->NOT_NULL_CHILD`
//>   are locked.
//> - If `n` has 2 children:  `n->parent`, `n`, `n->succ->parent`,
//>   `n->succ` and `n->succ->right` are locked.
static int acquire_tree_locks(avl_node_t *n, tdata_t *tdata)
{
	avl_node_t *s, *parent, *sp;
	long long retries = -1;

	while (1) {
		retries++;

		// JIMSIAK, this is necessary, otherwise performance collapses
		volatile int sum = 0;
		int i; for (i=0; i < retries * 9; i++) sum++;

		LOCK(&n->tree_lock);
		parent = lock_parent(n, tdata); // NEW

		//> Node has ONE or NO child
		if (!n->left || !n->right) {
			if (n->right) {
				if (TRYLOCK(&n->right->tree_lock) != 0) {
					UNLOCK(&parent->tree_lock);
					UNLOCK(&n->tree_lock);
					continue;
				}
			} else if (n->left) {
				if (TRYLOCK(&n->left->tree_lock) != 0) {
					UNLOCK(&parent->tree_lock);
					UNLOCK(&n->tree_lock);
					continue;
				}
			}
			return 0;
		}

		//> Node has TWO children
		s = n->succ;
		sp = NULL;
		if (s->parent != n) {
			sp = s->parent;
			if (TRYLOCK(&sp->tree_lock) != 0) {
				UNLOCK(&parent->tree_lock);
				UNLOCK(&n->tree_lock);
				continue;
			}
			if (sp != s->parent || IS_MARKED(sp)) {
				UNLOCK(&sp->tree_lock);
				UNLOCK(&parent->tree_lock);
				UNLOCK(&n->tree_lock);
				continue;
			}
		}

		if (TRYLOCK(&s->tree_lock) != 0) {
			if (sp) UNLOCK(&sp->tree_lock);
			UNLOCK(&parent->tree_lock);
			UNLOCK(&n->tree_lock);
			continue;
		}

		if (s->right) {
			if (TRYLOCK(&s->right->tree_lock)) {
				UNLOCK(&s->tree_lock);
				if (sp) UNLOCK(&sp->tree_lock);
				UNLOCK(&parent->tree_lock);
				UNLOCK(&n->tree_lock);
				continue;
			}
		}

		return 1;
	}
}

static void update_child(avl_node_t *parent, avl_node_t *old_ch, avl_node_t *new_ch)
{
	if (parent->left == old_ch) parent->left = new_ch;
	else                        parent->right = new_ch;
	if (new_ch != NULL) new_ch->parent = parent;
}

static void remove_from_tree(avl_t *avl, avl_node_t *n, int has_two_children,
                             tdata_t *tdata)
{
	avl_node_t *child, *parent, *s, *sparent, *schild;

	if (!has_two_children) {
		child = (n->right == NULL) ? n->left : n->right;
		parent = n->parent;
		update_child(parent, n, child);
		UNLOCK(&n->tree_lock);
		rebalance(avl, parent, child);
		return;
	} else {
		parent = n->parent;
		s = n->succ;
		schild = s->right;
		sparent = s->parent;
		update_child(sparent, s, schild);
		s->left = n->left;
		s->right = n->right;
		s->lheight = n->lheight;
		s->rheight = n->rheight;
		n->left->parent = s;
		if (n->right) n->right->parent = s;
		update_child(parent, n, s);
		UNLOCK(&parent->tree_lock);
		UNLOCK(&n->tree_lock);
		if (sparent == n) sparent = s;
		else UNLOCK(&s->tree_lock);
		rebalance(avl, sparent, schild);
		return;
	}
}

static int _avl_delete_helper(avl_t *avl, map_key_t k, tdata_t *tdata)
{
	avl_node_t *node, *p, *s, *s_succ;
	int has_two_children;

	while (1) {
		node = search(avl, k);
		p = (KEY_CMP(node->key, k) >= 0) ? node->pred : node;
		LOCK(&p->succ_lock);
		s = p->succ;

		if (KEY_CMP(k, p->key) > 0 && KEY_CMP(k, s->key) <= 0 && !IS_MARKED(p)) {
			if (KEY_CMP(s->key, k) > 0) {
				UNLOCK(&p->succ_lock);
				return 0;
			}
			LOCK(&s->succ_lock);
			has_two_children = acquire_tree_locks(s, tdata);
			MARK(s);
			s_succ = s->succ;
			s_succ->pred = p;
			p->succ = s_succ;
			UNLOCK(&s->succ_lock);
			UNLOCK(&p->succ_lock);
			remove_from_tree(avl, s, has_two_children, tdata);
			return 1; 
		}
		UNLOCK(&p->succ_lock);
	}
}

static int _avl_update_helper(avl_t *avl, map_key_t k, void *v, tdata_t *tdata)
{
	avl_node_t *node, *p, *s, *new, *parent, *s_succ;
	int has_two_children, op_is_insert = -1;

	while(1) {
		node = search(avl, k);
		p = (KEY_CMP(node->key, k) >= 0) ? node->pred : node;
		LOCK(&p->succ_lock);
		s = p->succ;

		if (KEY_CMP(k, p->key) > 0 && KEY_CMP(k, s->key) <= 0 && !IS_MARKED(p)) {
			if (op_is_insert == -1) {
				if (KEY_CMP(s->key, k) == 0) op_is_insert = 0;
				else                         op_is_insert = 1;
			}

			if (op_is_insert) {
				//> Key already in the tree
				if (KEY_CMP(s->key, k) == 0) {
					UNLOCK(&p->succ_lock);
					return 0;
				}
				//> Ordering insertion
				new = avl_node_new(k, v);
				parent = choose_parent(p, s, node, tdata);
				new->succ = s;
				new->pred = p;
				new->parent = parent;
				s->pred = new;
				p->succ = new;
				UNLOCK(&p->succ_lock);
				//> Tree Layout insertion
				insert_to_tree(avl, parent, new, tdata);
				return 1;
			} else {
				if (KEY_CMP(s->key, k) > 0) {
					UNLOCK(&p->succ_lock);
					return 2;
				}
				LOCK(&s->succ_lock);
				has_two_children = acquire_tree_locks(s, tdata);
				MARK(s);
				s_succ = s->succ;
				s_succ->pred = p;
				p->succ = s_succ;
				UNLOCK(&s->succ_lock);
				UNLOCK(&p->succ_lock);
				remove_from_tree(avl, s, has_two_children, tdata);
				return 3; 
			}
		}
		UNLOCK(&p->succ_lock);
	}
}

/******************************************************************************/
/*            Map interface implementation                                    */
/******************************************************************************/
void *map_new()
{

	avl_t *avl = avl_new();
	avl_node_t *parent, *root;

	printf("Size of tree node is %lu\n", sizeof(avl_node_t));

	parent = avl_node_new(MIN_KEY, 0);
	root = avl_node_new(MAX_KEY, 0);
	root->pred = parent;
	root->succ = parent;
	root->parent = parent;
	parent->right = root;
	parent->succ = root;
	avl->root = root;
	return avl;
}

void *map_tdata_new(int tid)
{
	nalloc = nalloc_thread_init(tid, sizeof(avl_node_t));
	return tdata_new(tid);
}

void map_tdata_print(void *thread_data)
{
	tdata_print(thread_data);
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	tdata_add(d1, d2, dst);
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

int map_insert(void *avl, void *thread_data, map_key_t key, void *data)
{
	int ret = 0;
	ret = _avl_insert_helper(avl, key, data, thread_data);
	return ret;
}

int map_delete(void *avl, void *thread_data, map_key_t key)
{
	int ret = 0;
	ret = _avl_delete_helper(avl, key, thread_data);
	return ret;
}

int map_update(void *avl, void *thread_data, map_key_t key, void *data)
{
	int ret = 0;
	ret = _avl_update_helper(avl, key, data, thread_data);
	return ret;
}

int map_validate(void *avl)
{
	int ret = 1;
	ret = _avl_validate_helper(((avl_t *)avl)->root->left);
	return ret;
}

char *map_name()
{
	return "avl_drachsler";
}
