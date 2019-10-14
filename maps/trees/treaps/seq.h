#ifndef _SEQ_H_
#define _SEQ_H_

#include "treap.h"
#include "stack.h"

static void treap_traverse_with_stack(treap_t *treap, map_key_t key, stack_t *stack)
{
	void *root = treap->root;
	treap_node_internal_t *internal;

	stack_reset(stack);
	while (root != NULL && treap_node_is_internal(root)) {
		internal = root;
		stack_push(stack, internal);
		root = (KEY_CMP(key, internal->key) <= 0) ? internal->left : internal->right;
	}
	//> Also add the final external node in the stack
	if (root != NULL) stack_push(stack, root);
}

static void *treap_traverse(treap_t *treap, map_key_t key)
{
	void *root = treap->root;
	treap_node_internal_t *internal;

	while (root != NULL && treap_node_is_internal(root)) {
		internal = root;
		root = (KEY_CMP(key, internal->key) <= 0) ? internal->left : internal->right;
	}
	return root;
}

static int treap_seq_lookup(treap_t *treap, map_key_t key)
{
	treap_node_external_t *external = treap_traverse(treap, key);
	return ( (external != NULL) && (treap_node_external_indexof(external, key) != -1) );
}

static void treap_rebalance(treap_t *treap, stack_t *stack)
{
	treap_node_internal_t *curr, *parent, *gparent;

	while (1) {
		curr = stack_pop(stack);
		parent = stack_pop(stack);
		if (curr == NULL || parent == NULL || curr->weight <= parent->weight) break;

		if (curr == parent->left) {
			//> Right rotation
			parent->left = curr->right;
			curr->right = parent;
		} else {
			//> Left rotation
			parent->right = curr->left;
			curr->left = parent;
		}

		gparent = stack_pop(stack);
		if (gparent == NULL)              treap->root = curr;
		else if (parent == gparent->left) gparent->left = curr;
		else                              gparent->right = curr;

		stack_push(stack, gparent);
		stack_push(stack, curr);
	}
}

static void _do_insert(treap_t *treap, treap_node_external_t *external,
                       stack_t *stack, map_key_t key, void *value)
{
	treap_node_external_t *new_external;
	treap_node_internal_t *new_internal, *parent;

	//> 1. External node has space for one more key
	if (!treap_node_external_full(external)) {
		treap_node_external_insert(external, key, value);
		return;
	}

	//> 2. No space left in the external node, need to split
	new_external = treap_node_external_split(external);
	if (KEY_CMP(key, external->keys[external->nr_keys-1]) < 0)
		treap_node_external_insert(external, key, value);
	else
		treap_node_external_insert(new_external, key, value);

	new_internal = treap_node_new(external->keys[external->nr_keys-1], NULL, 1);
	new_internal->left = external;
	new_internal->right = new_external;

	parent = stack_pop(stack);
	if (parent == NULL) {
		treap->root = new_internal;
	} else {
		if (KEY_CMP(new_internal->key, parent->key) < 0) parent->left = new_internal;
		else                                             parent->right = new_internal;

		stack_push(stack, parent);
		stack_push(stack, new_internal);
		treap_rebalance(treap, stack);
	}
}

static int treap_seq_insert(treap_t *treap, map_key_t key, void *value)
{
	treap_node_external_t *external;
	int stack_sz, key_index;
	stack_t stack;

	treap_traverse_with_stack(treap, key, &stack);
	stack_sz = stack_size(&stack);

	//> 1. Empty treap
	if (stack_sz == 0) {
		treap->root = treap_node_new(key, value, 0);
		return 1;
	}
	
	external = stack_pop(&stack);
	key_index = treap_node_external_indexof(external, key);

	//> 2. Key already in the tree
	if (key_index != -1) return 0;

	//> 3. Key not in the tree, insert it
	_do_insert(treap, external, &stack, key, value);
	return 1;
}

static void _do_delete(treap_t *treap, treap_node_external_t *external,
                       stack_t *stack, int key_index)
{
	treap_node_internal_t *internal, *internal_parent;
	treap_node_external_t *sibling;

	treap_node_external_delete_index(external, key_index);

	//> 3. External node is now empty
	if (treap_node_external_empty(external)) {
		internal = stack_pop(stack);
		if (internal == NULL) {
			treap->root = NULL;
		} else {
			sibling = (external == internal->left) ? internal->right : internal->left;
			internal_parent = stack_pop(stack);
			if (internal_parent == NULL) {
				treap->root = sibling;
			} else {
				if (internal == internal_parent->left) internal_parent->left = sibling;
				else                                   internal_parent->right = sibling;
			}
		}
	}

}

static int treap_seq_delete(treap_t *treap, map_key_t key)
{
	treap_node_external_t *external;
	int stack_sz, key_index;
	stack_t stack;

	treap_traverse_with_stack(treap, key, &stack);
	stack_sz = stack_size(&stack);

	//> 1. Empty treap
	if (stack_sz == 0) return 0;

	external = stack_pop(&stack);
	key_index = treap_node_external_indexof(external, key);

	//> 2. Key not in the tree
	if (key_index == -1) return 0;

	_do_delete(treap, external, &stack, key_index);
	return 1;
}

static int treap_seq_update(treap_t *treap, map_key_t key, void *value)
{
	treap_node_external_t *external, *sibling;
	treap_node_internal_t *internal, *internal_parent;
	int stack_sz, key_index;
	stack_t stack;

	treap_traverse_with_stack(treap, key, &stack);
	stack_sz = stack_size(&stack);

	//> 1. Empty treap, insert
	if (stack_sz == 0) {
		treap->root = treap_node_new(key, value, 0);
		return 1; // XXX
	}

	external = stack_pop(&stack);
	key_index = treap_node_external_indexof(external, key);

	if (key_index == -1) {
		_do_insert(treap, external, &stack, key, value);
		return 1; // XXX
	} else {
		_do_delete(treap, external, &stack, key_index);
		return 3;
	}
}

static __thread map_key_t rquery_result[1000];

static int treap_seq_rquery(treap_t *treap, map_key_t key1, map_key_t key2, int *nkeys)
{
	void *curr, *prev = NULL;
	treap_node_external_t *external;
	treap_node_internal_t *internal;
	int stack_sz, key_index;
	stack_t stack;

	treap_traverse_with_stack(treap, key1, &stack);
	stack_sz = stack_size(&stack);
	if (stack_sz == 0) return 0;

	*nkeys = 0;
	while (1) {
		curr = stack_pop(&stack);
		if (curr == NULL) {
			break;
		} if (treap_node_is_internal(curr)) {
			internal = curr;
			if (prev == NULL) {
				//> New internal node to visit
				stack_push(&stack, internal);
				stack_push(&stack, internal->left);
			} else if (prev == internal->left) {
				//> Already visited node's left child
				stack_push(&stack, internal);
				stack_push(&stack, internal->right);
				prev = NULL;
			} else if (prev == internal->right) {
				//> Already visited node's both children
				prev = internal;
			}
		} else {
			external = curr;
			key_index = treap_node_external_indexof(external, key1);
			if (key_index == -1) key_index = 0;
			while (key_index < external->nr_keys &&
			       KEY_CMP(external->keys[key_index], key2) <= 0)
				rquery_result[(*nkeys)++] = external->keys[key_index++];
			if (key_index < external->nr_keys)
				break;
			prev = external;
		}
	}

	return 1;
}

#endif /* _SEQ_H_ */
