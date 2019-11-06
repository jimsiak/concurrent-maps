#ifndef _SEQ_DS_H_
#define _SEQ_DS_H_

#if SEQ_DS_TYPE_TREAP
#include "../trees/treaps/treap.h"
#include "../trees/treaps/seq.h"
#define seq_ds_t         treap_t
#define seq_ds_name      "treap"
#define seq_ds_new       treap_new
#define seq_ds_lookup    treap_seq_lookup
#define seq_ds_insert    treap_seq_insert
#define seq_ds_update    treap_seq_update
#define seq_ds_delete    treap_seq_delete
#define seq_ds_query     treap_seq_rquery
#define seq_ds_print     treap_print
#define seq_ds_split     treap_split
#define seq_ds_join      treap_join
#define seq_ds_max_key   treap_max_key
#define seq_ds_min_key   treap_min_key
#define seq_ds_size      treap_size
#endif


#endif /* _SEQ_DS_H_ */
