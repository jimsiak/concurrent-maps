#ifndef _HT_H_
#define _HT_H_

/******************************************************************************/
/* A simple hash table implementation.                                        */
/******************************************************************************/
#define HT_LEN 16
#define HT_MAX_BUCKET_LEN 64
#define HT_GET_BUCKET(key) ((((long long)(key)) >> 4) % HT_LEN)
typedef struct {
	unsigned short bucket_next_index[HT_LEN];
	// Even numbers (0,2,4) are keys, odd numbers are values.
	void *entries[HT_LEN][HT_MAX_BUCKET_LEN * 2];
} ht_t;

static ht_t *ht_new()
{
	int i;
	ht_t *ret;

	XMALLOC(ret, 1);
	memset(&ret->bucket_next_index[0], 0, sizeof(ret->bucket_next_index));
	memset(&ret->entries[0][0], 0, sizeof(ret->entries));
	return ret;
}

static void ht_reset(ht_t *ht)
{
	memset(&ht->bucket_next_index[0], 0, sizeof(ht->bucket_next_index));
}

static void ht_insert(ht_t *ht, void *key, void *value)
{
	int bucket = HT_GET_BUCKET(key);
	unsigned short bucket_index = ht->bucket_next_index[bucket];

	ht->bucket_next_index[bucket] += 2;

	assert(bucket_index < HT_MAX_BUCKET_LEN * 2);

	ht->entries[bucket][bucket_index] = key;
	ht->entries[bucket][bucket_index+1] = value;
}

static void *ht_get(ht_t *ht, void *key)
{
	int bucket = HT_GET_BUCKET(key);
	int i;

	for (i=0; i < ht->bucket_next_index[bucket]; i+=2)
		if (key == ht->entries[bucket][i])
			return ht->entries[bucket][i+1];

	return NULL;
}

static void ht_print(ht_t *ht)
{
	int i, j;

	for (i=0; i < HT_LEN; i++) {
		printf("BUCKET[%3d]:", i);
		for (j=0; j < ht->bucket_next_index[i]; j+=2)
			printf(" (%p, %p)", ht->entries[i][j], ht->entries[i][j+1]);
		printf("\n");
	}
}
/******************************************************************************/

#endif /* _HT_H_ */
