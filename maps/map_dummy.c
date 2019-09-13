#include <stdio.h>
#include <stdlib.h>
#include "../lib/log.h"

#define MSG() log_debug("Hello!!!\n")

void *map_tdata_new(int tid)
{
	MSG();
	return NULL;
}

void map_tdata_print(void *tdata)
{
	MSG();
}

void map_tdata_add(void *d1, void *d2, void *dst)
{
	MSG();
}

void *map_new()
{
	MSG();
	return NULL;
}

char *map_name()
{
	MSG();
	return "dummy-unimplemented";
}

int map_validate(void *rbt)
{
	MSG();
	return 1;
}

int map_lookup(void *map, void *tdata, int key)
{
	MSG();
	return 1;
}

int map_insert(void *map, void *tdata, int key, void *value)
{
	MSG();
	return 1;
}

int map_delete(void *map, void *tdata, int key)
{
	MSG();
	return 1;
}

int map_update(void *map, void *tdata, int key, void *value)
{
	MSG();
	return 1;
}

int map_rquery(void *map, void *tdata, int key1, int key2)
{
	MSG();
	return 1;
}

void map_print(void *rbt)
{
	MSG();
}
