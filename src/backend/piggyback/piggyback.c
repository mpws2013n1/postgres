/*-------------------------------------------------------------------------
 *
 * piggyback.c
 *	 piggyback metadata while execution of a query
 *
 *
 * IDENTIFICATION
 *	src/backend/piggyback/piggyback.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "piggyback/piggyback.h"
#include "miscadmin.h"
#include <limits.h>

void printIt() {
	printf("THIS IS PRINTED");
	return;
}
// Singleton Piggyback instance.
Piggyback *piggyback = NULL;

/*
 * Initialize piggyback if not already done.
 */
void initPiggyback() {
	piggyback = (Piggyback*) (malloc(sizeof(Piggyback)));
}

/*
 * Set root node to enable data collection.
 */
void setPiggybackRootNode(Plan *rootNode) {
	// Save root node for later processing.
	piggyback->root = rootNode;

	// Flag to recognize first processing of root node.
	piggyback->newProcessing = true;

	//init attribute list
	piggyback->columnNames = NIL;
}

void printMetaData() {
	printDistinctValues();
}

void printDistinctValues() {
	if (!piggyback)
		return;
	int i;

	StringInfoData buf;
	pq_beginmessage(&buf, 'X');
	pq_sendint(&buf, piggyback->numberOfAttributes, 4);

	for (i = 0; i < piggyback->numberOfAttributes; i++) {
		char * columnName = (char *) list_nth(piggyback->columnNames, i);
		long distinctValues = (long) hashset_num_items(
				piggyback->distinctValues[i]);
		printf("column %s (%d) has %ld distinct values.\n", columnName, i,
				distinctValues);
		pq_sendstring(&buf, columnName);
		pq_sendint(&buf, i, 4);
		pq_sendint(&buf, distinctValues, 4);
	}

	pq_endmessage(&buf);
}

//begin stolen hashset - https://github.com/avsej/hashset.c
static const unsigned int prime_1 = 73;
static const unsigned int prime_2 = 5009;

static const unsigned int nil = ULONG_MAX - 1;
static const unsigned int rem = ULONG_MAX - 2;

hashset_t hashset_create() {
	hashset_t set = calloc(1, sizeof(struct hashset_st));

	if (set == NULL) {
		return NULL;
	}
	set->nbits = 3;
	set->capacity = (size_t) (1 << set->nbits);
	set->mask = set->capacity - 1;
	set->items = calloc(set->capacity, sizeof(size_t));
	if (set->items == NULL) {
		hashset_destroy(set);
		return NULL;
	}
	int i;
	for (i = 0; i < set->capacity; i++) {
		set->items[i] = nil;
	}
	set->nitems = 0;
	return set;
}

size_t hashset_num_items(hashset_t set) {
	return set->nitems;
}

void hashset_destroy(hashset_t set) {
	if (set) {
		free(set->items);
	}
	free(set);
}

static int hashset_add_member(hashset_t set, void *item) {
	size_t value = (size_t) item;
	size_t ii;

	if (value == nil || value == rem) {
		return -1;
	}

	ii = set->mask & (prime_1 * value);

	while (set->items[ii] != nil && set->items[ii] != rem) {
		if (set->items[ii] == value) {
			return 0;
		} else {
			/* search free slot */
			ii = set->mask & (ii + prime_2);
		}
	}
	set->nitems++;
	set->items[ii] = value;
	return 1;
}

static void maybe_rehash(hashset_t set) {
	size_t *old_items;
	size_t old_capacity, ii;

	if ((float) set->nitems >= (size_t) ((double) set->capacity * 0.85)) {
		old_items = set->items;
		old_capacity = set->capacity;
		set->nbits++;
		set->capacity = (size_t) (1 << set->nbits);
		set->mask = set->capacity - 1;
		set->items = calloc(set->capacity, sizeof(size_t));
		int i;
		for (i = 0; i < set->capacity; i++) {
			set->items[i] = nil;
		}
		set->nitems = 0;
		//assert(set->items);
		for (ii = 0; ii < old_capacity; ii++) {
			hashset_add_member(set, (void *) old_items[ii]);
		}
		free(old_items);
	}
}

int hashset_add(hashset_t set, void *item) {
	int rv = hashset_add_member(set, item);
	maybe_rehash(set);
	return rv;
}

int hashset_remove(hashset_t set, void *item) {
	size_t value = (size_t) item;
	size_t ii = set->mask & (prime_1 * value);

	while (set->items[ii] != nil) {
		if (set->items[ii] == value) {
			set->items[ii] = rem;
			set->nitems--;
			return 1;
		} else {
			ii = set->mask & (ii + prime_2);
		}
	}
	return 0;
}

int hashset_is_member(hashset_t set, void *item) {
	size_t value = (size_t) item;
	size_t ii = set->mask & (prime_1 * value);

	while (set->items[ii] != nil) {
		if (set->items[ii] == value) {
			return 1;
		} else {
			ii = set->mask & (ii + prime_2);
		}
	}
	return 0;
}
//end stolen hashset
