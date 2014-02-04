/*-------------------------------------------------------------------------
 *
 * piggyback.h
 *		piggyback metadata while execution of a query
 *
 *
 * IDENTIFICATION
 *	src/include/piggyback/piggyback.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIGGYBACK_H
#define PIGGYBACK_H

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "utils/hsearch.h"
#include "piggyback_statistics.h";

//begin stolen hashset
struct hashset_st {
	size_t nbits;
	size_t mask;

	size_t capacity;
	size_t *items;
	size_t nitems;
};

typedef struct hashset_st *hashset_t;

/* create hashset instance */
hashset_t hashset_create(void);

/* destroy hashset instance */
void hashset_destroy(hashset_t set);

size_t hashset_num_items(hashset_t set);

/* add item into the hashset.
 *
 * @note 0 and 1 is special values, meaning nil and deleted items. the
 *       function will return -1 indicating error.
 *
 * returns zero if the item already in the set and non-zero otherwise
 */
int hashset_add(hashset_t set, void *item);

/* remove item from the hashset
 *
 * returns non-zero if the item was removed and zero if the item wasn't
 * exist
 */
int hashset_remove(hashset_t set, void *item);

/* check if existence of the item
 *
 * returns non-zero if the item exists and zero otherwise
 */
int hashset_is_member(hashset_t set, void *item);

unsigned long hash(unsigned char *str);
//end stolen hashset

//begin stolen hashmap
/** Hashmap structure (forward declaration) */
struct s_hashmap;
typedef struct s_hashmap hashmap;

/** Creates a new hashmap near the given size. */
extern hashmap* hashmapCreate(int startsize);

/** Inserts a new element into the hashmap. */
extern void hashmapInsert(hashmap*, const void* data, unsigned long key);

/** Removes the storage for the element of the key and returns the element. */
extern void* hashmapRemove(hashmap*, unsigned long key);

/** Returns the element for the key. */
extern void* hashmapGet(hashmap*, unsigned long key);

/** Returns the number of saved elements. */
extern long hashmapCount(hashmap*);

/** Removes the hashmap structure. */
extern void hashmapdelete(hashmap*);
//end stolen hashmap

extern void printIt();

// Declare piggyback struct.
typedef struct _piggyback {
	be_PGStatistics *resultStatistics;
	Plan *root;
	hashset_t *distinctValues;
	hashmap **twoColumnsCombinations;
	// temporary save values for each column of a slot
	char **slotValues;
	int numberOfAttributes;
	int numberOfTuples;
} Piggyback;

extern void initPiggyback();

extern void setPiggybackRootNode(Plan *rootNode);

// printing
extern void printMetaData();
extern void printSingleColumnStatistics();
extern void printFunctionalDependencies();

#endif   /* PIGGYBACK_H */
