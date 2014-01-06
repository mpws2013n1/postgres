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
//end stolen hashset

extern void printIt();

// Declare piggyback struct.
typedef struct _piggyback {
	be_PGStatistics *resultStatistics;
	Plan *root;
	hashset_t **distinctValues;
	float4* distinctCounts;
	bool newProcessing;
	int numberOfAttributes;
	List* columnNames;
	int *minValue;
	int *maxValue;
	int *isNumeric;
	int numberOfTuples;
} Piggyback;

extern void initPiggyback();

extern void setPiggybackRootNode(Plan *rootNode);

// printing
extern void printMetaData();
extern void printDistinctValues();

#endif   /* PIGGYBACK_H */
