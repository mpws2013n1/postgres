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
#include "nodes/pg_list.h"

void printIt() {
	printf("THIS IS PRINTED");
	return;
}
// Singleton Piggyback instance.
Piggyback *piggyback = NULL;

/*
 * Set root node to enable data collection.
 */
void setPiggybackRootNode(Plan *rootNode) {
	// Save root node for later processing.
	piggyback->root = rootNode;
}

void printMetaData() {
	StringInfoData buf;
	pq_beginmessage(&buf, 'X');
	printSingleColumnStatistics(&buf);
	printFunctionalDependencies(&buf);
	pq_endmessage(&buf);
}

void printFunctionalDependencies(StringInfoData* buf) {
	if (!piggyback || piggyback->numberOfTuples <= 0) {
		pq_sendint(buf, 0, 4);
		return;
	}

	int fdCount = 0;
	int i;
	for (i = 1; i <= piggyback->numberOfAttributes; i++) {
		int j;
		for (j = i + 1; j <= piggyback->numberOfAttributes; j++) {
			if (j != i) {
				int distinctCountI = piggyback->resultStatistics->columnStatistics[i-1].distinct_status;
				int distinctCountJ = piggyback->resultStatistics->columnStatistics[j-1].distinct_status;

				// Skip if distinct count of one col equal zero, e.g. col attribute type not supported
				if(distinctCountI * distinctCountJ == 0) {
					continue;
				}

				int index = 0;
				int k;
				for (k = 1; k < i; k++) {
					index += piggyback->numberOfAttributes - k;
				}
				index += (j - i - 1);

				int twoColumnCombinationOfIAndJ = (int) hashset_num_items(piggyback->twoColumnsCombinations[index]);

				//printf("FD: column %d: distinct_count %d, column %d: distinct count %d, "
				//		"col_combination %d distinct count: %d \n", i-1, distinctCountI, j-1, distinctCountJ, index, twoColumnCombinationOfIAndJ);

				be_PGAttDesc *colIDesc = piggyback->resultStatistics->columnStatistics[i-1].columnDescriptor;
				be_PGAttDesc *colJDesc = piggyback->resultStatistics->columnStatistics[j-1].columnDescriptor;

				if (distinctCountI == twoColumnCombinationOfIAndJ) {
					be_PGFunctionalDependency* fd = calloc(1, sizeof(be_PGFunctionalDependency));
					fd->determinants = colIDesc;
					fd->dependent = colJDesc;
					piggyback->resultStatistics->functionalDependencies = lappend(piggyback->resultStatistics->functionalDependencies, fd);
					fdCount++;
				}
				if (distinctCountJ == twoColumnCombinationOfIAndJ) {
					be_PGFunctionalDependency* fd = calloc(1, sizeof(be_PGFunctionalDependency));
					fd->determinants = colJDesc;
					fd->dependent = colIDesc;
					piggyback->resultStatistics->functionalDependencies = lappend(piggyback->resultStatistics->functionalDependencies, fd);
					fdCount++;
				}
			}
		}
	}

	pq_sendint(buf, fdCount, 4);

	ListCell* cell;
	foreach(cell, piggyback->resultStatistics->functionalDependencies){
		be_PGFunctionalDependency* fd = (be_PGFunctionalDependency*)cell->data.ptr_value;
		//char fdString[255];
		//sprintf(fdString, "%s --> %s\n", fd->determinants->rescolumnname ,fd->dependent->rescolumnname );
		pq_sendstring(buf, fd->determinants->rescolumnname);
		pq_sendstring(buf, fd->dependent->rescolumnname);
	}
}

void printSingleColumnStatistics(StringInfoData* buf) {
	if (!piggyback || !piggyback->distinctValues || piggyback->numberOfTuples <= 0) {
		pq_sendint(buf, 0, 4);
		return;
	}

	int i;
	pq_sendint(buf, piggyback->numberOfAttributes, 4);

	for (i = 0; i < piggyback->numberOfAttributes; i++) {
		char * columnName = piggyback->resultStatistics->columnStatistics[i].columnDescriptor->rescolumnname;
		float4 distinctValuesCount = piggyback->resultStatistics->columnStatistics[i].distinct_status;
		// own calculation
		if (piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal == 0) {
			distinctValuesCount = (float4) hashset_num_items(piggyback->distinctValues[i]);
			// unique
		} else if (distinctValuesCount == -1) {
			distinctValuesCount = piggyback->numberOfTuples;
			// base stats
		} else if (distinctValuesCount > -1 && distinctValuesCount < 0) {
			distinctValuesCount = piggyback->numberOfTuples * distinctValuesCount * -1;
		} else if (distinctValuesCount == 0) {
			//TODO
		}
		// *((int*)(piggyback->resultStatistics->columnStatistics[i].minValue))

		// Write distinct values for FD calculation
		piggyback->resultStatistics->columnStatistics[i].distinct_status = distinctValuesCount;

		int minValue = *((int*)(piggyback->resultStatistics->columnStatistics[i].minValue));
		int maxValue = *((int*)(piggyback->resultStatistics->columnStatistics[i].maxValue));
		int isNumeric = piggyback->resultStatistics->columnStatistics[i].isNumeric;

		if(isNumeric) {
			printf("column %s (%d) has %d distinct values, %d as minimum, %d as maximum, numeric: %d \n",
					columnName, i, (int) distinctValuesCount, minValue, maxValue, isNumeric);
		} else {
			printf("column %s (%d) has %d distinct values, numeric: %d \n",
								columnName, i, (int) distinctValuesCount, isNumeric);
		}

		pq_sendstring(buf, columnName);
		pq_sendint(buf, i, 4);
		pq_sendint(buf, (int) distinctValuesCount, 4);
		pq_sendint(buf, minValue, 4);
		pq_sendint(buf, maxValue, 4);
		pq_sendint(buf, isNumeric, 4);
	}
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

unsigned long hash(unsigned char *str) {
	unsigned long hash = 5381;
	int c;

	while (c = *str++)
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

	return hash;
}

int hashset_add_string(hashset_t set, char* string) {
	void* item = hash(string);
	int rv = hashset_add_member(set, item);
	maybe_rehash(set);
	return rv;
}

int hashset_add_integer(hashset_t set, void *item) {
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
