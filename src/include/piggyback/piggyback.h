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

#include "nodes/plannodes.h"
#include "utils/hsearch.h"
#include "nodes/pg_list.h"

extern void printIt();

// Declare piggyback struct.
typedef struct _piggyback {
	Plan *root;
	HTAB **distinctValues;
	bool newProcessing;
	int numberOfAttributes;
	List* columnNames;
} Piggyback;

extern void initPiggyback();

extern void setPiggybackRootNode(Plan *rootNode);

// printing
extern void printMetaData();
extern void printDistinctValues();

#endif   /* PIGGYBACK_H */
