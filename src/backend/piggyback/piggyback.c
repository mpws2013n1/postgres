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

//#include "utils/hsearch.h"
//#include "utils/builtins.h"
//#include "nodes/pg_list.h"

void printIt()
{
	printf("THIS IS PRINTED");
	return;
}
// Singleton Piggyback instance.
Piggyback *piggyback = NULL;

/*
 * Initialize piggyback if not already done.
 */
void initPiggyback()
{
	piggyback = (Piggyback*)(malloc(sizeof(Piggyback)));
}

/*
 * Set root node to enable data collection.
 */
void setPiggybackRootNode(Plan *rootNode)
{
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
	if(!piggyback) return;
	int i;

	StringInfoData buf;
	pq_beginmessage(&buf, 'X');
	pq_sendint(&buf, piggyback->numberOfAttributes, 4);

	for (i = 0; i < piggyback->numberOfAttributes; i++) {
		char * columnName = (char *)list_nth(piggyback->columnNames, i);
		long distinctValues = hash_get_num_entries(piggyback->distinctValues[i]);
		int minValue = piggyback->minValue[i];
		int isNumeric = piggyback->isNumeric[i];

		printf("column %s (%d) has %ld distinct values, %d as minimum, numeric: %d \n", columnName, i, distinctValues, minValue, isNumeric);

		pq_sendstring(&buf, columnName);
		pq_sendint(&buf, i, 4);
		pq_sendint(&buf, distinctValues, 4);
		pq_sendint(&buf, minValue, 4);
		pq_sendint(&buf, isNumeric, 4);
	}

	pq_endmessage(&buf);
}
