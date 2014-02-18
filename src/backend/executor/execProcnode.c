/*-------------------------------------------------------------------------
 *
 * execProcnode.c
 *	 contains dispatch functions which call the appropriate "initialize",
 *	 "get a tuple", and "cleanup" routines for the given node type.
 *	 If the node has children, then it will presumably call ExecInitNode,
 *	 ExecProcNode, or ExecEndNode on its subnodes and do the appropriate
 *	 processing.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execProcnode.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecInitNode	-		initialize a plan node and its subplans
 *		ExecProcNode	-		get a tuple by executing the plan node
 *		ExecEndNode		-		shut down a plan node and its subplans
 *
 *	 NOTES
 *		This used to be three files.  It is now all combined into
 *		one file so that it is easier to keep ExecInitNode, ExecProcNode,
 *		and ExecEndNode in sync when new nodes are added.
 *
 *	 EXAMPLE
 *		Suppose we want the age of the manager of the shoe department and
 *		the number of employees in that department.  So we have the query:
 *
 *				select DEPT.no_emps, EMP.age
 *				from DEPT, EMP
 *				where EMP.name = DEPT.mgr and
 *					  DEPT.name = "shoe"
 *
 *		Suppose the planner gives us the following plan:
 *
 *						Nest Loop (DEPT.mgr = EMP.name)
 *						/		\
 *					   /		 \
 *				   Seq Scan		Seq Scan
 *					DEPT		  EMP
 *				(name = "shoe")
 *
 *		ExecutorStart() is called first.
 *		It calls InitPlan() which calls ExecInitNode() on
 *		the root of the plan -- the nest loop node.
 *
 *	  * ExecInitNode() notices that it is looking at a nest loop and
 *		as the code below demonstrates, it calls ExecInitNestLoop().
 *		Eventually this calls ExecInitNode() on the right and left subplans
 *		and so forth until the entire plan is initialized.	The result
 *		of ExecInitNode() is a plan state tree built with the same structure
 *		as the underlying plan tree.
 *
 *	  * Then when ExecutorRun() is called, it calls ExecutePlan() which calls
 *		ExecProcNode() repeatedly on the top node of the plan state tree.
 *		Each time this happens, ExecProcNode() will end up calling
 *		ExecNestLoop(), which calls ExecProcNode() on its subplans.
 *		Each of these subplans is a sequential scan so ExecSeqScan() is
 *		called.  The slots returned by ExecSeqScan() may contain
 *		tuples which contain the attributes ExecNestLoop() uses to
 *		form the tuples it returns.
 *
 *	  * Eventually ExecSeqScan() stops returning tuples and the nest
 *		loop join ends.  Lastly, ExecutorEnd() calls ExecEndNode() which
 *		calls ExecEndNestLoop() which in turn calls ExecEndNode() on
 *		its subplans which result in ExecEndSeqScan().
 *
 *		This should show how the executor works by having
 *		ExecInitNode(), ExecProcNode() and ExecEndNode() dispatch
 *		their work to the appopriate node support routines which may
 *		in turn call these routines themselves on their subplans.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "executor/nodeAppend.h"
#include "executor/nodeBitmapAnd.h"
#include "executor/nodeBitmapHeapscan.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeBitmapOr.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeForeignscan.h"
#include "executor/nodeFunctionscan.h"
#include "executor/nodeGroup.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeLimit.h"
#include "executor/nodeLockRows.h"
#include "executor/nodeMaterial.h"
#include "executor/nodeMergeAppend.h"
#include "executor/nodeMergejoin.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeNestloop.h"
#include "executor/nodeRecursiveunion.h"
#include "executor/nodeResult.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeSetOp.h"
#include "executor/nodeSort.h"
#include "executor/nodeSubplan.h"
#include "executor/nodeSubqueryscan.h"
#include "executor/nodeTidscan.h"
#include "executor/nodeUnique.h"
#include "executor/nodeValuesscan.h"
#include "executor/nodeWindowAgg.h"
#include "executor/nodeWorktablescan.h"
#include "miscadmin.h"

#include "utils/numeric.h"
#include "utils/hsearch.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "nodes/pg_list.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"
#include "piggyback/piggyback.h"
#include <limits.h>

extern Piggyback *piggyback;
void fillFDCandidateMaps();
void addToTwoColumnCombinationHashSet(int from, char* valueToConcat, int to, char* value);
void LookForFilterWithEquality(PlanState* result, Oid tableOid, List* qual);
void InvalidateStatisticsForTables(List* oldTableOids);
void InvalidateStatisticsForTable(int tableOid);
void SetStatisticValuesForEqual(int* equationValue, int columnStatisticId, be_PGAttDesc *columnData);
void SetStatisticValuesForUnequal(bool greaterThan, bool orEquals,int* equationValue, int columnStatisticId, be_PGAttDesc *columnData);

/* ------------------------------------------------------------------------
 *		ExecInitNode
 *
 *		Recursively initializes all the nodes in the plan tree rooted
 *		at 'node'.
 *
 *		Inputs:
 *		  'node' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		  'eflags' is a bitwise OR of flag bits described in executor.h
 *
 *		Returns a PlanState node corresponding to the given Plan node.
 * ------------------------------------------------------------------------
 */
PlanState *
ExecInitNode(Plan *node, EState *estate, int eflags) {
	PlanState *result;
	List *subps;
	ListCell *l;

	// Pointers that are necessary for specific node types like SeqScan.
	SeqScanState* resultAsScanState;
	IndexScanState* resultAsIndexScan;
	IndexOnlyScanState* resultAsIndexOnlyScan;
	List* oids = NULL;
	int tableOid = -1;
	/*
	 * do nothing when we get to the end of a leaf on tree.
	 */
	if (node == NULL)
		return NULL;

	switch (nodeTag(node)) {
	/*
	 * control nodes
	 */
	case T_Result:
		result = (PlanState *) ExecInitResult((Result *) node, estate, eflags);
		break;

	case T_ModifyTable:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitModifyTable((ModifyTable *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_Append:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitAppend((Append *) node, estate, eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_MergeAppend:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitMergeAppend((MergeAppend *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_RecursiveUnion:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitRecursiveUnion((RecursiveUnion *) node,
				estate, eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_BitmapAnd:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitBitmapAnd((BitmapAnd *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_BitmapOr:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitBitmapOr((BitmapOr *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

		/*
		 * scan nodes
		 */
	case T_SeqScan:
		resultAsScanState = ExecInitSeqScan((SeqScan *) node, estate,
				eflags);
		result = (PlanState *) resultAsScanState;

		if (resultAsScanState)
		{
			tableOid = resultAsScanState->ss_currentRelation->rd_id;
		}

		if (tableOid != -1 && piggyback != NULL)
		{
			int* tableOidPtr = (int*) malloc(sizeof(int));
			*tableOidPtr = tableOid;
			LookForFilterWithEquality(result, tableOid, result->qual);
			piggyback->tableOids = lappend(piggyback->tableOids, tableOidPtr);
		}
		break;

	case T_IndexScan:
		resultAsIndexScan = ExecInitIndexScan((IndexScan *) node, estate,
				eflags);
		result = (PlanState *) resultAsIndexScan;

		if (resultAsIndexScan)
		{
			tableOid = resultAsIndexScan->ss.ss_currentRelation->rd_id;
		}

		if (tableOid != -1)
		{
			int* tableOidPtr = (int*) malloc(sizeof(int));
			*tableOidPtr = tableOid;
			LookForFilterWithEquality(result, tableOid, resultAsIndexScan->indexqualorig);
			piggyback->tableOids = lappend(piggyback->tableOids, tableOidPtr);
		}
		break;

	// TODO: search for examples for IndexOnlyScan and test this case (examples on https://wiki.postgresql.org/wiki/Index-only_scans)
	case T_IndexOnlyScan:
		resultAsIndexOnlyScan = ExecInitIndexOnlyScan((IndexOnlyScan *) node, estate,
				eflags);
		result = (PlanState *) resultAsIndexOnlyScan;

		if (resultAsIndexOnlyScan)
		{
			tableOid = resultAsIndexOnlyScan->ss.ss_currentRelation->rd_id;
		}

		if (tableOid != -1)
		{
			int* tableOidPtr = (int*) malloc(sizeof(int));
			*tableOidPtr = tableOid;
			LookForFilterWithEquality(result, tableOid, resultAsIndexOnlyScan->indexqual);
			piggyback->tableOids = lappend(piggyback->tableOids, tableOidPtr);
		}
		break;

	case T_BitmapIndexScan:
		result = (PlanState *) ExecInitBitmapIndexScan((BitmapIndexScan *) node,
				estate, eflags);
		break;

	case T_BitmapHeapScan:
		result = (PlanState *) ExecInitBitmapHeapScan((BitmapHeapScan *) node,
				estate, eflags);
		break;

	case T_TidScan:
		result = (PlanState *) ExecInitTidScan((TidScan *) node, estate,
				eflags);
		break;

	case T_SubqueryScan:
		result = (PlanState *) ExecInitSubqueryScan((SubqueryScan *) node,
				estate, eflags);
		break;

	case T_FunctionScan:
		result = (PlanState *) ExecInitFunctionScan((FunctionScan *) node,
				estate, eflags);
		break;

	case T_ValuesScan:
		result = (PlanState *) ExecInitValuesScan((ValuesScan *) node, estate,
				eflags);
		break;

	case T_CteScan:
		result = (PlanState *) ExecInitCteScan((CteScan *) node, estate,
				eflags);
		break;

	case T_WorkTableScan:
		result = (PlanState *) ExecInitWorkTableScan((WorkTableScan *) node,
				estate, eflags);
		break;

	case T_ForeignScan:
		result = (PlanState *) ExecInitForeignScan((ForeignScan *) node, estate,
				eflags);
		break;

		/*
		 * join nodes
		 */
	case T_NestLoop:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitNestLoop((NestLoop *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_MergeJoin:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitMergeJoin((MergeJoin *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_HashJoin:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitHashJoin((HashJoin *) node, estate,
						eflags);
		InvalidateStatisticsForTables(oids);
		break;

		/*
		 * materialization nodes
		 */
	case T_Material:
		result = (PlanState *) ExecInitMaterial((Material *) node, estate,
				eflags);
		break;

	case T_Sort:
		result = (PlanState *) ExecInitSort((Sort *) node, estate, eflags);
		break;

	case T_Group:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitGroup((Group *) node, estate, eflags);
		InvalidateStatisticsForTables(oids);
		break;

	// we do not want to invalid the statistic values, because they do not change the values from the original tables
	case T_Agg:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitAgg((Agg *) node, estate, eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_WindowAgg:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitWindowAgg((WindowAgg *) node, estate,
				eflags);
		InvalidateStatisticsForTables(oids);
		break;

	case T_Unique:
		result = (PlanState *) ExecInitUnique((Unique *) node, estate, eflags);
		break;

	case T_Hash:
		result = (PlanState *) ExecInitHash((Hash *) node, estate, eflags);
		break;

	case T_SetOp:
		result = (PlanState *) ExecInitSetOp((SetOp *) node, estate, eflags);
		break;

	case T_LockRows:
		result = (PlanState *) ExecInitLockRows((LockRows *) node, estate,
				eflags);
		break;

	case T_Limit:
		oids = piggyback->tableOids;
		result = (PlanState *) ExecInitLimit((Limit *) node, estate, eflags);
		InvalidateStatisticsForTables(oids);
		break;

	default:
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
		result = NULL; /* keep compiler quiet */
		break;
	}

	/*
	 * Initialize any initPlans present in this node.  The planner put them in
	 * a separate list for us.
	 */
	subps = NIL;
	foreach(l, node->initPlan)
	{
		SubPlan *subplan = (SubPlan *) lfirst(l);
		SubPlanState *sstate;

		Assert(IsA(subplan, SubPlan));
		sstate = ExecInitSubPlan(subplan, result);
		subps = lappend(subps, sstate);
	}
	result->initPlan = subps;

	/* Set up instrumentation for this node if requested */
	if (estate->es_instrument)
		result->instrument = InstrAlloc(1, estate->es_instrument);

	return result;
}

void
InvalidateStatisticsForTables(List* oldTableOids)
{
	List* relevantTableOids = NULL;
	ListCell* l1;
	ListCell* l2;
	//relevantTableOids = list_difference(piggyback->tableOids, oldTableOids);
	foreach(l1, piggyback->tableOids)
	{
		int isNewOid = 1;
		foreach(l2, oldTableOids)
		{
			if (*((int*)lfirst(l1)) == *((int*)lfirst(l2)))
			{
				isNewOid = 0;
				break;
			}
		}
		if (isNewOid == 1)
		{
			relevantTableOids = lappend(relevantTableOids, (int*)lfirst(l1));
		}
	}

	foreach (l1, relevantTableOids)
	{
		int currentOid = *((int*)lfirst(l1));
		InvalidateStatisticsForTable(currentOid);
	}
}

void
InvalidateStatisticsForTable(int tableOid)
{
	int i = 0;
	for (; i < piggyback->numberOfAttributes; i++)
	{
		if (tableOid == piggyback->resultStatistics->columnStatistics[i].columnDescriptor->srctableid)
		{
			// this columnStatistic is obsolete
			piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal = 0;
			piggyback->resultStatistics->columnStatistics[i].minValueIsFinal = 0;
			piggyback->resultStatistics->columnStatistics[i].maxValueIsFinal = 0;
			piggyback->resultStatistics->columnStatistics[i].mostFrequentValueIsFinal = 0;
		}
	}
}

void
SetStatisticValuesForEqual(int* equationValue, int columnStatisticId, be_PGAttDesc *columnData) {
	// only write values, if the selected field is part of the result table
	if (columnStatisticId < piggyback->numberOfAttributes){
		piggyback->resultStatistics->columnStatistics[columnStatisticId].columnDescriptor = columnData;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].isNumeric = 1;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].maxValue = equationValue;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].minValue = equationValue;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].mostFrequentValue = equationValue;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].n_distinct = 1;

		// the meta data for this column is complete and should not be calculated again
		piggyback->resultStatistics->columnStatistics[columnStatisticId].n_distinctIsFinal = 1;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].minValueIsFinal = 1;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].maxValueIsFinal = 1;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].mostFrequentValueIsFinal = 1;
	}
	else {
		printf("there are statistics results from a selection with = that are not part of the result table\n");
	}
}

// meaning of the first and second parameter:
// >=	== greaterThan true, orEquals true
// > 	== greaterThan true, orEquals false
// <=	== greaterThan false, orEquals true
// <	== greaterThan false, orEquals false
void
SetStatisticValuesForUnequal(bool greaterThan, bool orEquals, int* equationValue, int columnStatisticId, be_PGAttDesc *columnData) {
	// only write values, if the selected field is part of the result table
	int *value = (int*) malloc(sizeof(int));
	if (!orEquals)
	{
		if (greaterThan) // for instance x > 3 means x has at least the value 4
		{
			*value = *equationValue + 1;
		}
		else // for instance x < 3 means x has at maximum the value 2
		{
			*value = *equationValue - 1;
		}
	}
	else
	{
		*value = *equationValue;
	}

	if (columnStatisticId < piggyback->numberOfAttributes)
	{
		piggyback->resultStatistics->columnStatistics[columnStatisticId].columnDescriptor = columnData;
		if(greaterThan)
			piggyback->resultStatistics->columnStatistics[columnStatisticId].minValue = value;
		else
			piggyback->resultStatistics->columnStatistics[columnStatisticId].maxValue = value;

		// the meta data for this column is complete and should not be calculated again
		if(greaterThan)
			piggyback->resultStatistics->columnStatistics[columnStatisticId].minValueIsFinal = 1;
		else
			piggyback->resultStatistics->columnStatistics[columnStatisticId].maxValueIsFinal = 1;

		piggyback->resultStatistics->columnStatistics[columnStatisticId].n_distinctIsFinal = 0;
		piggyback->resultStatistics->columnStatistics[columnStatisticId].mostFrequentValueIsFinal = 0;
	}
	else
	{
		printf("there are statistics results from a selection with > or < with a constant that are not part of the result table\n");
	}
}

void
LookForFilterWithEquality(PlanState* result, Oid tableOid, List* qual)
{
	if (qual) {
		int opno = ((OpExpr*) ((ExprState*) linitial(qual))->expr)->opno;
		int columnId = ((Var*) ((OpExpr*) ((ExprState*) linitial(qual))->expr)->args->head->data.ptr_value)->varattno;
		be_PGAttDesc *columnData = (be_PGAttDesc*) malloc(sizeof(be_PGAttDesc));
		columnData->srccolumnid = columnId;

		// TODO: write this in a method that returns i for better readability
		int i = 0;
		for (; i < piggyback->numberOfAttributes; i++)
		{
			if (tableOid == piggyback->resultStatistics->columnStatistics[i].columnDescriptor->srctableid
					&& columnData->srccolumnid == piggyback->resultStatistics->columnStatistics[i].columnDescriptor->srccolumnid)
				break;
		}

		// invalid all columns of this table, because there is a selection
		InvalidateStatisticsForTable(tableOid);

		// the magic numbers are operator identifiers from posgres/src/include/catalog/pg_operator.h
		// equals
		if(opno == 94 || opno == 96 || opno == 410 || opno == 416 || opno == 1862 || opno == 1868 || opno == 15 || opno == 532 || opno == 533) { // it is a equality like number_of_tracks = 3
			int numberOfAttributes = result->plan->targetlist->length;

			int *minAndMaxAndAvg = (int*) malloc(sizeof(int));
			minAndMaxAndAvg = &(((Const*) ((OpExpr*) ((ExprState*) linitial(qual))->expr)->args->tail->data.ptr_value)->constvalue);

			// we always set the type to 8byte-integer because we don't need a detailed differentiation
			columnData->typid = 20;

			SetStatisticValuesForEqual(minAndMaxAndAvg, i, columnData);
		}
		// <
		else if(opno == 37 || opno == 95 || opno == 97 || opno == 412 || opno == 418 || opno == 534 || opno == 535 || opno == 1864 || opno == 1870) {

			int *min = (int*) malloc(sizeof(int));
			min = &(((Const*) ((OpExpr*) ((ExprState*) linitial(qual))->expr)->args->tail->data.ptr_value)->constvalue);

			// we always set the type to 8byte-integer because we don't need a detailed differentiation
			columnData->typid = 20;

			SetStatisticValuesForUnequal(false, false, min, i, columnData);
		}
		// <=
		else if(opno == 80 || opno == 414 || opno == 420 || opno == 522 || opno == 523 || opno == 540 || opno == 541 || opno == 1866 || opno == 1872) {

			int *min = (int*) malloc(sizeof(int));
			min = &(((Const*) ((OpExpr*) ((ExprState*) linitial(qual))->expr)->args->tail->data.ptr_value)->constvalue);

			// we always set the type to 8byte-integer because we don't need a detailed differentiation
			columnData->typid = 20;

			SetStatisticValuesForUnequal(false, true, min, i, columnData);
		}
		// >
		else if(opno == 76 || opno == 413 || opno == 419 || opno == 520 || opno == 521 || opno == 536 || opno == 1865 || opno == 1871) {

			int *max = (int*) malloc(sizeof(int));
			max = &(((Const*) ((OpExpr*) ((ExprState*) linitial(qual))->expr)->args->tail->data.ptr_value)->constvalue);

			// we always set the type to 8byte-integer because we don't need a detailed differentiation
			columnData->typid = 20;

			SetStatisticValuesForUnequal(true, false, max, i, columnData);
		}
		// >=
		else if(opno == 82 || opno == 415 || opno == 430 || opno == 524 || opno == 525 || opno == 537 || opno == 542 || opno == 543 || opno == 1867 || opno == 1873) {

			int *max = (int*) malloc(sizeof(int));
			max = &(((Const*) ((OpExpr*) ((ExprState*) linitial(qual))->expr)->args->tail->data.ptr_value)->constvalue);

			// we always set the type to 8byte-integer because we don't need a detailed differentiation
			columnData->typid = 20;

			SetStatisticValuesForUnequal(true, true, max, i, columnData);
		}
		else {
			printf("this opno is no =, <, >, <= or >=: %d (for column id %d)\n", opno, columnId);
			// found a selection, therefore we cannot use old statistics
			// InvalidateStatisticsForTable(tableOid); (this has to be always, because there could be more than one column of this table in the result
		}
	}
}

/* ----------------------------------------------------------------
 *		ExecProcNode
 *
 *		Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecProcNode(PlanState *node) {
	TupleTableSlot *result;

	CHECK_FOR_INTERRUPTS();

	if (node->chgParam != NULL) /* something changed */
		ExecReScan(node); /* let ReScan handle this */

	if (node->instrument)
		InstrStartNode(node->instrument);

	switch (nodeTag(node)) {
	/*
	 * control nodes
	 */
	case T_ResultState:
		result = ExecResult((ResultState *) node);
		break;

	case T_ModifyTableState:
		result = ExecModifyTable((ModifyTableState *) node);
		break;

	case T_AppendState:
		result = ExecAppend((AppendState *) node);
		break;

	case T_MergeAppendState:
		result = ExecMergeAppend((MergeAppendState *) node);
		break;

	case T_RecursiveUnionState:
		result = ExecRecursiveUnion((RecursiveUnionState *) node);
		break;

		/* BitmapAndState does not yield tuples */

		/* BitmapOrState does not yield tuples */

		/*
		 * scan nodes
		 */
	case T_SeqScanState:
		result = ExecSeqScan((SeqScanState *) node);
		break;

	case T_IndexScanState:
		result = ExecIndexScan((IndexScanState *) node);
		break;

	case T_IndexOnlyScanState:
		result = ExecIndexOnlyScan((IndexOnlyScanState *) node);
		break;

		/* BitmapIndexScanState does not yield tuples */

	case T_BitmapHeapScanState:
		result = ExecBitmapHeapScan((BitmapHeapScanState *) node);
		break;

	case T_TidScanState:
		result = ExecTidScan((TidScanState *) node);
		break;

	case T_SubqueryScanState:
		result = ExecSubqueryScan((SubqueryScanState *) node);
		break;

	case T_FunctionScanState:
		result = ExecFunctionScan((FunctionScanState *) node);
		break;

	case T_ValuesScanState:
		result = ExecValuesScan((ValuesScanState *) node);
		break;

	case T_CteScanState:
		result = ExecCteScan((CteScanState *) node);
		break;

	case T_WorkTableScanState:
		result = ExecWorkTableScan((WorkTableScanState *) node);
		break;

	case T_ForeignScanState:
		result = ExecForeignScan((ForeignScanState *) node);
		break;

		/*
		 * join nodes
		 */
	case T_NestLoopState:
		result = ExecNestLoop((NestLoopState *) node);
		break;

	case T_MergeJoinState:
		result = ExecMergeJoin((MergeJoinState *) node);
		break;

	case T_HashJoinState:
		result = ExecHashJoin((HashJoinState *) node);
		break;

		/*
		 * materialization nodes
		 */
	case T_MaterialState:
		result = ExecMaterial((MaterialState *) node);
		break;

	case T_SortState:
		result = ExecSort((SortState *) node);
		break;

	case T_GroupState:
		result = ExecGroup((GroupState *) node);
		break;

	case T_AggState:
		result = ExecAgg((AggState *) node);
		break;

	case T_WindowAggState:
		result = ExecWindowAgg((WindowAggState *) node);
		break;

	case T_UniqueState:
		result = ExecUnique((UniqueState *) node);
		break;

	case T_HashState:
		result = ExecHash((HashState *) node);
		break;

	case T_SetOpState:
		result = ExecSetOp((SetOpState *) node);
		break;

	case T_LockRowsState:
		result = ExecLockRows((LockRowsState *) node);
		break;

	case T_LimitState:
		result = ExecLimit((LimitState *) node);
		break;

	default:
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
		result = NULL;
		break;
	}

	/*
	 * Process with piggyback if current node is root node.
	 */

	bool calculateFDs = false;

	if (piggyback != NULL) {
		if (node->plan == piggyback->root && result && !result->tts_isempty && result->tts_tupleDescriptor) {
			piggyback->numberOfAttributes = result->tts_tupleDescriptor->natts;
			Form_pg_attribute *attrList = result->tts_tupleDescriptor->attrs;

			Datum datum;
			bool isNull;

			// fetch all data
			slot_getallattrs(result);

			int i;
			for (i = 0; i < piggyback->numberOfAttributes; i++) {
				if(!(piggyback->resultStatistics->columnStatistics[i].minValueIsFinal
						&& piggyback->resultStatistics->columnStatistics[i].maxValueIsFinal &&
						piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal)) { //TODO add mostFrequentValueIsFinal if ever implemented
					datum = slot_getattr(result, i + 1, &isNull);

					if (isNull) {
						piggyback->slotValues[i] = "";
						continue;
					}

					// Use data type aware conversion.
					Form_pg_attribute attr = attrList[i];

					switch (attr->atttypid) {
					case INT8OID:
					case INT2OID:
					case INT2VECTOROID:
					case INT4OID: { // Int
						piggyback->resultStatistics->columnStatistics[i].isNumeric = 1;
						int *val_pntr = (int*) malloc(sizeof(int));
						int value = DatumGetInt32(datum);
						*val_pntr = value;

						// Write temporary slot value for FD calculation
						char* cvalue = calloc(20, sizeof(char));
						sprintf(cvalue, "%d", value);
						piggyback->slotValues[i] = cvalue;

						if (value < *((int*)(piggyback->resultStatistics->columnStatistics[i].minValueTemp))) {
							piggyback->resultStatistics->columnStatistics[i].minValueTemp = val_pntr;
							if (piggyback->resultStatistics->columnStatistics[i].minValueTemp
									== piggyback->resultStatistics->columnStatistics[i].minValue)
								piggyback->resultStatistics->columnStatistics[i].minValueIsFinal = TRUE;
						}
						if (value > *((int*)(piggyback->resultStatistics->columnStatistics[i].maxValueTemp))) {
							piggyback->resultStatistics->columnStatistics[i].maxValueTemp = val_pntr;
							if(piggyback->resultStatistics->columnStatistics[i].maxValueTemp
									== piggyback->resultStatistics->columnStatistics[i].maxValue)
								piggyback->resultStatistics->columnStatistics[i].maxValueIsFinal = TRUE;
						}
						if (!piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal) {
							hashset_add_integer(piggyback->distinctValues[i], value);
							if (hashset_num_items(piggyback->distinctValues)
									== piggyback->resultStatistics->columnStatistics[i].n_distinct) //TODO make sure there is the actual number in here, not the status
								piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal = TRUE;
						}
						break;
					}
					case NUMERICOID: {// Decimal
						piggyback->resultStatistics->columnStatistics[i].isNumeric = 0;
						double *val_pntr = (double*) malloc(sizeof(double));
						Numeric numericValue = DatumGetNumeric(datum);
						char* cvalue = DatumGetCString(DirectFunctionCall1(numeric_out,
								  NumericGetDatum(numericValue)));
						double value = atof(cvalue);
						*val_pntr = value;

						piggyback->slotValues[i] = cvalue;

						if (!piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal) {
							hashset_add_string(piggyback->distinctValues[i],
									piggyback->slotValues[i]);
							if (hashset_num_items(piggyback->distinctValues[i])
									== piggyback->resultStatistics->columnStatistics[i].n_distinct)
								piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal =
										TRUE;
						}
						break;
					}
					case BPCHAROID:
					case VARCHAROID: { // Varchar
						piggyback->slotValues[i] = TextDatumGetCString(datum);

						piggyback->resultStatistics->columnStatistics[i].isNumeric = 0;
						if (!piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal) {
							hashset_add_string(piggyback->distinctValues[i], piggyback->slotValues[i]);
							if (hashset_num_items(piggyback->distinctValues[i])
									== piggyback->resultStatistics->columnStatistics[i].n_distinct)
								piggyback->resultStatistics->columnStatistics[i].n_distinctIsFinal = TRUE;
						}
						break;
					}
					default:
						piggyback->slotValues[i] = "";
						break;
					}
				}
			}
			if (calculateFDs) {
				fillFDCandidateMaps();
			}
		}
	}

	if (node->instrument)
		InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0);

	return result;
}

void prune(){
	int i;
	int blockSize = piggyback->numberOfAttributes - 1;
	for (i = 0; i < piggyback->numberOfAttributes; i++) {
		int j;
		for (j = 0; j < piggyback->numberOfAttributes; j++) {
			if (i == j) {
				continue;
			}
			int indexBlock = i * blockSize;
			int indexSummand = j > i ? j - 1 : j;
			int index = indexBlock + indexSummand;

			if (piggyback->resultStatistics->columnStatistics[i].n_distinct != 0
					&& piggyback->resultStatistics->columnStatistics[j].n_distinct != 0) {
				if (piggyback->resultStatistics->columnStatistics[i].n_distinct
						< piggyback->resultStatistics->columnStatistics[j].n_distinct
						&& piggyback->resultStatistics->columnStatistics[j].n_distinctIsFinal) {
					hashmapDelete(piggyback->twoColumnsCombinations[index]);
					piggyback->twoColumnsCombinations[index] = NULL;
				}
			}
		}
	}
}

/*
 * Stores FD combinations in HashMap, if already existing and conflicting: mark FD as invalid
 */
void fillFDCandidateMaps() {

	if(!piggyback->fdsPruned){
		prune();
		piggyback->fdsPruned = true;
	}

	int i;
	int blockSize = piggyback->numberOfAttributes-1;
	for(i=0; i< piggyback->numberOfAttributes; i++){
		int j;
		for(j=0; j< piggyback->numberOfAttributes; j++){
			if(i==j){
				continue;
			}
			int indexBlock = i*blockSize;
			int indexSummand = j>i ? j-1 : j;
			int index = indexBlock+indexSummand;
			hashmap* targetMap = piggyback->twoColumnsCombinations[index];
			if(targetMap==NULL){
				continue;
			}else{
				char* rhs = (char*) hashmapGet(targetMap, hash(piggyback->slotValues[i]));

				if(rhs==0){
					hashmapInsert(targetMap, (void *)piggyback->slotValues[j],hash(piggyback->slotValues[i]));
				}else{
					if(rhs!=piggyback->slotValues[j]){
						hashmapDelete(piggyback->twoColumnsCombinations[index]);
						piggyback->twoColumnsCombinations[index]=NULL;
					}
				}
			}
		}
	}
}


/* ----------------------------------------------------------------
 *		MultiExecProcNode
 *
 *		Execute a node that doesn't return individual tuples
 *		(it might return a hashtable, bitmap, etc).  Caller should
 *		check it got back the expected kind of Node.
 *
 * This has essentially the same responsibilities as ExecProcNode,
 * but it does not do InstrStartNode/InstrStopNode (mainly because
 * it can't tell how many returned tuples to count).  Each per-node
 * function must provide its own instrumentation support.
 * ----------------------------------------------------------------
 */
Node *
MultiExecProcNode(PlanState *node) {
	Node *result;

	CHECK_FOR_INTERRUPTS();

	if (node->chgParam != NULL) /* something changed */
		ExecReScan(node); /* let ReScan handle this */

	switch (nodeTag(node)) {
	/*
	 * Only node types that actually support multiexec will be listed
	 */

	case T_HashState:
		result = MultiExecHash((HashState *) node);
		break;

	case T_BitmapIndexScanState:
		result = MultiExecBitmapIndexScan((BitmapIndexScanState *) node);
		break;

	case T_BitmapAndState:
		result = MultiExecBitmapAnd((BitmapAndState *) node);
		break;

	case T_BitmapOrState:
		result = MultiExecBitmapOr((BitmapOrState *) node);
		break;

	default:
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
		result = NULL;
		break;
	}

	return result;
}

/* ----------------------------------------------------------------
 *		ExecEndNode
 *
 *		Recursively cleans up all the nodes in the plan rooted
 *		at 'node'.
 *
 *		After this operation, the query plan will not be able to be
 *		processed any further.	This should be called only after
 *		the query plan has been fully executed.
 * ----------------------------------------------------------------
 */
void ExecEndNode(PlanState *node) {
	// TODO: remove memory leak
	if (piggyback) {
		printMetaData();

		piggyback = NULL;
	}

	/*
	 * do nothing when we get to the end of a leaf on tree.
	 */
	if (node == NULL)
		return;

	if (node->chgParam != NULL) {
		bms_free(node->chgParam);
		node->chgParam = NULL;
	}

	switch (nodeTag(node)) {
	/*
	 * control nodes
	 */
	case T_ResultState:
		ExecEndResult((ResultState *) node);
		break;

	case T_ModifyTableState:
		ExecEndModifyTable((ModifyTableState *) node);
		break;

	case T_AppendState:
		ExecEndAppend((AppendState *) node);
		break;

	case T_MergeAppendState:
		ExecEndMergeAppend((MergeAppendState *) node);
		break;

	case T_RecursiveUnionState:
		ExecEndRecursiveUnion((RecursiveUnionState *) node);
		break;

	case T_BitmapAndState:
		ExecEndBitmapAnd((BitmapAndState *) node);
		break;

	case T_BitmapOrState:
		ExecEndBitmapOr((BitmapOrState *) node);
		break;

		/*
		 * scan nodes
		 */
	case T_SeqScanState:
		ExecEndSeqScan((SeqScanState *) node);
		break;

	case T_IndexScanState:
		ExecEndIndexScan((IndexScanState *) node);
		break;

	case T_IndexOnlyScanState:
		ExecEndIndexOnlyScan((IndexOnlyScanState *) node);
		break;

	case T_BitmapIndexScanState:
		ExecEndBitmapIndexScan((BitmapIndexScanState *) node);
		break;

	case T_BitmapHeapScanState:
		ExecEndBitmapHeapScan((BitmapHeapScanState *) node);
		break;

	case T_TidScanState:
		ExecEndTidScan((TidScanState *) node);
		break;

	case T_SubqueryScanState:
		ExecEndSubqueryScan((SubqueryScanState *) node);
		break;

	case T_FunctionScanState:
		ExecEndFunctionScan((FunctionScanState *) node);
		break;

	case T_ValuesScanState:
		ExecEndValuesScan((ValuesScanState *) node);
		break;

	case T_CteScanState:
		ExecEndCteScan((CteScanState *) node);
		break;

	case T_WorkTableScanState:
		ExecEndWorkTableScan((WorkTableScanState *) node);
		break;

	case T_ForeignScanState:
		ExecEndForeignScan((ForeignScanState *) node);
		break;

		/*
		 * join nodes
		 */
	case T_NestLoopState:
		ExecEndNestLoop((NestLoopState *) node);
		break;

	case T_MergeJoinState:
		ExecEndMergeJoin((MergeJoinState *) node);
		break;

	case T_HashJoinState:
		ExecEndHashJoin((HashJoinState *) node);
		break;

		/*
		 * materialization nodes
		 */
	case T_MaterialState:
		ExecEndMaterial((MaterialState *) node);
		break;

	case T_SortState:
		ExecEndSort((SortState *) node);
		break;

	case T_GroupState:
		ExecEndGroup((GroupState *) node);
		break;

	case T_AggState:
		ExecEndAgg((AggState *) node);
		break;

	case T_WindowAggState:
		ExecEndWindowAgg((WindowAggState *) node);
		break;

	case T_UniqueState:
		ExecEndUnique((UniqueState *) node);
		break;

	case T_HashState:
		ExecEndHash((HashState *) node);
		break;

	case T_SetOpState:
		ExecEndSetOp((SetOpState *) node);
		break;

	case T_LockRowsState:
		ExecEndLockRows((LockRowsState *) node);
		break;

	case T_LimitState:
		ExecEndLimit((LimitState *) node);
		break;

	default:
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
		break;
	}
}
