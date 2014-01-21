/*-------------------------------------------------------------------------
 *
 * piggyback_statistics.h
 *		stores piggyback metadata
 *
 *
 * IDENTIFICATION
 *	src/include/piggyback/piggyback_statistics.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIGGYBACK_STATISTICS_H
#define PIGGYBACK_STATISTICS_H


typedef struct be_PGAttDesc
{
	char	   *rescolumnname;			/* column name */
	Oid			srctableid;		/* source table, if known */
	int			srccolumnid;	/* source column, if known */
	int			rescolumnid;	/* result column */
	Oid			typid;			/* type id */
} be_PGAttDesc;

typedef struct be_PGColumnStatistic {
	be_PGAttDesc *columnDescriptor;
	int isNumeric;					//Boolean
	int distinct_status;
	int n_distinctIsFinal;			//Boolean
	void *minValue;
	void *minValueTemp;
	int minValueIsFinal;			//Boolean
	void *maxValue;
	void *maxValueTemp;
	int maxValueIsFinal;			//Boolean
	void *mostFrequentValue;
	int mostFrequentValueIsFinal;	//Boolean
} be_PGColumnStatistic;

typedef struct be_PGUniqueColumnCombination {
	be_PGAttDesc *columnDescriptors;
	int	isUcc;
} be_PGUniqueColumnCombination;

typedef struct be_PGFunctionalDependency {
	be_PGAttDesc *determinants;
	be_PGAttDesc *dependent;
} be_PGFunctionalDependency;

typedef struct be_PGStatistics {
	be_PGColumnStatistic *columnStatistics;
	be_PGUniqueColumnCombination *uniqueColumnCombinations;
	be_PGFunctionalDependency *functionalDependencies;
} be_PGStatistics;

#endif   /* PIGGYBACK_STATISTICS_H */
