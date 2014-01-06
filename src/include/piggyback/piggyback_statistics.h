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
	char	   *name;			/* column name */
	Oid			tableid;		/* source table, if known */
	int			columnid;		/* source column, if known */
	Oid			typid;			/* type id */
} be_PGAttDesc;

typedef struct be_PGColumnStatistic {
	be_PGAttDesc *columnDescriptor;
	int isNumeric;
	int n_distinct;
	void *minValue;
	void *maxValue;
	void *mostFrequentValue;
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
