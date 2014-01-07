/*-------------------------------------------------------------------------
 *
 * libpq-piggyback.h
 *		Piggyback Client Interface Extension for providing statistics
 *
 *
 * IDENTIFICATION
 *	src/interfaces/libpq/libpq-piggyback.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_PIGGYBACK_H
#define LIBPQ_PIGGYBACK_H

typedef struct fe_PGAttDesc
{
	char	   *name;			/* column name */
	Oid			tableid;		/* source table, if known */
	int			columnid;		/* source column, if known */
	Oid			typid;			/* type id */
} fe_PGAttDesc;

typedef struct fe_PGColumnStatistic {
	fe_PGAttDesc *columnDescriptor;
	int isNumeric;
	int n_distinct;
	void *minValue;
	void *maxValue;
	void *mostFrequentValue;
} fe_PGColumnStatistic;

typedef struct fe_PGUniqueColumnCombination {
	fe_PGAttDesc *columnDescriptors;
	int	isUcc;
} fe_PGUniqueColumnCombination;

typedef struct fe_PGFunctionalDependency {
	fe_PGAttDesc *determinants;
	fe_PGAttDesc *dependent;
} fe_PGFunctionalDependency;

typedef struct fe_PGStatistics {
	fe_PGColumnStatistic *columnStatistics;
	fe_PGUniqueColumnCombination *uniqueColumnCombinations;
	fe_PGFunctionalDependency *functionalDependencies;
} fe_PGStatistics;

#endif   /* LIBPQ_PIGGYBACK_H */
