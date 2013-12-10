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

typedef struct PGColumnStatistic {
	char *columnName;
	int columnNumber;
	int n_distinct;
	int minValue;
	int isNumeric;
} PGColumnStatistic;

typedef struct PGStatistics {
	PGColumnStatistic *columnStatistics;
} PGStatistics;

#endif   /* LIBPQ_PIGGYBACK_H */
