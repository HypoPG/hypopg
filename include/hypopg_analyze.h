/*-------------------------------------------------------------------------
 *
 * hypopg_analyze.c: Implementation of ANALYZE-like command for hypothetical
 * objects
 *
 * This file contains all the internal code to compute and store statistics on
 * hypothetical objects.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2015-2018: Julien Rouhaud
 *
 *-------------------------------------------------------------------------
 */

#ifndef _HYPOPG_ANALYZE_H_
#define _HYPOPG_ANALYZE_H_

#if PG_VERSION_NUM >= 100000

/*--- Structs --- */

typedef struct hypoStatsKey
{
	Oid relid;
	AttrNumber attnum;
} hypoStatsKey;

typedef struct hypoStatsEntry
{
	hypoStatsKey key;
	HeapTuple statsTuple;
} hypoStatsEntry;

/*--- Variables exported ---*/

/* Hash table storing the partition-level statistics */
extern HTAB *hypoStatsHash;
#endif			/* PG_VERSION_NUM >= 100000 */

Selectivity hypo_clauselist_selectivity(PlannerInfo *root, RelOptInfo *rel,
		List *clauses, Oid table_relid, Oid parent_oid);

/*--- Functions --- */

Datum		hypopg_analyze(PG_FUNCTION_ARGS);
Datum		hypopg_statistic(PG_FUNCTION_ARGS);
#if PG_VERSION_NUM >= 100000
void hypo_stat_remove(Oid tableid);
#endif

#endif			/* _HYPOPG_ANALYZE_H_ */
