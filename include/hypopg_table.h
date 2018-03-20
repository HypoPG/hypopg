/*-------------------------------------------------------------------------
 *
 * hypopg_table.h: Implementation of hypothetical partitioning for PostgreSQL
 *
 * This file contains all includes for the internal code related to
 * hypothetical partition support.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2015-2018: Julien Rouhaud
 *
 *-------------------------------------------------------------------------
*/
#ifndef _HYPOPG_TABLE_H_
#define _HYPOPG_TABLE_H_

#if PG_VERSION_NUM >= 100000
#define HYPO_PARTITION_NOT_SUPPORTED

#define HYPO_TABLE_NB_COLS		5	/* # of column hypopg_table() returns */
#define HYPO_ADD_PART_COLS	2	/* # of column hypopg_add_partition() returns */

#include "optimizer/paths.h"

/*--- Structs --- */

/*--------------------------------------------------------
 * Hypothetical partition storage, pretty much the needed data from RelOptInfo.
 * Some dynamic informations such as pages and lines are not stored but
 * computed when the hypothetical partition is used.
 */
typedef struct hypoTable
{
	Oid			oid;			/* hypothetical table unique identifier */
	Oid			parentid;		/* In case of partition, it's direct parent,
								   otherwise InvalidOid */
	char	   *tablename;		/* hypothetical partition name, or original
								   table name for root parititon */
	Oid			namespace;		/* Oid of the hypothetical table's schema */

	/* used for partitioned relations */
	PartitionScheme part_scheme;	/* Partitioning scheme. */

	/* added for internal use */
	PartitionBoundSpec	*boundspec;	/* Needed to generate the PartitionDesc and
									   PartitionBoundInfo */
	PartitionKey	partkey;	/* Needed to generate the partition key
								   expressions and deparsing */
	Oid		   *partopclass;	/* oid of partkey's element opclass, needed for
								   deparsing the key */
} hypoTable;

/* List of hypothetic partitions for current backend */
extern List	   *hypoTables;
#else
#define HYPO_PARTITION_NOT_SUPPORTED() elog(ERROR, "hypopg: Hypothetical partitioning requires PostgreSQl 10 or above"); PG_RETURN_VOID();
#endif

/*--- Functions --- */

void hypo_table_reset(void);

Datum		hypopg_table(PG_FUNCTION_ARGS);
Datum		hypopg_add_partition(PG_FUNCTION_ARGS);
Datum		hypopg_drop_table(PG_FUNCTION_ARGS);
Datum		hypopg_partition_table(PG_FUNCTION_ARGS);
Datum		hypopg_reset_table(PG_FUNCTION_ARGS);

void hypo_createHypotheticalTable(PlannerInfo *root,
								  Oid relationObjectId,
								  RelOptInfo *rel);
void hypo_setHypotheticalDummyrel(PlannerInfo *root, RelOptInfo *rel,
								  Index rti, RangeTblEntry *rte);

#endif
