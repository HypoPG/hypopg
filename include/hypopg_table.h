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

#define HYPO_TABLE_NB_COLS		6	/* # of column hypopg_table() returns */
#define HYPO_ADD_PART_COLS	2	/* # of column hypopg_add_partition() returns */

#define HYPO_RTE_IS_TAGGED(rte) (rte && (rte->security_barrier))
#define HYPO_RTI_IS_TAGGED(rti, root) (planner_rt_fetch(rti, root)->security_barrier)
#define HYPO_TAG_RTI(rti, root) (planner_rt_fetch(rti, root)->security_barrier = true)

/* XXX maybe use a mapping array here instead of rte->values_lists*/
#define HYPO_TABLE_RTE_HAS_HYPOOID(rte) (rte && (rte->values_lists != NIL))
#define HYPO_TABLE_RTE_GET_HYPOOID(rte) linitial_oid(rte->values_lists)
#define HYPO_TABLE_RTI_GET_HYPOOID(rti, root) linitial_oid(planner_rt_fetch(rti, root)->values_lists)
#define HYPO_TABLE_RTE_SET_HYPOOID(rte, oid) rte->values_lists = list_make1_oid(oid)
#define HYPO_TABLE_RTE_CLEAR_HYPOOID(rte) rte->values_lists = NIL
#define HYPO_TABLE_RTE_COPY_HYPOOID_FROM_RTE(target, src) target->values_lists = src->values_lists

#include "optimizer/paths.h"

/*--- Structs --- */

/*--------------------------------------------------------
 * Hypothetical partition storage.
 * Some dynamic informations such as pages and lines can be stored by
 * hypopg_analyze().  If not, they'll be computed when the hypothetical
 * partition is used.
 */
typedef struct hypoTable
{
	Oid			oid;			/* hypothetical table unique identifier */
	Oid			parentid;		/* In case of partition, it's direct parent,
								   otherwise InvalidOid */
	Oid			rootid;			/* In case of partition, its root parentid,
								   otherwise its own oid */
	char		tablename[NAMEDATALEN];		/* hypothetical partition name, or
								   original table name for root parititon */
	Oid			namespace;		/* Oid of the hypothetical table's schema */
	bool		set_tuples;		/* tuples are already set or not */
	int			tuples;		    /* number of tuples of this table */
	List	   *children;		/* unsorted OIDs of children, if any */
	PartitionBoundSpec	*boundspec;	/* Needed to generate the PartitionDesc and
									   PartitionBoundInfo */
	PartitionKey	partkey;	/* Needed to generate the partition key
								   expressions and deparsing */
	Oid		   *partopclass;	/* oid of partkey's element opclass, needed for
								   deparsing the key */
	bool		valid;
} hypoTable;

/* List of hypothetic partitions for current backend */
extern HTAB	   *hypoTables;
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

#if PG_VERSION_NUM >= 100000
hypoTable *hypo_find_table(Oid tableid, bool missing_ok);
List *hypo_get_partition_constraints(PlannerInfo *root, RelOptInfo *rel,
					    hypoTable *parent, bool force_generation);
List *hypo_get_partition_quals_inh(hypoTable *part, hypoTable *parent);
hypoTable *hypo_table_name_get_entry(const char *name);
bool hypo_table_oid_is_hypothetical(Oid relid);
bool hypo_table_remove(Oid tableid, hypoTable *parent, bool deep);
void hypo_injectHypotheticalPartitioning(PlannerInfo *root,
					 Oid relationObjectId,
					 RelOptInfo *rel);
#if PG_VERSION_NUM < 110000
void hypo_markDummyIfExcluded(PlannerInfo *root, RelOptInfo *rel,
							  Index rti, RangeTblEntry *rte);
#endif
#endif

#endif
