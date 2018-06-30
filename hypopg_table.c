/*-------------------------------------------------------------------------
 *
 * hypopg_table.c: Implementation of hypothetical partitioning for PostgreSQL
 *
 * This file contains all the internal code related to hypothetical partition
 * support.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (C) 2015-2018: Julien Rouhaud
 *
 *-------------------------------------------------------------------------
 */

#include <math.h>

#include "postgres.h"
#include "fmgr.h"

#include "funcapi.h"
#include "miscadmin.h"

#if PG_VERSION_NUM >= 100000
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/relation.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/predtest.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_utilcmd.h"
#include "rewrite/rewriteManip.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "partitioning/partbounds.h"
#endif

#include "include/hypopg.h"
#include "include/hypopg_analyze.h"
#include "include/hypopg_table.h"

/*--- Variables exported ---*/

List	   *hypoTables;

/*--- Functions --- */

PG_FUNCTION_INFO_V1(hypopg_add_partition);
PG_FUNCTION_INFO_V1(hypopg_drop_table);
PG_FUNCTION_INFO_V1(hypopg_partition_table);
PG_FUNCTION_INFO_V1(hypopg_reset_table);
PG_FUNCTION_INFO_V1(hypopg_table);


#if PG_VERSION_NUM >= 100000
static void hypo_addTable(hypoTable *entry);
static int hypo_expand_partitioned_entry(PlannerInfo *root, Oid
		relationObjectId, RelOptInfo *rel, Relation parentrel,
		hypoTable *branch, int firstpos, int parent_rti);
static void hypo_expand_single_inheritance_child(PlannerInfo *root,
		Oid relationObjectId, RelOptInfo *rel, Relation parentrel,
		hypoTable *branch, RangeTblEntry *rte, hypoTable *child, Oid newrelid,
		int parent_rti, bool expandBranch);
static List *hypo_find_inheritance_children(hypoTable *parent);
static List *hypo_get_qual_from_partbound(hypoTable *parent,
		PartitionBoundSpec *spec);
static PartitionDesc hypo_generate_partitiondesc(hypoTable *parent);
static void hypo_generate_partkey(CreateStmt *stmt, Oid parentid,
		hypoTable *entry);
static PartitionScheme hypo_find_partition_scheme(PlannerInfo *root,
												  PartitionKey partkey);
static void hypo_generate_partition_key_exprs(hypoTable *entry,
		RelOptInfo *rel);
static PartitionBoundSpec *hypo_get_boundspec(Oid tableid);
static Oid hypo_get_default_partition_oid(Oid parentid);
static char *hypo_get_partbounddef(hypoTable *entry);
static char *hypo_get_partkeydef(hypoTable *entry);
static hypoTable *hypo_newTable(Oid parentid);
static hypoTable *hypo_table_find_parent_oid(Oid parentid);
static hypoTable *hypo_table_name_get_entry(const char *name);
static void hypo_table_pfree(hypoTable *entry);
static bool hypo_table_remove(Oid tableid);
static const hypoTable *hypo_table_store_parsetree(CreateStmt *node,
						   const char *queryString, Oid parentid, Oid rootid);
static PartitionBoundSpec *hypo_transformPartitionBound(ParseState *pstate,
		hypoTable *parent, PartitionBoundSpec *spec);
static void hypo_set_relation_partition_info(PlannerInfo *root, RelOptInfo *rel,
				 hypoTable *entry);
static List *hypo_get_qual_for_hash(hypoTable *parent, PartitionBoundSpec *spec);
static List *hypo_get_qual_for_list(hypoTable *parent, PartitionBoundSpec *spec);
static List *hypo_get_qual_for_range(hypoTable *parent, PartitionBoundSpec *spec,
									 bool for_default);

#define HYPO_RTI_IS_TAGGED(rti, root) (planner_rt_fetch(rti, root)->security_barrier)
#define HYPO_TAG_RTI(rti, root) (planner_rt_fetch(rti, root)->security_barrier = true)


/* Add an hypoTable to hypoTables */
static void
hypo_addTable(hypoTable *entry)
{
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);

	hypoTables = lappend(hypoTables, entry);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * Adaptation of expand_partitioned_rtentry
 */
static int
hypo_expand_partitioned_entry(PlannerInfo *root, Oid relationObjectId,
		RelOptInfo *rel, Relation parentrel, hypoTable *branch, int firstpos,
		int parent_rti)
{
	hypoTable *parent;
	List *inhoids;
	int nparts;
	RangeTblEntry *rte;
	ListCell *cell;
	int newrelid, oldsize = root->simple_rel_array_size;
	int i;

	Assert(hypo_table_oid_is_hypothetical(relationObjectId));

	if (!branch)
		parent = hypo_find_table(relationObjectId, false);
	else
		parent = branch;

	/* get all of the partition oids */
	inhoids = hypo_find_inheritance_children(parent);
	nparts = list_length(inhoids);

	/*
	 * resize and clean rte and rel arrays.  We need a slot for self expansion
	 * and one per partition
	 */
	root->simple_rel_array_size += nparts + 1;

	root->simple_rel_array = (RelOptInfo **)
		repalloc(root->simple_rel_array,
				root->simple_rel_array_size *
				sizeof(RelOptInfo *));
	root->simple_rte_array = (RangeTblEntry **)
		repalloc(root->simple_rte_array,
				root->simple_rel_array_size *
				sizeof(RangeTblEntry *));

	for (i=oldsize; i<root->simple_rel_array_size; i++)
	{
		root->simple_rel_array[i] = NULL;
		root->simple_rte_array[i] = NULL;
	}

	/* Get the rte from the root partition */
	rte = root->simple_rte_array[rel->relid];

	/*  if this is not the root partition, update the basic metadata */
	if (!branch)
	{
		Assert(parent_rti == -1);

		rte->relkind = RELKIND_PARTITIONED_TABLE;
		rte->inh = true;
		HYPO_TAG_RTI(rel->relid, root);
	}
	else /* branch partition, expand it */
	{
		/* The rte we retrieved has already been updated */
		Assert(rte->relkind == RELKIND_PARTITIONED_TABLE);

		rte = copyObject(rte);
		if(!rte->alias)
			rte->alias = makeNode(Alias);
		rte->alias->aliasname = branch->tablename;

		hypo_expand_single_inheritance_child(root, relationObjectId, rel,
				parentrel, branch, rte, branch, firstpos, parent_rti, true);

		firstpos++;
	}

	/* add the partitioned table itself */
	root->simple_rte_array[firstpos] = rte;

	root->parse->rtable = lappend(root->parse->rtable,
			root->simple_rte_array[firstpos]);

	HYPO_TAG_RTI(firstpos, root);

	/*
	 * create RangeTblEntries and AppendRelInfos hypothetically
	 * for all hypothetical partitions
	 */
	newrelid = firstpos + 1;
	foreach(cell, inhoids)
	{
		Oid childOid = lfirst_oid(cell);
		hypoTable *child;

		child = hypo_find_table(childOid, false);

		/* Expand the child if it's partitioned */
		if (child->partkey)
		{
			/*
			 * firstpos-1 is the ancestor rti for expanded children of
			 * this branch.
			 */
			newrelid = hypo_expand_partitioned_entry(root, relationObjectId,
					rel, parentrel, child, newrelid, firstpos-1);
			continue;
		}

		/* firstpos-1 is the ancestor rti of this children */
		hypo_expand_single_inheritance_child(root, relationObjectId, rel,
				parentrel, branch, rte, child, newrelid, firstpos-1, false);

		newrelid++;
	}

	/* add partition info for root partition */
	if (!branch)
		hypo_set_relation_partition_info(root, rel, parent);

	return newrelid;
}

/*
 * Adaptation of expand_single_inheritance_child
 */
static void
hypo_expand_single_inheritance_child(PlannerInfo *root, Oid relationObjectId,
		RelOptInfo *rel, Relation parentrel, hypoTable *branch,
		RangeTblEntry *rte, hypoTable *child, Oid newrelid, int parent_rti,
		bool expandBranch)
{
	RangeTblEntry *childrte;
	AppendRelInfo *appinfo;

	childrte = copyObject(rte);

	if (!expandBranch)
	{
		childrte->rtekind = RTE_RELATION;
		childrte->inh = false;
	}
	else
	{
		Assert(branch);
		Assert(child == branch);
	}

	childrte->relid  = relationObjectId; //originalOID;
	if (child->partkey)
		childrte->relkind = RELKIND_PARTITIONED_TABLE;
	else
		childrte->relkind = RELKIND_RELATION;

	if(!childrte->alias)
		childrte->alias = makeNode(Alias);
	childrte->alias->aliasname = child->tablename;

	/* FIXME maybe use a mapping array here instead of rte->values_lists*/
	if (expandBranch)
		childrte->values_lists = list_make1_oid(branch->oid); //partitionOID
	else
		childrte->values_lists = list_make1_oid(child->oid); //partitionOID

	root->simple_rte_array[newrelid] = childrte;
	HYPO_TAG_RTI(newrelid, root);

	appinfo = makeNode(AppendRelInfo);

	if (expandBranch || branch)
		appinfo->parent_relid = parent_rti;
	else
		appinfo->parent_relid = rel->relid;

	appinfo->child_relid = newrelid;
	appinfo->parent_reltype = parentrel->rd_rel->reltype;
	appinfo->child_reltype = parentrel->rd_rel->reltype;
	make_inh_translation_list(parentrel, parentrel, newrelid,
			&appinfo->translated_vars);
	appinfo->parent_reloid = relationObjectId;
	root->append_rel_list = lappend(root->append_rel_list,
			appinfo);
	root->parse->rtable = lappend(root->parse->rtable,
			root->simple_rte_array[newrelid]);
}

/*
 * Adaptation of find_inheritance_children().
 *
 * FIXME: simplify the algorithm if we maintain the number of partition per
 * parent.
 */
static List *
hypo_find_inheritance_children(hypoTable *parent)
{
	List	   *list = NIL;
	ListCell   *lc;
	Oid			inhrelid;
	Oid		   *oidarr;
	int			maxoids,
				numoids,
				i;

	Assert(CurrentMemoryContext != HypoMemoryContext);

	maxoids = 32;
	oidarr = (Oid *) palloc(maxoids * sizeof(Oid));
	numoids = 0;

	foreach(lc, hypoTables)
	{
		hypoTable *entry = (hypoTable *) lfirst(lc);

		if (entry->parentid != parent->oid)
			continue;

		inhrelid = entry->oid;
		if (numoids >= maxoids)
		{
			maxoids *= 2;
			oidarr = (Oid *) repalloc(oidarr, maxoids * sizeof(Oid));
		}
		oidarr[numoids++] = inhrelid;
	}
	/*
	 * If we found more than one child, sort them by OID.  This ensures
	 * reasonably consistent behavior regardless of the vagaries of an
	 * indexscan.  This is important since we need to be sure all backends
	 * lock children in the same order to avoid needless deadlocks.
	 */
	if (numoids > 1)
		qsort(oidarr, numoids, sizeof(Oid), oid_cmp);

	/*
	 * Bild the result list.
	 */
	for (i = 0; i < numoids; i++)
	{
		inhrelid = oidarr[i];

		list = lappend_oid(list, inhrelid);
	}

	pfree(oidarr);

	return list;
}

/*
 * Given a (root) hypothetically partitioned table, generate the
 * PartitionBoundInfo data corresponding to the declared hypothetical
 * partitions.  Caller is reponsible of providing the right MemoryContext,
 * however HypoMemoryContext is not allowed.  This is heavily inspired on
 * RelationBuildPartitionDesc().
 */
static PartitionDesc
hypo_generate_partitiondesc(hypoTable *parent)
{
	List	   *inhoids,
			   *partoids;
	Oid		   *oids = NULL;
	List	   *boundspecs = NIL;
	ListCell   *cell;
	int			i,
				nparts;
	PartitionKey key = parent->partkey;
	PartitionDesc result;

	int			ndatums = 0;
	int			default_index = -1;

	/* Hash partitioning specific */
	PartitionHashBound **hbounds = NULL;

	/* List partitioning specific */
	PartitionListValue **all_values = NULL;
	int			null_index = -1;

	/* Range partitioning specific */
	PartitionRangeBound **rbounds = NULL;

	Assert(CurrentMemoryContext != HypoMemoryContext);

	if (key == NULL)
		return NULL;

	/* Get partition oids from pg_inherits */
	inhoids = hypo_find_inheritance_children(parent);

	if (!inhoids)
		return NULL;

	/* Collect bound spec nodes in a list */
	i = 0;
	partoids = NIL;
	foreach(cell, inhoids)
	{
		Oid			inhrelid = lfirst_oid(cell);
		Node	   *boundspec;

		boundspec = (Node *) hypo_get_boundspec(inhrelid);

		/*
		 * Sanity check: If the PartitionBoundSpec says this is the default
		 * partition, its OID should correspond to whatever's stored in
		 * pg_partitioned_table.partdefid; if not, the catalog is corrupt.
		 */
		if (castNode(PartitionBoundSpec, boundspec)->is_default)
		{
			Oid			partdefid;

			partdefid = hypo_get_default_partition_oid(parent->oid);
			if (partdefid != inhrelid)
				elog(ERROR, "expected partdefid %u, but got %u",
					 inhrelid, partdefid);
		}

		boundspecs = lappend(boundspecs, boundspec);
		partoids = lappend_oid(partoids, inhrelid);
	}

	nparts = list_length(partoids);

	if (nparts > 0)
	{
		oids = (Oid *) palloc(nparts * sizeof(Oid));
		i = 0;
		foreach(cell, partoids)
			oids[i++] = lfirst_oid(cell);

		/* Convert from node to the internal representation */
		if (key->strategy == PARTITION_STRATEGY_HASH)
		{
			ndatums = nparts;
			hbounds = (PartitionHashBound **)
				palloc(nparts * sizeof(PartitionHashBound *));

			i = 0;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));

				if (spec->strategy != PARTITION_STRATEGY_HASH)
					elog(ERROR, "invalid strategy in partition bound spec");

				hbounds[i] = (PartitionHashBound *)
					palloc(sizeof(PartitionHashBound));

				hbounds[i]->modulus = spec->modulus;
				hbounds[i]->remainder = spec->remainder;
				hbounds[i]->index = i;
				i++;
			}

			/* Sort all the bounds in ascending order */
			qsort(hbounds, nparts, sizeof(PartitionHashBound *),
				  qsort_partition_hbound_cmp);
		}
		else if (key->strategy == PARTITION_STRATEGY_LIST)
		{
			List	   *non_null_values = NIL;

			/*
			 * Create a unified list of non-null values across all partitions.
			 */
			i = 0;
			null_index = -1;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));
				ListCell   *c;

				if (spec->strategy != PARTITION_STRATEGY_LIST)
					elog(ERROR, "invalid strategy in partition bound spec");

				/*
				 * Note the index of the partition bound spec for the default
				 * partition. There's no datum to add to the list of non-null
				 * datums for this partition.
				 */
				if (spec->is_default)
				{
					default_index = i;
					i++;
					continue;
				}

				foreach(c, spec->listdatums)
				{
					Const	   *val = castNode(Const, lfirst(c));
					PartitionListValue *list_value = NULL;

					if (!val->constisnull)
					{
						list_value = (PartitionListValue *)
							palloc0(sizeof(PartitionListValue));
						list_value->index = i;
						list_value->value = val->constvalue;
					}
					else
					{
						/*
						 * Never put a null into the values array, flag
						 * instead for the code further down below where we
						 * construct the actual relcache struct.
						 */
						if (null_index != -1)
							elog(ERROR, "found null more than once");
						null_index = i;
					}

					if (list_value)
						non_null_values = lappend(non_null_values,
												  list_value);
				}

				i++;
			}

			ndatums = list_length(non_null_values);

			/*
			 * Collect all list values in one array. Alongside the value, we
			 * also save the index of partition the value comes from.
			 */
			all_values = (PartitionListValue **) palloc(ndatums *
														sizeof(PartitionListValue *));
			i = 0;
			foreach(cell, non_null_values)
			{
				PartitionListValue *src = lfirst(cell);

				all_values[i] = (PartitionListValue *)
					palloc(sizeof(PartitionListValue));
				all_values[i]->value = src->value;
				all_values[i]->index = src->index;
				i++;
			}

			qsort_arg(all_values, ndatums, sizeof(PartitionListValue *),
					  qsort_partition_list_value_cmp, (void *) key);
		}
		else if (key->strategy == PARTITION_STRATEGY_RANGE)
		{
			int			k;
			PartitionRangeBound **all_bounds,
					   *prev;

			all_bounds = (PartitionRangeBound **) palloc0(2 * nparts *
														  sizeof(PartitionRangeBound *));

			/*
			 * Create a unified list of range bounds across all the
			 * partitions.
			 */
			i = ndatums = 0;
			foreach(cell, boundspecs)
			{
				PartitionBoundSpec *spec = castNode(PartitionBoundSpec,
													lfirst(cell));
				PartitionRangeBound *lower,
						   *upper;

				if (spec->strategy != PARTITION_STRATEGY_RANGE)
					elog(ERROR, "invalid strategy in partition bound spec");

				/*
				 * Note the index of the partition bound spec for the default
				 * partition. There's no datum to add to the allbounds array
				 * for this partition.
				 */
				if (spec->is_default)
				{
					default_index = i++;
					continue;
				}

				lower = make_one_partition_rbound(key, i, spec->lowerdatums,
											 true);
				upper = make_one_partition_rbound(key, i, spec->upperdatums,
											 false);
				all_bounds[ndatums++] = lower;
				all_bounds[ndatums++] = upper;
				i++;
			}

			Assert(ndatums == nparts * 2 ||
				   (default_index != -1 && ndatums == (nparts - 1) * 2));

			/* Sort all the bounds in ascending order */
			qsort_arg(all_bounds, ndatums,
					  sizeof(PartitionRangeBound *),
					  qsort_partition_rbound_cmp,
					  (void *) key);

			/* Save distinct bounds from all_bounds into rbounds. */
			rbounds = (PartitionRangeBound **)
				palloc(ndatums * sizeof(PartitionRangeBound *));
			k = 0;
			prev = NULL;
			for (i = 0; i < ndatums; i++)
			{
				PartitionRangeBound *cur = all_bounds[i];
				bool		is_distinct = false;
				int			j;

				/* Is the current bound distinct from the previous one? */
				for (j = 0; j < key->partnatts; j++)
				{
					Datum		cmpval;

					if (prev == NULL || cur->kind[j] != prev->kind[j])
					{
						is_distinct = true;
						break;
					}

					/*
					 * If the bounds are both MINVALUE or MAXVALUE, stop now
					 * and treat them as equal, since any values after this
					 * point must be ignored.
					 */
					if (cur->kind[j] != PARTITION_RANGE_DATUM_VALUE)
						break;

					cmpval = FunctionCall2Coll(&key->partsupfunc[j],
											   key->partcollation[j],
											   cur->datums[j],
											   prev->datums[j]);
					if (DatumGetInt32(cmpval) != 0)
					{
						is_distinct = true;
						break;
					}
				}

				/*
				 * Only if the bound is distinct save it into a temporary
				 * array i.e. rbounds which is later copied into boundinfo
				 * datums array.
				 */
				if (is_distinct)
					rbounds[k++] = all_bounds[i];

				prev = cur;
			}

			/* Update ndatums to hold the count of distinct datums. */
			ndatums = k;
		}
		else
			elog(ERROR, "unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	result = (PartitionDescData *) palloc0(sizeof(PartitionDescData));
	result->nparts = nparts;
	if (nparts > 0)
	{
		PartitionBoundInfo boundinfo;
		int		   *mapping;
		int			next_index = 0;

		result->oids = (Oid *) palloc0(nparts * sizeof(Oid));

		boundinfo = (PartitionBoundInfoData *)
			palloc0(sizeof(PartitionBoundInfoData));
		boundinfo->strategy = key->strategy;
		boundinfo->default_index = -1;
		boundinfo->ndatums = ndatums;
		boundinfo->null_index = -1;
		boundinfo->datums = (Datum **) palloc0(ndatums * sizeof(Datum *));

		/* Initialize mapping array with invalid values */
		mapping = (int *) palloc(sizeof(int) * nparts);
		for (i = 0; i < nparts; i++)
			mapping[i] = -1;

		switch (key->strategy)
		{
			case PARTITION_STRATEGY_HASH:
				{
					/* Modulus are stored in ascending order */
					int			greatest_modulus = hbounds[ndatums - 1]->modulus;

					boundinfo->indexes = (int *) palloc(greatest_modulus *
														sizeof(int));

					for (i = 0; i < greatest_modulus; i++)
						boundinfo->indexes[i] = -1;

					for (i = 0; i < nparts; i++)
					{
						int			modulus = hbounds[i]->modulus;
						int			remainder = hbounds[i]->remainder;

						boundinfo->datums[i] = (Datum *) palloc(2 *
																sizeof(Datum));
						boundinfo->datums[i][0] = Int32GetDatum(modulus);
						boundinfo->datums[i][1] = Int32GetDatum(remainder);

						while (remainder < greatest_modulus)
						{
							/* overlap? */
							Assert(boundinfo->indexes[remainder] == -1);
							boundinfo->indexes[remainder] = i;
							remainder += modulus;
						}

						mapping[hbounds[i]->index] = i;
						pfree(hbounds[i]);
					}
					pfree(hbounds);
					break;
				}

			case PARTITION_STRATEGY_LIST:
				{
					boundinfo->indexes = (int *) palloc(ndatums * sizeof(int));

					/*
					 * Copy values.  Indexes of individual values are mapped
					 * to canonical values so that they match for any two list
					 * partitioned tables with same number of partitions and
					 * same lists per partition.  One way to canonicalize is
					 * to assign the index in all_values[] of the smallest
					 * value of each partition, as the index of all of the
					 * partition's values.
					 */
					for (i = 0; i < ndatums; i++)
					{
						boundinfo->datums[i] = (Datum *) palloc(sizeof(Datum));
						boundinfo->datums[i][0] = datumCopy(all_values[i]->value,
															key->parttypbyval[0],
															key->parttyplen[0]);

						/* If the old index has no mapping, assign one */
						if (mapping[all_values[i]->index] == -1)
							mapping[all_values[i]->index] = next_index++;

						boundinfo->indexes[i] = mapping[all_values[i]->index];
					}

					/*
					 * If null-accepting partition has no mapped index yet,
					 * assign one.  This could happen if such partition
					 * accepts only null and hence not covered in the above
					 * loop which only handled non-null values.
					 */
					if (null_index != -1)
					{
						Assert(null_index >= 0);
						if (mapping[null_index] == -1)
							mapping[null_index] = next_index++;
						boundinfo->null_index = mapping[null_index];
					}

					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						/*
						 * The default partition accepts any value not
						 * specified in the lists of other partitions, hence
						 * it should not get mapped index while assigning
						 * those for non-null datums.
						 */
						Assert(default_index >= 0 &&
							   mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}

					/* All partition must now have a valid mapping */
					Assert(next_index == nparts);
					break;
				}

			case PARTITION_STRATEGY_RANGE:
				{
					boundinfo->kind = (PartitionRangeDatumKind **)
						palloc(ndatums *
							   sizeof(PartitionRangeDatumKind *));
					boundinfo->indexes = (int *) palloc((ndatums + 1) *
														sizeof(int));

					for (i = 0; i < ndatums; i++)
					{
						int			j;

						boundinfo->datums[i] = (Datum *) palloc(key->partnatts *
																sizeof(Datum));
						boundinfo->kind[i] = (PartitionRangeDatumKind *)
							palloc(key->partnatts *
								   sizeof(PartitionRangeDatumKind));
						for (j = 0; j < key->partnatts; j++)
						{
							if (rbounds[i]->kind[j] == PARTITION_RANGE_DATUM_VALUE)
								boundinfo->datums[i][j] =
									datumCopy(rbounds[i]->datums[j],
											  key->parttypbyval[j],
											  key->parttyplen[j]);
							boundinfo->kind[i][j] = rbounds[i]->kind[j];
						}

						/*
						 * There is no mapping for invalid indexes.
						 *
						 * Any lower bounds in the rbounds array have invalid
						 * indexes assigned, because the values between the
						 * previous bound (if there is one) and this (lower)
						 * bound are not part of the range of any existing
						 * partition.
						 */
						if (rbounds[i]->lower)
							boundinfo->indexes[i] = -1;
						else
						{
							int			orig_index = rbounds[i]->index;

							/* If the old index has no mapping, assign one */
							if (mapping[orig_index] == -1)
								mapping[orig_index] = next_index++;

							boundinfo->indexes[i] = mapping[orig_index];
						}
					}

					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						Assert(default_index >= 0 && mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}
					boundinfo->indexes[i] = -1;
					break;
				}

			default:
				elog(ERROR, "unexpected partition strategy: %d",
					 (int) key->strategy);
		}
		result->boundinfo = boundinfo;

		/*
		 * Now assign OIDs from the original array into mapped indexes of the
		 * result array.  Order of OIDs in the former is defined by the
		 * catalog scan that retrieved them, whereas that in the latter is
		 * defined by canonicalized representation of the partition bounds.
		 */
		for (i = 0; i < nparts; i++)
			result->oids[mapping[i]] = oids[i];
		pfree(mapping);
	}

	return result;
}

/*
 * Given a CreateStmt, generate a PartitionKey corresponding to the provided
 * PartitionSpec clause, and also store each key's opclass as it's needed for
 * the deparsing.  This is heavily inspired on DefineRelation() and
 * RelationBuildPartitionKey().
 */
static void
hypo_generate_partkey(CreateStmt *stmt, Oid parentid, hypoTable *entry)
{
	char			strategy;
	int				partnatts;
	AttrNumber		partattrs[PARTITION_MAX_KEYS];
	Oid				partopclass[PARTITION_MAX_KEYS];
	Oid				partcollation[PARTITION_MAX_KEYS];
	List		   *partexprs = NIL;
	Relation		rel;
	PartitionKey	key;
	int				i;
	MemoryContext	oldcontext;

	Assert(stmt->partspec);

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
	key = (PartitionKey) palloc0(sizeof(PartitionKeyData));
	MemoryContextSwitchTo(oldcontext);

	rel = heap_open(parentid, AccessShareLock);

	/*--- Adapted from DefineRelation ---*/
	partnatts = list_length(stmt->partspec->partParams);
	key->partnatts = partnatts;

	/* Protect fixed-size arrays here and in executor */
	if (partnatts > PARTITION_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("cannot partition using more than %d columns",
						PARTITION_MAX_KEYS)));

	/*
	 * We need to transform the raw parsetrees corresponding to partition
	 * expressions into executable expression trees.  Like column defaults
	 * and CHECK constraints, we could not have done the transformation
	 * earlier.
	 */
	stmt->partspec = transformPartitionSpec(rel, stmt->partspec,
											&strategy);

	ComputePartitionAttrs(rel, stmt->partspec->partParams,
						  partattrs, &partexprs, partopclass,
						  partcollation, strategy);

	key->strategy = strategy;
	key->partexprs = partexprs;

	/*--- Adapted from RelationBuildPartitionKey ---*/
{
	ListCell   *partexprs_item;
	int16		procnum;

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);

	key->partopfamily = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->partopcintype = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->partsupfunc = (FmgrInfo *) palloc0(key->partnatts * sizeof(FmgrInfo));

	key->partcollation = (Oid *) palloc0(key->partnatts * sizeof(Oid));

	/* Gather type and collation info as well */
	key->parttypid = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->parttypmod = (int32 *) palloc0(key->partnatts * sizeof(int32));
	key->parttyplen = (int16 *) palloc0(key->partnatts * sizeof(int16));
	key->parttypbyval = (bool *) palloc0(key->partnatts * sizeof(bool));
	key->parttypalign = (char *) palloc0(key->partnatts * sizeof(char));
	key->parttypcoll = (Oid *) palloc0(key->partnatts * sizeof(Oid));
	key->partattrs = (int16 *) palloc0(key->partnatts * sizeof(int16));

	entry->partopclass = (Oid *) palloc0(key->partnatts * sizeof(Oid));

	MemoryContextSwitchTo(oldcontext);

	/* determine support function number to search for */
	procnum = (key->strategy == PARTITION_STRATEGY_HASH) ?
		HASHEXTENDED_PROC : BTORDER_PROC;

	/* Copy partattrs and fill other per-attribute info */
	memcpy(key->partattrs, partattrs, key->partnatts * sizeof(int16));
	partexprs_item = list_head(key->partexprs);
	for (i = 0; i < key->partnatts; i++)
	{
		AttrNumber	attno = key->partattrs[i];
		HeapTuple	opclasstup;
		Form_pg_opclass opclassform;
		Oid			funcid;

		/* Collect opfamily information */
		opclasstup = SearchSysCache1(CLAOID,
									 ObjectIdGetDatum(partopclass[i]));
		if (!HeapTupleIsValid(opclasstup))
			elog(ERROR, "cache lookup failed for opclass %u", partopclass[i]);

		opclassform = (Form_pg_opclass) GETSTRUCT(opclasstup);
		key->partopfamily[i] = opclassform->opcfamily;
		key->partopcintype[i] = opclassform->opcintype;

		/* Get a support function for the specified opfamily and datatypes */
		funcid = get_opfamily_proc(opclassform->opcfamily,
								   opclassform->opcintype,
								   opclassform->opcintype,
								   procnum);
		if (!OidIsValid(funcid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("operator class \"%s\" of access method %s is missing support function %d for type %s",
							NameStr(opclassform->opcname),
							(key->strategy == PARTITION_STRATEGY_HASH) ?
							"hash" : "btree",
							procnum,
							format_type_be(opclassform->opcintype))));

		fmgr_info(funcid, &key->partsupfunc[i]);

		/* Collation */
		key->partcollation[i] = partcollation[i];

		/* Collect type information */
		if (attno != 0)
		{
			Form_pg_attribute att = TupleDescAttr(rel->rd_att, attno - 1);

			key->parttypid[i] = att->atttypid;
			key->parttypmod[i] = att->atttypmod;
			key->parttypcoll[i] = att->attcollation;
		}
		else
		{
			if (partexprs_item == NULL)
				elog(ERROR, "wrong number of partition key expressions");

			key->parttypid[i] = exprType(lfirst(partexprs_item));
			key->parttypmod[i] = exprTypmod(lfirst(partexprs_item));
			key->parttypcoll[i] = exprCollation(lfirst(partexprs_item));

			partexprs_item = lnext(partexprs_item);
		}
		get_typlenbyvalalign(key->parttypid[i],
							 &key->parttyplen[i],
							 &key->parttypbyval[i],
							 &key->parttypalign[i]);

		ReleaseSysCache(opclasstup);
	}
}

	heap_close(rel, AccessShareLock);


	/*--------
	 * Copied in previous loop
	key->partattrs = partattrs;
	key->partopfamily = partopfamily;
	key->partopcintype = partopcintype;
	key->partsupfunc = partsupfunc;
	key->partcollation = partcollation;
	key->parttypid = parttypid;
	key->parttypmod = parttypmod
	key->parttyplen = parttyplen;
	key->parttypbyval = parttypbyval;
	key->parttypalign = parttypalign;
	key->parttypcoll= parttypcoll;
	*/

	entry->partkey = key;

	memcpy(entry->partopclass, partopclass, sizeof(Oid) * partnatts);
}

/*
 *
 * Given a PlannerInfo and PartitionKey, find or create a PartitionScheme for
 * this relation.
 *
 * Heavily inspired on find_partition_scheme()
 */
static PartitionScheme
hypo_find_partition_scheme(PlannerInfo *root, PartitionKey partkey)
{
	PartitionScheme part_scheme;
	int			partnatts,
		        i;
	ListCell *lc;

	Assert(CurrentMemoryContext != HypoMemoryContext);

	/* adapted from plancat.c / find_partition_scheme() */
	Assert(partkey != NULL);

	partnatts = partkey->partnatts;

	foreach(lc, root->part_schemes)
	{
		part_scheme = lfirst(lc);

		/* Match partitioning strategy and number of keys. */
		if (partkey->strategy != part_scheme->strategy ||
			partnatts != part_scheme->partnatts)
			continue;

		/* Match partition key type properties. */
		if (memcmp(partkey->partopfamily, part_scheme->partopfamily,
				   sizeof(Oid) * partnatts) != 0 ||
			memcmp(partkey->partopcintype, part_scheme->partopcintype,
				   sizeof(Oid) * partnatts) != 0 ||
			memcmp(partkey->partcollation, part_scheme->partcollation,
				   sizeof(Oid) * partnatts) != 0)
			continue;

		/*
		 * Length and byval information should match when partopcintype
		 * matches.
		 */
		Assert(memcmp(partkey->parttyplen, part_scheme->parttyplen,
					  sizeof(int16) * partnatts) == 0);
		Assert(memcmp(partkey->parttypbyval, part_scheme->parttypbyval,
					  sizeof(bool) * partnatts) == 0);

		/*
		 * If partopfamily and partopcintype matched, must have the same
		 * partition comparison functions.  Note that we cannot reliably
		 * Assert the equality of function structs themselves for they might
		 * be different across PartitionKey's, so just Assert for the function
		 * OIDs.
		 */
#ifdef USE_ASSERT_CHECKING
		for (i = 0; i < partkey->partnatts; i++)
			Assert(partkey->partsupfunc[i].fn_oid ==
				   part_scheme->partsupfunc[i].fn_oid);
#endif

		/* Found matching partition scheme. */
		return part_scheme;
	}


	part_scheme = (PartitionScheme) palloc0(sizeof(PartitionSchemeData));
	part_scheme->strategy = partkey->strategy;
	part_scheme->partnatts = partkey->partnatts;

	part_scheme->partopfamily = (Oid *) palloc(sizeof(Oid) * partnatts);
	memcpy(part_scheme->partopfamily, partkey->partopfamily,
		   sizeof(Oid) * partnatts);

	part_scheme->partopcintype = (Oid *) palloc(sizeof(Oid) * partnatts);
	memcpy(part_scheme->partopcintype, partkey->partopcintype,
		   sizeof(Oid) * partnatts);

	part_scheme->partcollation = (Oid *) palloc(sizeof(Oid) * partnatts);
	memcpy(part_scheme->partcollation, partkey->partcollation,
		   sizeof(Oid) * partnatts);
	//part_scheme->parttypcoll = (Oid *) palloc(sizeof(Oid) * partnatts);
	//memcpy(part_scheme->parttypcoll, partkey->parttypcoll,
	//	   sizeof(Oid) * partnatts);

	part_scheme->parttyplen = (int16 *) palloc(sizeof(int16) * partnatts);
	memcpy(part_scheme->parttyplen, partkey->parttyplen,
		   sizeof(int16) * partnatts);

	part_scheme->parttypbyval = (bool *) palloc(sizeof(bool) * partnatts);
	memcpy(part_scheme->parttypbyval, partkey->parttypbyval,
		   sizeof(bool) * partnatts);

	part_scheme->partsupfunc = (FmgrInfo *)
		palloc(sizeof(FmgrInfo) * partnatts);
	for (i = 0; i < partnatts; i++)
		fmgr_info_copy(&part_scheme->partsupfunc[i], &partkey->partsupfunc[i],
					   CurrentMemoryContext);

	/* Add the partitioning scheme to PlannerInfo. */
	root->part_schemes = lappend(root->part_schemes, part_scheme);

	return part_scheme;
}

/*
 * Given a PartitionKey, fill the PartitionScheme->partexrps struct.
 *
 * Heavily inspired on set_baserel_partition_key_exprs
 */
static void
hypo_generate_partition_key_exprs(hypoTable *entry, RelOptInfo *rel)
{
	PartitionKey partkey = entry->partkey;
	int			partnatts;
	int			cnt;
	List	  **partexprs;
	ListCell   *lc;
	Index		varno = rel->relid;

	Assert(CurrentMemoryContext != HypoMemoryContext);

	/* A partitioned table should have a partition key. */
	Assert(partkey != NULL);

	partnatts = partkey->partnatts;
	partexprs = (List **) palloc(sizeof(List *) * partnatts);
	lc = list_head(partkey->partexprs);

	for (cnt = 0; cnt < partnatts; cnt++)
	{
		Expr	   *partexpr;
		AttrNumber	attno = partkey->partattrs[cnt];

		if (attno != InvalidAttrNumber)
		{
			/* Single column partition key is stored as a Var node. */
			Assert(attno > 0);

			partexpr = (Expr *) makeVar(varno, attno,
										partkey->parttypid[cnt],
										partkey->parttypmod[cnt],
										partkey->parttypcoll[cnt], 0);
		}
		else
		{
			if (lc == NULL)
				elog(ERROR, "wrong number of partition key expressions");

			/* Re-stamp the expression with given varno. */
			partexpr = (Expr *) copyObject(lfirst(lc));
			ChangeVarNodes((Node *) partexpr, 1, varno, 0);
			lc = lnext(lc);
		}

		partexprs[cnt] = list_make1(partexpr);
	}

	rel->partexprs = partexprs;

	/*
	 * A base relation can not have nullable partition key expressions. We
	 * still allocate array of empty expressions lists to keep partition key
	 * expression handling code simple. See build_joinrel_partition_info() and
	 * match_expr_to_partition_keys().
	 */
	rel->nullable_partexprs = (List **) palloc0(sizeof(List *) * partnatts);
}

/*
 * Return the PartitionBoundSpec associated to a partition if any, otherwise
 * return NULL
 */
static PartitionBoundSpec *
hypo_get_boundspec(Oid tableid)
{
	hypoTable *entry = hypo_find_table(tableid, true);

	if (entry)
		return entry->boundspec;

	return NULL;
}

/*
 * Return the Oid of the default partition if any, otherwise return InvalidOid
 */
static Oid
hypo_get_default_partition_oid(Oid parentid)
{
	ListCell *lc;

	foreach(lc, hypoTables)
	{
		hypoTable *entry = (hypoTable *) lfirst(lc);

		if (entry->parentid != parentid)
			continue;

		if (entry->boundspec->is_default)
			return entry->oid;
	}

	return InvalidOid;
}

/*
 * Deparse the stored PartitionBoundSpec data
 *
 * Heavily inspired on get_rule_expr()
 */
static char *
hypo_get_partbounddef(hypoTable *entry)
{
	StringInfoData	_buf;
	StringInfo buf;
	PartitionBoundSpec *spec = entry->boundspec;
	ListCell   *cell;
	char	   *sep;
	deparse_context _context;
	deparse_context *context = &_context;

	/* Simulate the context that should get passed */
	initStringInfo(&_buf);
	buf = &_buf;
	_context.buf = &_buf;

	if (spec->is_default)
	{
		appendStringInfoString(buf, "DEFAULT");
		return buf->data;
	}

	switch (spec->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			Assert(spec->modulus > 0 && spec->remainder >= 0);
			Assert(spec->modulus > spec->remainder);

			appendStringInfoString(buf, "FOR VALUES");
			appendStringInfo(buf, " WITH (modulus %d, remainder %d)",
							 spec->modulus, spec->remainder);
			break;

		case PARTITION_STRATEGY_LIST:
			Assert(spec->listdatums != NIL);

			appendStringInfoString(buf, "FOR VALUES IN (");
			sep = "";
			foreach(cell, spec->listdatums)
			{
				Const	   *val = castNode(Const, lfirst(cell));

				appendStringInfoString(buf, sep);
				get_const_expr(val, context, -1);
				sep = ", ";
			}

			appendStringInfoChar(buf, ')');
			break;

		case PARTITION_STRATEGY_RANGE:
			Assert(spec->lowerdatums != NIL &&
				   spec->upperdatums != NIL &&
				   list_length(spec->lowerdatums) ==
				   list_length(spec->upperdatums));

			appendStringInfo(buf, "FOR VALUES FROM %s TO %s",
							 get_range_partbound_string(spec->lowerdatums),
							 get_range_partbound_string(spec->upperdatums));
			break;

		default:
			elog(ERROR, "unrecognized partition strategy: %d",
				 (int) spec->strategy);
			break;
	}

	return buf->data;
}

/*
 * Deparse the stored PartitionKey data
 *
 * Heavily inspired on pg_get_partkeydef_worker()
 */
static char *
hypo_get_partkeydef(hypoTable *entry)
{
	StringInfoData	buf;
	PartitionKey	partkey = entry->partkey;
	Oid			relid;
	ListCell   *partexpr_item;
	List	   *context;
	int			keyno;
	char	   *str;
	char	   *sep;

	if (!partkey)
		elog(ERROR, "hypopg: hypothetical table %s is not partitioned",
				quote_identifier(entry->tablename));

	relid = entry->rootid;

	partexpr_item = list_head(partkey->partexprs);
	context = deparse_context_for(get_relation_name(relid), relid);

	initStringInfo(&buf);
	appendStringInfo(&buf, "PARTITION BY ");
	switch(partkey->strategy)
	{
		case PARTITION_STRATEGY_HASH:
			appendStringInfo(&buf, "HASH");
			break;
		case PARTITION_STRATEGY_LIST:
			appendStringInfo(&buf, "LIST");
			break;
		case PARTITION_STRATEGY_RANGE:
			appendStringInfo(&buf, "RANGE");
			break;
		default:
			elog(ERROR, "hypopg: unexpected partition strategy %d",
					(int) partkey->strategy);
	}

	appendStringInfoString(&buf, " (");
	sep = "";
	for (keyno = 0; keyno < partkey->partnatts; keyno++)
	{
		AttrNumber	attnum = partkey->partattrs[keyno];
		Oid			keycoltype;
		Oid			keycolcollation;
		Oid			partcoll;

		appendStringInfoString(&buf, sep);
		sep = ", ";
		if (attnum != 0)
		{
			/* Simple attribute reference */
			char	   *attname;
			int32		keycoltypmod;

			attname = get_attname(relid, attnum, false);
			appendStringInfoString(&buf, quote_identifier(attname));
			get_atttypetypmodcoll(relid, attnum,
								  &keycoltype, &keycoltypmod,
								  &keycolcollation);
		}
		else
		{
			/* Expression */
			Node	   *partkey;

			if (partexpr_item == NULL)
				elog(ERROR, "too few entries in partexprs list");
			partkey = (Node *) lfirst(partexpr_item);
			partexpr_item = lnext(partexpr_item);

			/* Deparse */
			str = deparse_expression(partkey, context, false, false);
			/* Need parens if it's not a bare function call */
			if (looks_like_function(partkey))
				appendStringInfoString(&buf, str);
			else
				appendStringInfo(&buf, "(%s)", str);

			keycoltype = exprType(partkey);
			keycolcollation = exprCollation(partkey);
		}

		/* Add collation, if not default for column */
		partcoll = partkey->partcollation[keyno];
		if (OidIsValid(partcoll) && partcoll != keycolcollation)
			appendStringInfo(&buf, " COLLATE %s",
							 generate_collation_name((partcoll)));

		/* Add the operator class name, if not default */
		get_opclass_name(entry->partopclass[keyno], keycoltype, &buf);
	}

	appendStringInfoChar(&buf, ')');

	return buf.data;
}

/*
 * palloc a new hypoTable, and give it a new OID, and some other global stuff.
 */
static hypoTable *
hypo_newTable(Oid parentid)
{
	hypoTable	   *entry;
	hypoTable	   *parent;
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);

	entry = (hypoTable *) palloc0(sizeof(hypoTable));

	entry->tablename = palloc0(NAMEDATALEN);
	entry->boundspec = NULL; /* wil be generated later if needed */
	entry->partkey = NULL; /* wil be generated later if needed */

	MemoryContextSwitchTo(oldcontext);

	/*
	 * If the given root table oid isn't present in hypoTables, we're
	 * partitioning it, so keep its oid, otherwise generate a new oid
	 */
	parent= hypo_table_find_parent_oid(parentid);

	if (parent)
	{
		entry->parentid = parent->oid;
		entry->oid = hypo_getNewOid(parent->rootid);
		entry->rootid = parent->rootid;
	}
	else
	{
		entry->parentid = InvalidOid;
		entry->oid = parentid;
		entry->rootid = parentid;
	}

	return entry;
}

/*
 * Find the direct parent oid of a hypothetical partition given it's parentid
 * field.  Return InvalidOid is it's a top level table.
 */
static hypoTable *
hypo_table_find_parent_oid(Oid parentid)
{
	ListCell *lc;

	foreach(lc, hypoTables)
	{
		hypoTable *entry = (hypoTable *) lfirst(lc);

		if (entry->oid == parentid)
			return entry;
	}

	return NULL;
}

/*
 * Return the stored hypothetical table corresponding to the Oid if any,
 * otherwise return NULL
 */
hypoTable *
hypo_find_table(Oid tableid, bool missing_ok)
{
	ListCell *lc;

	foreach(lc, hypoTables)
	{
		hypoTable *entry = (hypoTable *) lfirst(lc);

		if (entry->oid != tableid)
			continue;

		return entry;
	}

	if (!missing_ok)
		elog(ERROR, "hypopg: could not find parent for %d", tableid);

	return NULL;
}

/*
 * Return the hypothetical oid if  the given name is an hypothetical partition,
 * otherwise return InvalidOid
 */
static hypoTable *
hypo_table_name_get_entry(const char *name)
{
	ListCell   *lc;

	foreach(lc, hypoTables)
	{
		hypoTable  *entry = (hypoTable *) lfirst(lc);

		if (strcmp(entry->tablename, name) == 0)
			return entry;
	}

	return NULL;
}

/*
 * Is the given relid an hypothetical partition ?
 */
bool
hypo_table_oid_is_hypothetical(Oid relid)
{
	ListCell   *lc;

	foreach(lc, hypoTables)
	{
		hypoTable  *entry = (hypoTable *) lfirst(lc);

		if (entry->oid == relid)
			return true;
	}

	return false;
}

/* pfree all allocated memory for within an hypoTable and the entry itself. */
static void
hypo_table_pfree(hypoTable *entry)
{
	/* pfree all memory that has been allocated */
	pfree(entry->tablename);

	if (entry->boundspec)
		pfree(entry->boundspec);

	if (entry->partkey)
	{
		pfree(entry->partkey->partopfamily);
		pfree(entry->partkey->partopcintype);
		pfree(entry->partkey->partsupfunc);
		pfree(entry->partkey->partcollation);
		pfree(entry->partkey->parttypid);
		pfree(entry->partkey->parttypmod);
		pfree(entry->partkey->parttyplen);
		pfree(entry->partkey->parttypbyval);
		pfree(entry->partkey->parttypalign);
		pfree(entry->partkey->parttypcoll);
		pfree(entry->partkey);
	}

	if (entry->partopclass)
		pfree(entry->partopclass);

	/* finally pfree the entry */
	pfree(entry);
}

/*
 * Remove an hypothetical table (or unpartition a previously hypothetically
 * partitioned table) from the list of hypothetical tables.  pfree (by calling
 * hypo_table_pfree) all memory that has been allocated.  Also free all stored
 * hypothetical statistics belonging to it if any.
 */
static bool
hypo_table_remove(Oid tableid)
{
	ListCell   *lc;

	foreach(lc, hypoTables)
	{
		hypoTable  *entry = (hypoTable *) lfirst(lc);

		if (entry->oid == tableid)
		{
#if PG_VERSION_NUM >= 100000
			/* Remove any stored statistics */
			hypo_stat_remove(tableid);
#endif

			hypoTables = list_delete_ptr(hypoTables, entry);
			hypo_table_pfree(entry);
			return true;
		}
	}
	return false;
}

/*
 * Remove cleanly all hypothetical tables by calling hypo_table_remove() on
 * each entry. hypo_table_remove() function pfree all allocated memory
 */
void
hypo_table_reset(void)
{
	ListCell   *lc;

	/*
	 * The cell is removed in hypo_table_remove(), so we can't iterate using
	 * standard foreach / lnext macros.
	 */
	while ((lc = list_head(hypoTables)) != NULL)
	{
		hypoTable  *entry = (hypoTable *) lfirst(lc);

		hypo_table_remove(entry->oid);
	}

	list_free(hypoTables);
	hypoTables = NIL;
	return;
}

/*
 * Create an hypothetical partition from its CREATE TABLE parsetree.  This
 * function is where all the hypothetic partition creation is done, except the
 * partition size estimation.
 */
static const hypoTable *
hypo_table_store_parsetree(CreateStmt *node, const char *queryString,
		Oid parentid, Oid rootid)
{
	hypoTable	   *entry;
	List		   *stmts;
	CreateStmt	   *stmt = NULL;
	ListCell	   *lc;
	PartitionBoundSpec *boundspec;

	/* Run parse analysis ... */
	stmts = transformCreateStmt(node, queryString);

	foreach(lc, stmts)
	{
		if (!IsA(lfirst(lc), CreateStmt))
			continue;

		stmt = (CreateStmt *) lfirst(lc);

		/* There should only be one CreateStmt out of transformCreateStmt */
		break;
	}

	Assert(stmt);

	boundspec = stmt->partbound;
	if (boundspec)
	{
		if (boundspec->is_default)
		{
			Oid defaultpart = hypo_get_default_partition_oid(parentid);

			if (defaultpart != InvalidOid)
				elog(ERROR, "partition \"%s\" conflicts with existing default partition \"%s\"",
					quote_identifier(node->relation->relname),
					quote_identifier(hypo_find_table(defaultpart, false)->tablename));
		}
	}

	/* now create the hypothetical index entry */
	entry = hypo_newTable(parentid);

	strncpy(entry->tablename, node->relation->relname, NAMEDATALEN);

	/* The CreateStmt specified a PARTITION BY clause, store it */
	if (stmt->partspec)
		hypo_generate_partkey(stmt, rootid, entry);

	if (boundspec)
	{
		MemoryContext oldcontext;
		ParseState *pstate;

		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;

		oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
		entry->boundspec = hypo_transformPartitionBound(pstate,
				hypo_find_table(parentid, false), boundspec);
		MemoryContextSwitchTo(oldcontext);
	}

	hypo_addTable(entry);

	return entry;
}

/*
 * Adapted from parse_utilcmd.c / transformPartitionBound()
 */
static PartitionBoundSpec *
hypo_transformPartitionBound(ParseState *pstate, hypoTable *parent,
		PartitionBoundSpec *spec)
{
	PartitionBoundSpec *result_spec;
	PartitionKey key = parent->partkey;
	char		strategy = get_partition_strategy(key);
	int			partnatts = get_partition_natts(key);
	List	   *partexprs = get_partition_exprs(key);

	/* Avoid scribbling on input */
	result_spec = copyObject(spec);

	if (spec->is_default)
	{
		if (strategy == PARTITION_STRATEGY_HASH)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("a hash-partitioned table may not have a default partition")));

		/*
		 * In case of the default partition, parser had no way to identify the
		 * partition strategy. Assign the parent's strategy to the default
		 * partition bound spec.
		 */
		result_spec->strategy = strategy;

		return result_spec;
	}

	if (strategy == PARTITION_STRATEGY_HASH)
	{
		if (spec->strategy != PARTITION_STRATEGY_HASH)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid bound specification for a hash partition"),
					 parser_errposition(pstate, exprLocation((Node *) spec))));

		if (spec->modulus <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("modulus for hash partition must be a positive integer")));

		Assert(spec->remainder >= 0);

		if (spec->remainder >= spec->modulus)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("remainder for hash partition must be less than modulus")));
	}
	else if (strategy == PARTITION_STRATEGY_LIST)
	{
		ListCell   *cell;
		char	   *colname;
		Oid			coltype;
		int32		coltypmod;

		if (spec->strategy != PARTITION_STRATEGY_LIST)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid bound specification for a list partition"),
					 parser_errposition(pstate, exprLocation((Node *) spec))));

		/* Get the only column's name in case we need to output an error */
		if (key->partattrs[0] != 0)
			colname = get_attname(parent->rootid,
								  key->partattrs[0], false);
		else
			colname = deparse_expression((Node *) linitial(partexprs),
										 deparse_context_for(parent->tablename,
															 parent->rootid),
										 false, false);
		/* Need its type data too */
		coltype = get_partition_col_typid(key, 0);
		coltypmod = get_partition_col_typmod(key, 0);

		result_spec->listdatums = NIL;
		foreach(cell, spec->listdatums)
		{
			A_Const    *con = castNode(A_Const, lfirst(cell));
			Const	   *value;
			ListCell   *cell2;
			bool		duplicate;

			value = transformPartitionBoundValue(pstate, con,
												 colname, coltype, coltypmod);

			/* Don't add to the result if the value is a duplicate */
			duplicate = false;
			foreach(cell2, result_spec->listdatums)
			{
				Const	   *value2 = castNode(Const, lfirst(cell2));

				if (equal(value, value2))
				{
					duplicate = true;
					break;
				}
			}
			if (duplicate)
				continue;

			result_spec->listdatums = lappend(result_spec->listdatums,
											  value);
		}
	}
	else if (strategy == PARTITION_STRATEGY_RANGE)
	{
		ListCell   *cell1,
				   *cell2;
		int			i,
					j;

		if (spec->strategy != PARTITION_STRATEGY_RANGE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid bound specification for a range partition"),
					 parser_errposition(pstate, exprLocation((Node *) spec))));

		if (list_length(spec->lowerdatums) != partnatts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("FROM must specify exactly one value per partitioning column")));
		if (list_length(spec->upperdatums) != partnatts)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("TO must specify exactly one value per partitioning column")));

		/*
		 * Once we see MINVALUE or MAXVALUE for one column, the remaining
		 * columns must be the same.
		 */
		validateInfiniteBounds(pstate, spec->lowerdatums);
		validateInfiniteBounds(pstate, spec->upperdatums);

		/* Transform all the constants */
		i = j = 0;
		result_spec->lowerdatums = result_spec->upperdatums = NIL;
		forboth(cell1, spec->lowerdatums, cell2, spec->upperdatums)
		{
			PartitionRangeDatum *ldatum = (PartitionRangeDatum *) lfirst(cell1);
			PartitionRangeDatum *rdatum = (PartitionRangeDatum *) lfirst(cell2);
			char	   *colname;
			Oid			coltype;
			int32		coltypmod;
			A_Const    *con;
			Const	   *value;

			/* Get the column's name in case we need to output an error */
			if (key->partattrs[i] != 0)
				colname = get_attname(parent->rootid,
									  key->partattrs[i], false);
			else
			{
				colname = deparse_expression((Node *) list_nth(partexprs, j),
											 deparse_context_for(parent->tablename,
																 parent->oid),
											 false, false);
				++j;
			}
			/* Need its type data too */
			coltype = get_partition_col_typid(key, i);
			coltypmod = get_partition_col_typmod(key, i);

			if (ldatum->value)
			{
				con = castNode(A_Const, ldatum->value);
				value = transformPartitionBoundValue(pstate, con,
													 colname,
													 coltype, coltypmod);
				if (value->constisnull)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("cannot specify NULL in range bound")));
				ldatum = copyObject(ldatum);	/* don't scribble on input */
				ldatum->value = (Node *) value;
			}

			if (rdatum->value)
			{
				con = castNode(A_Const, rdatum->value);
				value = transformPartitionBoundValue(pstate, con,
													 colname,
													 coltype, coltypmod);
				if (value->constisnull)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("cannot specify NULL in range bound")));
				rdatum = copyObject(rdatum);	/* don't scribble on input */
				rdatum->value = (Node *) value;
			}

			result_spec->lowerdatums = lappend(result_spec->lowerdatums,
											   ldatum);
			result_spec->upperdatums = lappend(result_spec->upperdatums,
											   rdatum);

			++i;
		}
	}
	else
		elog(ERROR, "unexpected partition strategy: %d", (int) strategy);

	return result_spec;
}


/*
 * If this rel is the table we want to partition hypothetically, we inject
 * the hypothetical partitioning to this rel
 */
void
hypo_injectHypotheticalPartitioning(PlannerInfo *root,
				    Oid relationObjectId,
				    RelOptInfo *rel)
{
	Assert(hypo_table_oid_is_hypothetical(relationObjectId));

	/*
	 * if this rel is parent, prepare some structures to inject
	 * hypothetical partitioning
	 */
	if(!HYPO_RTI_IS_TAGGED(rel->relid,root))
	{
		Relation parentrel;

		parentrel = heap_open(relationObjectId, AccessShareLock);
		hypo_expand_partitioned_entry(root, relationObjectId, rel, parentrel, NULL, root->simple_rel_array_size, -1);
		heap_close(parentrel, NoLock);
	}

	/*
	 * If this rel is partitioned by hash, we should rewrite the rel->pages
	 * and the rel->pages here according to the number of partitions.
	 *
	 * If this rel is partitioned list or range, we add the partition constraints
	 * to the rte->securityQuals so that the rel->rows is computed correctly at
	 * the set_baserel_size_estimates(). We shouldn't rewrite the rel->pages
	 * and the rel->tuples here, because they will be rewritten at the later hook.
	 */
	if (rel->reloptkind != RELOPT_BASEREL
		&&HYPO_RTI_IS_TAGGED(rel->relid,root))
	{
		Oid partoid;
		hypoTable *part;
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
		Selectivity selectivity;
		double pages;

		Assert(rte->values_lists);
		partoid = linitial_oid(rte->values_lists);
		part = hypo_find_table(partoid, false);

		/*
		 * there's no need to estimate branch partitions pages and tuples, but
		 * we have to setup their partitioning data
		 */
		if (part->partkey)
		{
			hypo_set_relation_partition_info(root, rel, part);
			return;
		}

		/*
		 * hypo_clauselist_selectivity will retrieve the constraints for this
		 * partition and all its ancestors
		 */
		selectivity = hypo_clauselist_selectivity(root, rel, NIL,
				part->rootid, part->parentid);

		elog(DEBUG1, "hypopg: selectivity for partition \"%s\": %lf",
				hypo_find_table(linitial_oid(planner_rt_fetch(rel->relid,
							root)->values_lists), false)->tablename,
				selectivity);

		pages = ceil(rel->pages * selectivity);
		rel->pages = (BlockNumber) pages;
		rel->tuples = clamp_row_est(rel->tuples * selectivity);
	}
}


/*
 * Set partitioning scheme and relation information for a hypothetically
 * partitioned table.
 *
 * Heavily inspired on set_relation_partition_info
 */
static void
hypo_set_relation_partition_info(PlannerInfo *root, RelOptInfo *rel,
		hypoTable *entry)
{
	PartitionDesc partdesc;
	PartitionKey partkey;

	Assert(planner_rt_fetch(rel->relid, root)->relkind ==
			RELKIND_PARTITIONED_TABLE);

	partdesc = hypo_generate_partitiondesc(entry);

	if (!partdesc)
		return;

	partkey = entry->partkey;
	rel->part_scheme = hypo_find_partition_scheme(root, partkey);
	Assert(partdesc != NULL && rel->part_scheme != NULL);
	rel->boundinfo = partition_bounds_copy(partdesc->boundinfo, partkey);
	rel->nparts = partdesc->nparts;
	hypo_generate_partition_key_exprs(entry, rel);

	/* Add the partition_qual if it's not the root partition */
	if (OidIsValid(entry->parentid))
	{
		hypoTable *parent = hypo_find_table(entry->parentid, false);
		rel->partition_qual = hypo_get_partition_constraints(root, rel, parent);
	}
}


/*
 * Given a rel corresponding to a hypothetically partitioned table, returns a
 * List of partition constraints for this partition, including all its
 * ancestors if any.  The main processing is done in
 * hypo_get_partition_quals_inh.
 *
 * It is inspired on get_relation_constraints()
 */
List *
hypo_get_partition_constraints(PlannerInfo *root, RelOptInfo *rel, hypoTable *parent)
{
	ListCell *lc;
	Oid childOid;
	hypoTable *child;
	List *constraints;

	/* FIXME maybe use a mapping array here instead of rte->values_lists*/
	lc = list_head(planner_rt_fetch(rel->relid, root)->values_lists);
	childOid = lfirst_oid(lc);
	child = hypo_find_table(childOid, false);

	Assert(child->parentid == parent->oid);

	/* get its partition constraints */
	constraints = hypo_get_partition_quals_inh(child, parent);

	if (constraints)
	{
		/*
		 * Run each expression through const-simplification and
		 * canonicalization similar to check constraints.
		 */
		constraints = (List *) eval_const_expressions(root, (Node *) constraints);
		/* FIXME this func will be modified at pg11 */
		// constraints = (List *) canonicalize_qual((Expr *) constraints, true);

		/* Fix Vars to have the desired varno */
		if (rel->relid != 1)
			ChangeVarNodes((Node *) constraints, 1, rel->relid, 0);
	}

	return constraints;
}

/*
 * Return the partition constraints belonging to the given partition and all
 * of its ancestor.  The parent parameter is optional.
 */
List *
hypo_get_partition_quals_inh(hypoTable *part, hypoTable *parent)
{
	PartitionBoundSpec *spec;
	List *constraints = NIL;

	Assert(OidIsValid(part->parentid));

	if (parent == NULL)
		parent = hypo_find_table(part->parentid, false);

	spec = part->boundspec;

	constraints = hypo_get_qual_from_partbound(parent, spec);

	/* Append parent's constraint if any */
	if (OidIsValid(parent->parentid))
	{
		List *parent_constraints;

		parent_constraints = hypo_get_partition_quals_inh(parent, NULL);

		constraints = list_concat(parent_constraints, constraints);
	}

	return constraints;
}



/*
 * Return the list of executable expressions as partition constraint
 *
 * Heavily inspired on get_qual_from_partbound
 */
static List *
hypo_get_qual_from_partbound(hypoTable *parent, PartitionBoundSpec *spec)
{
	PartitionKey key = parent->partkey;
	List *my_qual = NIL;

	Assert(key != NULL);

	switch (key->strategy)
	{

	case PARTITION_STRATEGY_HASH:
		Assert(spec->strategy == PARTITION_STRATEGY_HASH);
		my_qual = hypo_get_qual_for_hash(parent, spec);
		break;

	case PARTITION_STRATEGY_LIST:
		Assert(spec->strategy == PARTITION_STRATEGY_LIST);
		my_qual = hypo_get_qual_for_list(parent, spec);
		break;

	case PARTITION_STRATEGY_RANGE:
		Assert(spec->strategy == PARTITION_STRATEGY_RANGE);
		my_qual = hypo_get_qual_for_range(parent, spec, false);
		break;

	default:
		elog(ERROR, "unexpected partition strategy: %d",
			 (int) key->strategy);
	}

	return my_qual;
}


/*
 * Returns a CHECK constraint expression to use as a hash partition's
 * constraint, given the parent entry and partition bound structure.
 *
 * Heavily inspired on get_qual_for_hash
 */
static List *
hypo_get_qual_for_hash(hypoTable *parent, PartitionBoundSpec *spec)
{
	PartitionKey key = parent->partkey;
	FuncExpr   *fexpr;
	Node	   *relidConst;
	Node	   *modulusConst;
	Node	   *remainderConst;
	List	   *args;
	ListCell   *partexprs_item;
	int			i;

	/* Fixed arguments. */
	relidConst = (Node *) makeConst(OIDOID,
									-1,
									InvalidOid,
									sizeof(Oid),
									ObjectIdGetDatum(parent->oid), //parentid?
									false,
									true);

	modulusConst = (Node *) makeConst(INT4OID,
									  -1,
									  InvalidOid,
									  sizeof(int32),
									  Int32GetDatum(spec->modulus),
									  false,
									  true);

	remainderConst = (Node *) makeConst(INT4OID,
										-1,
										InvalidOid,
										sizeof(int32),
										Int32GetDatum(spec->remainder),
										false,
										true);

	args = list_make3(relidConst, modulusConst, remainderConst);
	partexprs_item = list_head(key->partexprs);

	/* Add an argument for each key column. */
	for (i = 0; i < key->partnatts; i++)
	{
		Node	   *keyCol;

		/* Left operand */
		if (key->partattrs[i] != 0)
		{
			keyCol = (Node *) makeVar(1,
									  key->partattrs[i],
									  key->parttypid[i],
									  key->parttypmod[i],
									  key->parttypcoll[i],
									  0);
		}
		else
		{
			keyCol = (Node *) copyObject(lfirst(partexprs_item));
			partexprs_item = lnext(partexprs_item);
		}

		args = lappend(args, keyCol);
	}

	fexpr = makeFuncExpr(F_SATISFIES_HASH_PARTITION,
						 BOOLOID,
						 args,
						 InvalidOid,
						 InvalidOid,
						 COERCE_EXPLICIT_CALL);

	return list_make1(fexpr);
}



/*
 * Returns an implicit-AND list of expressions to use as a list partition's
 * constraint, given the parent entry and partition bound structure.
 *
 * Heavily inspired on get_qual_for_list
 */
static List *
hypo_get_qual_for_list(hypoTable *parent, PartitionBoundSpec *spec)
{
	PartitionKey key = parent->partkey;
	List	   *result;
	Expr	   *keyCol;
	Expr	   *opexpr;
	NullTest   *nulltest;
	ListCell   *cell;
	List	   *elems = NIL;
	bool		list_has_null = false;

	/*
	 * Only single-column list partitioning is supported, so we are worried
	 * only about the partition key with index 0.
	 */
	Assert(key->partnatts == 1);

	/* Construct Var or expression representing the partition column */
	if (key->partattrs[0] != 0)
		keyCol = (Expr *) makeVar(1,
								  key->partattrs[0],
								  key->parttypid[0],
								  key->parttypmod[0],
								  key->parttypcoll[0],
								  0);
	else
		keyCol = (Expr *) copyObject(linitial(key->partexprs));

	/*
	 * For default list partition, collect datums for all the partitions. The
	 * default partition constraint should check that the partition key is
	 * equal to none of those.
	 */
	if (spec->is_default)
	{
		int			i;
		int			ndatums = 0;
		PartitionDesc pdesc = hypo_generate_partitiondesc(parent);
		PartitionBoundInfo boundinfo = pdesc->boundinfo;

		if (boundinfo)
		{
			ndatums = boundinfo->ndatums;

			if (partition_bound_accepts_nulls(boundinfo))
				list_has_null = true;
		}

		/*
		 * If default is the only partition, there need not be any partition
		 * constraint on it.
		 */
		if (ndatums == 0 && !list_has_null)
			return NIL;

		for (i = 0; i < ndatums; i++)
		{
			Const	   *val;

			/*
			 * Construct Const from known-not-null datum.  We must be careful
			 * to copy the value, because our result has to be able to outlive
			 * the relcache entry we're copying from.
			 */
			val = makeConst(key->parttypid[0],
							key->parttypmod[0],
							key->parttypcoll[0],
							key->parttyplen[0],
							datumCopy(*boundinfo->datums[i],
									  key->parttypbyval[0],
									  key->parttyplen[0]),
							false,	/* isnull */
							key->parttypbyval[0]);

			elems = lappend(elems, val);
		}
	}
	else
	{
		/*
		 * Create list of Consts for the allowed values, excluding any nulls.
		 */
		foreach(cell, spec->listdatums)
		{
			Const	   *val = castNode(Const, lfirst(cell));

			if (val->constisnull)
				list_has_null = true;
			else
				elems = lappend(elems, copyObject(val));
		}
	}

	if (elems)
	{
		/*
		 * Generate the operator expression from the non-null partition
		 * values.
		 */
		opexpr = make_partition_op_expr(key, 0, BTEqualStrategyNumber,
										keyCol, (Expr *) elems);
	}
	else
	{
		/*
		 * If there are no partition values, we don't need an operator
		 * expression.
		 */
		opexpr = NULL;
	}

	if (!list_has_null)
	{
		/*
		 * Gin up a "col IS NOT NULL" test that will be AND'd with the main
		 * expression.  This might seem redundant, but the partition routing
		 * machinery needs it.
		 */
		nulltest = makeNode(NullTest);
		nulltest->arg = keyCol;
		nulltest->nulltesttype = IS_NOT_NULL;
		nulltest->argisrow = false;
		nulltest->location = -1;

		result = opexpr ? list_make2(nulltest, opexpr) : list_make1(nulltest);
	}
	else
	{
		/*
		 * Gin up a "col IS NULL" test that will be OR'd with the main
		 * expression.
		 */
		nulltest = makeNode(NullTest);
		nulltest->arg = keyCol;
		nulltest->nulltesttype = IS_NULL;
		nulltest->argisrow = false;
		nulltest->location = -1;

		if (opexpr)
		{
			Expr	   *or;

			or = makeBoolExpr(OR_EXPR, list_make2(nulltest, opexpr), -1);
			result = list_make1(or);
		}
		else
			result = list_make1(nulltest);
	}

	/*
	 * Note that, in general, applying NOT to a constraint expression doesn't
	 * necessarily invert the set of rows it accepts, because NOT (NULL) is
	 * NULL.  However, the partition constraints we construct here never
	 * evaluate to NULL, so applying NOT works as intended.
	 */
	if (spec->is_default)
	{
		result = list_make1(make_ands_explicit(result));
		result = list_make1(makeBoolExpr(NOT_EXPR, result, -1));
	}

	return result;
}


/*
 * Returns an implicit-AND list of expressions to use as a range partition's
 * constraint, given the parent entry and partition bound structure.
 *
 * Heavily inspired on get_qual_for_range
 */
static List *
hypo_get_qual_for_range(hypoTable *parent, PartitionBoundSpec *spec, bool for_default)
{

	List	   *result = NIL;
	ListCell   *cell1,
			   *cell2,
			   *partexprs_item,
			   *partexprs_item_saved;
	int			i,
				j;
	PartitionRangeDatum *ldatum,
			   *udatum;
	PartitionKey key = parent->partkey;
	Expr	   *keyCol;
	Const	   *lower_val,
			   *upper_val;
	List	   *lower_or_arms,
			   *upper_or_arms;
	int			num_or_arms,
				current_or_arm;
	ListCell   *lower_or_start_datum,
			   *upper_or_start_datum;
	bool		need_next_lower_arm,
				need_next_upper_arm;

	if (spec->is_default)
	{
		List	   *or_expr_args = NIL;
		PartitionDesc pdesc = hypo_generate_partitiondesc(parent);
		Oid		   *inhoids = pdesc->oids;
		int			nparts = pdesc->nparts,
					i;

		for (i = 0; i < nparts; i++)
		{
			Oid			inhrelid = inhoids[i];  //is this a parent oid or dummy child oid?
			HeapTuple	tuple;
			Datum		datum;
			bool		isnull;
			PartitionBoundSpec *bspec;

			elog(NOTICE,"inhrelid : %u",inhrelid);

			tuple = SearchSysCache1(RELOID, inhrelid);
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for relation %u", inhrelid);

			datum = SysCacheGetAttr(RELOID, tuple,
									Anum_pg_class_relpartbound,
									&isnull);

			Assert(!isnull);
			bspec = (PartitionBoundSpec *)
				stringToNode(TextDatumGetCString(datum));
			if (!IsA(bspec, PartitionBoundSpec))
				elog(ERROR, "expected PartitionBoundSpec");

			if (!bspec->is_default)
			{
				List	   *part_qual;

				part_qual = hypo_get_qual_for_range(parent, bspec, true);

				/*
				 * AND the constraints of the partition and add to
				 * or_expr_args
				 */
				or_expr_args = lappend(or_expr_args, list_length(part_qual) > 1
									   ? makeBoolExpr(AND_EXPR, part_qual, -1)
									   : linitial(part_qual));
			}
			ReleaseSysCache(tuple);
		}

		if (or_expr_args != NIL)
		{

			Expr	   *other_parts_constr;

			/*
			 * Combine the constraints obtained for non-default partitions
			 * using OR.  As requested, each of the OR's args doesn't include
			 * the NOT NULL test for partition keys (which is to avoid its
			 * useless repetition).  Add the same now.
			 */
			other_parts_constr =
				makeBoolExpr(AND_EXPR,
							 lappend(get_range_nulltest(key),
									 list_length(or_expr_args) > 1
									 ? makeBoolExpr(OR_EXPR, or_expr_args,
													-1)
									 : linitial(or_expr_args)),
							 -1);

			/*
			 * Finally, the default partition contains everything *NOT*
			 * contained in the non-default partitions.
			 */
			result = list_make1(makeBoolExpr(NOT_EXPR,
											 list_make1(other_parts_constr), -1));
		}

		return result;
	}

	lower_or_start_datum = list_head(spec->lowerdatums);
	upper_or_start_datum = list_head(spec->upperdatums);
	num_or_arms = key->partnatts;

	/*
	 * If it is the recursive call for default, we skip the get_range_nulltest
	 * to avoid accumulating the NullTest on the same keys for each partition.
	 */
	if (!for_default)
		result = get_range_nulltest(key);

	/*
	 * Iterate over the key columns and check if the corresponding lower and
	 * upper datums are equal using the btree equality operator for the
	 * column's type.  If equal, we emit single keyCol = common_value
	 * expression.  Starting from the first column for which the corresponding
	 * lower and upper bound datums are not equal, we generate OR expressions
	 * as shown in the function's header comment.
	 */
	i = 0;
	partexprs_item = list_head(key->partexprs);
	partexprs_item_saved = partexprs_item;	/* placate compiler */
	forboth(cell1, spec->lowerdatums, cell2, spec->upperdatums)
	{
		EState	   *estate;
		MemoryContext oldcxt;
		Expr	   *test_expr;
		ExprState  *test_exprstate;
		Datum		test_result;
		bool		isNull;

		ldatum = castNode(PartitionRangeDatum, lfirst(cell1));
		udatum = castNode(PartitionRangeDatum, lfirst(cell2));

		/*
		 * Since get_range_key_properties() modifies partexprs_item, and we
		 * might need to start over from the previous expression in the later
		 * part of this function, save away the current value.
		 */
		partexprs_item_saved = partexprs_item;

		get_range_key_properties(key, i, ldatum, udatum,
								 &partexprs_item,
								 &keyCol,
								 &lower_val, &upper_val);

		/*
		 * If either value is NULL, the corresponding partition bound is
		 * either MINVALUE or MAXVALUE, and we treat them as unequal, because
		 * even if they're the same, there is no common value to equate the
		 * key column with.
		 */
		if (!lower_val || !upper_val)
			break;

		/* Create the test expression */
		estate = CreateExecutorState();
		oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
		test_expr = make_partition_op_expr(key, i, BTEqualStrategyNumber,
										   (Expr *) lower_val,
										   (Expr *) upper_val);
		fix_opfuncids((Node *) test_expr);
		test_exprstate = ExecInitExpr(test_expr, NULL);
		test_result = ExecEvalExprSwitchContext(test_exprstate,
												GetPerTupleExprContext(estate),
												&isNull);
		MemoryContextSwitchTo(oldcxt);
		FreeExecutorState(estate);

		/* If not equal, go generate the OR expressions */
		if (!DatumGetBool(test_result))
			break;

		/*
		 * The bounds for the last key column can't be equal, because such a
		 * range partition would never be allowed to be defined (it would have
		 * an empty range otherwise).
		 */
		if (i == key->partnatts - 1)
			elog(ERROR, "invalid range bound specification");

		/* Equal, so generate keyCol = lower_val expression */
		result = lappend(result,
						 make_partition_op_expr(key, i, BTEqualStrategyNumber,
												keyCol, (Expr *) lower_val));

		i++;
	}

	/* First pair of lower_val and upper_val that are not equal. */
	lower_or_start_datum = cell1;
	upper_or_start_datum = cell2;

	/* OR will have as many arms as there are key columns left. */
	num_or_arms = key->partnatts - i;
	current_or_arm = 0;
	lower_or_arms = upper_or_arms = NIL;
	need_next_lower_arm = need_next_upper_arm = true;
	while (current_or_arm < num_or_arms)
	{
		List	   *lower_or_arm_args = NIL,
				   *upper_or_arm_args = NIL;

		/* Restart scan of columns from the i'th one */
		j = i;
		partexprs_item = partexprs_item_saved;

		for_both_cell(cell1, lower_or_start_datum, cell2, upper_or_start_datum)
		{
			PartitionRangeDatum *ldatum_next = NULL,
					   *udatum_next = NULL;

			ldatum = castNode(PartitionRangeDatum, lfirst(cell1));
			if (lnext(cell1))
				ldatum_next = castNode(PartitionRangeDatum,
									   lfirst(lnext(cell1)));
			udatum = castNode(PartitionRangeDatum, lfirst(cell2));
			if (lnext(cell2))
				udatum_next = castNode(PartitionRangeDatum,
									   lfirst(lnext(cell2)));
			get_range_key_properties(key, j, ldatum, udatum,
									 &partexprs_item,
									 &keyCol,
									 &lower_val, &upper_val);

			if (need_next_lower_arm && lower_val)
			{
				uint16		strategy;

				/*
				 * For the non-last columns of this arm, use the EQ operator.
				 * For the last column of this arm, use GT, unless this is the
				 * last column of the whole bound check, or the next bound
				 * datum is MINVALUE, in which case use GE.
				 */
				if (j - i < current_or_arm)
					strategy = BTEqualStrategyNumber;
				else if (j == key->partnatts - 1 ||
						 (ldatum_next &&
						  ldatum_next->kind == PARTITION_RANGE_DATUM_MINVALUE))
					strategy = BTGreaterEqualStrategyNumber;
				else
					strategy = BTGreaterStrategyNumber;

				lower_or_arm_args = lappend(lower_or_arm_args,
											make_partition_op_expr(key, j,
																   strategy,
																   keyCol,
																   (Expr *) lower_val));
			}

			if (need_next_upper_arm && upper_val)
			{
				uint16		strategy;

				/*
				 * For the non-last columns of this arm, use the EQ operator.
				 * For the last column of this arm, use LT, unless the next
				 * bound datum is MAXVALUE, in which case use LE.
				 */
				if (j - i < current_or_arm)
					strategy = BTEqualStrategyNumber;
				else if (udatum_next &&
						 udatum_next->kind == PARTITION_RANGE_DATUM_MAXVALUE)
					strategy = BTLessEqualStrategyNumber;
				else
					strategy = BTLessStrategyNumber;

				upper_or_arm_args = lappend(upper_or_arm_args,
											make_partition_op_expr(key, j,
																   strategy,
																   keyCol,
																   (Expr *) upper_val));
			}

			/*
			 * Did we generate enough of OR's arguments?  First arm considers
			 * the first of the remaining columns, second arm considers first
			 * two of the remaining columns, and so on.
			 */
			++j;
			if (j - i > current_or_arm)
			{
				/*
				 * We must not emit any more arms if the new column that will
				 * be considered is unbounded, or this one was.
				 */
				if (!lower_val || !ldatum_next ||
					ldatum_next->kind != PARTITION_RANGE_DATUM_VALUE)
					need_next_lower_arm = false;
				if (!upper_val || !udatum_next ||
					udatum_next->kind != PARTITION_RANGE_DATUM_VALUE)
					need_next_upper_arm = false;
				break;
			}
		}

		if (lower_or_arm_args != NIL)
			lower_or_arms = lappend(lower_or_arms,
									list_length(lower_or_arm_args) > 1
									? makeBoolExpr(AND_EXPR, lower_or_arm_args, -1)
									: linitial(lower_or_arm_args));

		if (upper_or_arm_args != NIL)
			upper_or_arms = lappend(upper_or_arms,
									list_length(upper_or_arm_args) > 1
									? makeBoolExpr(AND_EXPR, upper_or_arm_args, -1)
									: linitial(upper_or_arm_args));

		/* If no work to do in the next iteration, break away. */
		if (!need_next_lower_arm && !need_next_upper_arm)
			break;

		++current_or_arm;
	}

	/*
	 * Generate the OR expressions for each of lower and upper bounds (if
	 * required), and append to the list of implicitly ANDed list of
	 * expressions.
	 */
	if (lower_or_arms != NIL)
		result = lappend(result,
						 list_length(lower_or_arms) > 1
						 ? makeBoolExpr(OR_EXPR, lower_or_arms, -1)
						 : linitial(lower_or_arms));
	if (upper_or_arms != NIL)
		result = lappend(result,
						 list_length(upper_or_arms) > 1
						 ? makeBoolExpr(OR_EXPR, upper_or_arms, -1)
						 : linitial(upper_or_arms));

	/*
	 * As noted above, for non-default, we return list with constant TRUE. If
	 * the result is NIL during the recursive call for default, it implies
	 * this is the only other partition which can hold every value of the key
	 * except NULL. Hence we return the NullTest result skipped earlier.
	 */
	if (result == NIL)
		result = for_default
			? get_range_nulltest(key)
			: list_make1(makeBoolConst(true, false));

	return result;
}
#endif

/*
 * SQL wrapper to create an hypothetical partition with his parsetree
 */
Datum
hypopg_add_partition(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	const char *partname = PG_GETARG_NAME(0)->data;
	char	   *partitionof = TextDatumGetCString(PG_GETARG_TEXT_PP(1));
	char	   *partition_by = NULL;
	StringInfoData	sql;
	Oid			parentid, rootid;
	const hypoTable  *entry;
	List	   *parsetree_list;
	RawStmt	   *raw_stmt;
	CreateStmt *stmt;
	RangeVar   *rv;
	TupleDesc	tupdesc;
	Datum		values[HYPO_ADD_PART_COLS];
	bool		nulls[HYPO_ADD_PART_COLS];
	int			i = 0;

	if (!PG_ARGISNULL(2))
		partition_by = TextDatumGetCString(PG_GETARG_TEXT_PP(2));

	if (hypo_table_name_get_entry(partname) != NULL)
		elog(ERROR, "hypopg: hypothetical table %s already exists",
				quote_identifier(partname));

	if (RelnameGetRelid(partname) != InvalidOid)
		elog(ERROR, "hypopg: real table %s already exists",
				quote_identifier(partname));

	tupdesc = CreateTemplateTupleDesc(HYPO_ADD_PART_COLS, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "relid", OIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++i, "tablename", TEXTOID, -1, 0);
	Assert(i == HYPO_ADD_PART_COLS);
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));
	i = 0;

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE TABLE %s %s",
			quote_identifier(partname), partitionof);

	if (partition_by)
		appendStringInfo(&sql, " %s",
			partition_by);

	parsetree_list = pg_parse_query(sql.data);
	raw_stmt = (RawStmt *) linitial(parsetree_list);
	stmt = (CreateStmt *) raw_stmt->stmt;
	Assert(IsA(stmt, CreateStmt));

	if (!stmt->partbound || !stmt->inhRelations)
		elog(ERROR, "hypopg: you must specify a PARTITION OF clause");

	/* Find the parent's oid */
	if (list_length(stmt->inhRelations) != 1)
		elog(ERROR, "hypopg: unexpected list length %d, expected 1",
				list_length(stmt->inhRelations));

	rv = (RangeVar *) linitial(stmt->inhRelations);
	parentid = RangeVarGetRelid(rv, AccessShareLock, true);

	if (OidIsValid(parentid) && !hypo_table_oid_is_hypothetical(parentid))
		elog(ERROR, "hypopg: %s must be hypothetically partitioned first",
				quote_identifier(rv->relname));

	/* Look for a hypothetical parent if we didn't find a real table */
	if (!OidIsValid(parentid))
	{
		hypoTable *parent;

		parent = hypo_table_name_get_entry(rv->relname);

		if (parent == NULL)
			elog(ERROR, "hypopg: %s does not exists",
					quote_identifier(rv->relname));

		if(rv->schemaname)
			elog(ERROR, "hypopg: cannot use qualified name with hypothetical"
					" partition");

		parentid = parent->oid;
		rootid = parent->rootid;
	}
	else
	{
		/*
		 * if we found a real table, there's no subpartitioning, so the root
		 * and the parent are the same
		 */
		rootid = parentid;
	}

	entry = hypo_table_store_parsetree((CreateStmt *) stmt, sql.data,
			parentid, rootid);

	pfree(sql.data);

	values[i++] = ObjectIdGetDatum(entry->oid);
	values[i++] = CStringGetTextDatum(entry->tablename);
	Assert(i == HYPO_ADD_PART_COLS);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
#endif
}

/*
 * SQL wrapper to drop an hypothetical partition, or unpartition a previsously
 * hypothatically partitioned existing table.
 */
Datum
hypopg_drop_table(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	Oid			tableid = PG_GETARG_OID(0);

	PG_RETURN_BOOL(hypo_table_remove(tableid));
#endif
}

/*
 * SQL wrapper to hypothetically partition an existing table.
 */
Datum
hypopg_partition_table(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	Oid				tableid = PG_GETARG_OID(0);
	char		   *partition_by = TextDatumGetCString(PG_GETARG_TEXT_PP(1));
	const hypoTable *entry;
	char		   *root_name = NULL;
	char		   *nspname = NULL;
	bool			found = false;
	HeapTuple		tp;
	Relation		relation;
	List		   *children = NIL;
	List		   *parsetree_list;
	ListCell	   *lc;
	StringInfoData	sql;
	RawStmt		   *raw_stmt;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(tableid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

		if (reltup->relkind == RELKIND_RELATION)
			found = true;

		root_name = pstrdup(NameStr(reltup->relname));
		nspname = get_namespace_name(reltup->relnamespace);

		ReleaseSysCache(tp);
	}
	else
		elog(ERROR, "hypopg: Object %d does not exists", tableid);

	if (!found)
		elog(ERROR, "hypopg: %s.%s is not a simple table",
				quote_identifier(nspname), quote_identifier(root_name));


	relation = heap_open(tableid, AccessShareLock);
	children = find_inheritance_children(tableid, AccessShareLock);
	heap_close(relation, AccessShareLock);

	if (children != NIL)
		elog(ERROR, "hypopg: Table %s.%s has inherited tables",
				quote_identifier(nspname), quote_identifier(root_name));

	foreach(lc, hypoTables)
	{
		hypoTable *search = (hypoTable *) lfirst(lc);

		if (search->oid == tableid)
			elog(ERROR, "hypopg: Table %s.%s is already hypothetically partitioned",
					quote_identifier(nspname), quote_identifier(root_name));
	}

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE TABLE hypoTable (LIKE %s.%s) %s",
			quote_identifier(nspname), quote_identifier(root_name), partition_by);

	parsetree_list = pg_parse_query(sql.data);
	raw_stmt = (RawStmt *) linitial(parsetree_list);
	Assert(IsA(raw_stmt->stmt, CreateStmt));

	entry = hypo_table_store_parsetree((CreateStmt *) raw_stmt->stmt, sql.data,
			tableid, tableid);

	/* special case for root table, copy it's original name */
	strncpy(entry->tablename, root_name, NAMEDATALEN);

	pfree(sql.data);
	pfree(root_name);
	pfree(nspname);

	PG_RETURN_BOOL(entry != NULL);
#endif
}

/*
 * SQL wrapper to remove all declared hypothetical partitions.
 */
Datum
hypopg_reset_table(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	hypo_table_reset();
	PG_RETURN_VOID();
#endif
}

/*
 * List created hypothetical tables
 */
Datum
hypopg_table(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	ListCell   *lc;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	foreach(lc, hypoTables)
	{
		hypoTable  *entry = (hypoTable *) lfirst(lc);
		Datum		values[HYPO_TABLE_NB_COLS];
		bool		nulls[HYPO_TABLE_NB_COLS];
		int			i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = ObjectIdGetDatum(entry->oid);
		values[i++] = CStringGetTextDatum(entry->tablename);
		if (entry->parentid == InvalidOid)
			nulls[i++] = true;
		else
			values[i++] = ObjectIdGetDatum(entry->parentid);

		values[i++] = ObjectIdGetDatum(entry->rootid);

		if (entry->partkey)
			values[i++] = CStringGetTextDatum(hypo_get_partkeydef(entry));
		else
			nulls[i++] = true;

		if (entry->boundspec)
			values[i++] = CStringGetTextDatum(hypo_get_partbounddef(entry));
		else
			nulls[i++] = true;

		Assert(i == HYPO_TABLE_NB_COLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
#endif
}
