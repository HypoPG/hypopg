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
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#if PG_VERSION_NUM < 110000
#include "catalog/pg_inherits_fn.h"
#endif
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
#if PG_VERSION_NUM >= 110000
#include "utils/partcache.h"
#include "partitioning/partbounds.h"
#endif
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#endif		/* pg10+ */

#include "include/hypopg.h"
#include "include/hypopg_analyze.h"
#include "include/hypopg_index.h"
#include "include/hypopg_table.h"

/*--- Variables exported ---*/

HTAB	   *hypoTables;

/*--- Functions --- */

PG_FUNCTION_INFO_V1(hypopg_add_partition);
PG_FUNCTION_INFO_V1(hypopg_drop_table);
PG_FUNCTION_INFO_V1(hypopg_partition_table);
PG_FUNCTION_INFO_V1(hypopg_reset_table);
PG_FUNCTION_INFO_V1(hypopg_table);


#if PG_VERSION_NUM >= 100000		/* closed just before the SQL wrapper */
static void hypo_initTablesHash();
static int hypo_expand_partitioned_entry(PlannerInfo *root, Oid
		relationObjectId, RelOptInfo *rel, Relation parentrel,
										 hypoTable *branch, int firstpos, int parent_rti
#if PG_VERSION_NUM < 110000
										 ,List **partitioned_child_rels
#endif
	);
static void hypo_expand_single_inheritance_child(PlannerInfo *root,
		Oid relationObjectId, RelOptInfo *rel, Relation parentrel,
		hypoTable *branch, RangeTblEntry *rte, hypoTable *child, Oid newrelid,
		int parent_rti, bool expandBranch);
static List *hypo_find_inheritance_children(hypoTable *parent);
#if PG_VERSION_NUM < 110000
static List *hypo_find_all_inheritors(Oid parentrelId, List **numparents);
#endif
static List *hypo_get_qual_from_partbound(hypoTable *parent,
		PartitionBoundSpec *spec);
static PartitionDesc hypo_generate_partitiondesc(hypoTable *parent);
static void hypo_generate_partkey(CreateStmt *stmt, Oid parentid,
		hypoTable *entry);
#if PG_VERSION_NUM >= 110000
static PartitionScheme hypo_find_partition_scheme(PlannerInfo *root,
												  PartitionKey partkey);
static void hypo_generate_partition_key_exprs(hypoTable *entry,
		RelOptInfo *rel);
#endif
static PartitionBoundSpec *hypo_get_boundspec(Oid tableid);
#if PG_VERSION_NUM >= 110000
static Oid hypo_get_default_partition_oid(hypoTable *parent);
#endif
static char *hypo_get_partbounddef(hypoTable *entry);
static char *hypo_get_partkeydef(hypoTable *entry);
static hypoTable *hypo_newTable(Oid parentid);
#if PG_VERSION_NUM >= 110000
static void hypo_table_check_constraints_compatibility(hypoTable *table);
#endif
static hypoTable *hypo_table_find_parent_oid(Oid parentid);
static void hypo_table_pfree(hypoTable *entry, bool freeFieldsOnly);
static hypoTable *hypo_table_store_parsetree(CreateStmt *node,
						   const char *queryString, hypoTable *parent,
						   Oid rootid);
static PartitionBoundSpec *hypo_transformPartitionBound(ParseState *pstate,
		hypoTable *parent, PartitionBoundSpec *spec);
#if PG_VERSION_NUM >= 110000
static void hypo_set_relation_partition_info(PlannerInfo *root, RelOptInfo *rel,
				 hypoTable *entry);
#endif
static List *hypo_get_qual_for_list(hypoTable *parent, PartitionBoundSpec *spec);
static List *hypo_get_qual_for_range(hypoTable *parent, PartitionBoundSpec *spec,
									 bool for_default);
static void hypo_check_new_partition_bound(char *relname, hypoTable *parent,
						  PartitionBoundSpec *spec);


/* Setup the hypoTables hash */
static void hypo_initTablesHash()
{
	HASHCTL		info;

	Assert(!hypoTables);
	Assert(CurrentMemoryContext != HypoMemoryContext);

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(hypoTable);
	info.hcxt = HypoMemoryContext;

	hypoTables = hash_create("hypo_tables",
			1024,
			&info,
			HASH_ELEM | HASH_BLOBS | HASH_CONTEXT
			);
}

/*
 * Adaptation of expand_partitioned_rtentry
 */
static int
hypo_expand_partitioned_entry(PlannerInfo *root, Oid relationObjectId,
							  RelOptInfo *rel, Relation parentrel, hypoTable *branch, int firstpos,
							  int parent_rti
#if PG_VERSION_NUM < 110000
							  ,List **partitioned_child_rels
#endif
	)
{
#if PG_VERSION_NUM < 110000
	List *inhOIDs;
	ListCell *l;
#else
	hypoTable *parent;
	PartitionDesc partdesc;
#endif
	int nparts;
	RangeTblEntry *rte;
	int newrelid, oldsize = root->simple_rel_array_size;
	int i, j;
	Oid *partoids;

	Assert(hypo_table_oid_is_hypothetical(relationObjectId));

#if PG_VERSION_NUM >= 110000
	if (!branch)
		parent = hypo_find_table(relationObjectId, false);
	else
		parent = branch;
#endif

#if PG_VERSION_NUM < 110000
	/*
	 * get all of the partition oids including a root table from
	 * hypo_find_all_inheritors, but remove the root table oid from
	 * the list since we expand a root table separately
	 */
	inhOIDs = hypo_find_all_inheritors(relationObjectId, NULL);
	inhOIDs = list_delete_first(inhOIDs);
	nparts = list_length(inhOIDs);
	partoids = (Oid *) palloc(nparts * sizeof(Oid));
	i = 0;
	foreach(l, inhOIDs)
		partoids[i++] = lfirst_oid(l);
#else
	/* get all of the partition oids from PartitionDesc */
	partdesc = hypo_generate_partitiondesc(parent);
	partoids = partdesc->oids;
	nparts = partdesc->nparts;
#endif

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
		rte->inh = (nparts > 0);
		HYPO_TAG_RTI(rel->relid, root);
#if PG_VERSION_NUM < 110000
		*partitioned_child_rels = lappend_int(*partitioned_child_rels,
											  firstpos);
#endif
	}
	else /* branch partition, expand it */
	{
		/* The rte we retrieved has already been updated */
		Assert(rte->relkind == RELKIND_PARTITIONED_TABLE);

		rte = copyObject(rte);
		if(!rte->alias)
			rte->alias = makeNode(Alias);
		rte->alias->aliasname = branch->tablename;
		rte->inh = (nparts > 0);

		hypo_expand_single_inheritance_child(root, relationObjectId, rel,
				parentrel, branch, rte, branch, firstpos, parent_rti, true);

		firstpos++;
	}
	Assert(rte->inh == (nparts > 0));

	/* add the partitioned table itself */
	root->simple_rte_array[firstpos] = rte;

	root->parse->rtable = lappend(root->parse->rtable,
			root->simple_rte_array[firstpos]);

	HYPO_TAG_RTI(firstpos, root);

	/*
	 * if the table has no partition, we need to tell caller than it has to use
	 * the new position
	 */
	if (nparts == 0)
		return firstpos + 1;

	/*
	 * create RangeTblEntries and AppendRelInfos hypothetically
	 * for all hypothetical partitions
	 */
	newrelid = firstpos + 1;
	for (j = 0; j < nparts; j++)
	{
		Oid childOid = partoids[j];
		hypoTable *child;
		int    ancestor;

		/*
		 * if this is a branch partition, firstpos-1 is the ancestor
		 * rti of this rel.  if not, rel->relid is the ancestor.
		 */
		if (branch)
			ancestor = firstpos-1;
		else
			ancestor = rel->relid;

		child = hypo_find_table(childOid, false);
#if PG_VERSION_NUM < 110000
		if (child->partkey)
			*partitioned_child_rels = lappend_int(*partitioned_child_rels,
												  firstpos);
#else
		/* Expand the child if it's partitioned */
		if (child->partkey)
		{
			newrelid = hypo_expand_partitioned_entry(root, relationObjectId,
													 rel, parentrel, child, newrelid, ancestor);
			continue;
		}
#endif
		hypo_expand_single_inheritance_child(root, relationObjectId, rel,
				parentrel, branch, rte, child, newrelid, ancestor, false);

		newrelid++;
	}

#if PG_VERSION_NUM >= 110000
	/* add partition info for root partition */
	if (!branch)
		hypo_set_relation_partition_info(root, rel, parent);
#endif
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

	if (expandBranch)
		HYPO_TABLE_RTE_SET_HYPOOID(childrte, branch->oid); // partitionOID
	else
		HYPO_TABLE_RTE_SET_HYPOOID(childrte, child->oid); // partitionOID

	root->simple_rte_array[newrelid] = childrte;
	HYPO_TAG_RTI(newrelid, root);
	root->parse->rtable = lappend(root->parse->rtable,
								  root->simple_rte_array[newrelid]);
#if PG_VERSION_NUM < 110000
	if (child->partkey)
	{
		childrte->inh = true;
		return;
	}
#endif
	appinfo = makeNode(AppendRelInfo);

#if PG_VERSION_NUM >= 110000
	if (expandBranch || branch)
		appinfo->parent_relid = parent_rti;
	else
#endif
		appinfo->parent_relid = rel->relid;

	appinfo->child_relid = newrelid;
	appinfo->parent_reltype = parentrel->rd_rel->reltype;
	appinfo->child_reltype = parentrel->rd_rel->reltype;
	make_inh_translation_list(parentrel, parentrel, newrelid,
			&appinfo->translated_vars);
	appinfo->parent_reloid = relationObjectId;
	root->append_rel_list = lappend(root->append_rel_list,
			appinfo);
}

/*
 * Adaptation of find_inheritance_children().
 */
static List *
hypo_find_inheritance_children(hypoTable *parent)
{
	List	   *list = NIL;
	ListCell   *lc;
	Oid			inhrelid;
	Oid		   *oidarr;
	int			numoids,
				i;

	Assert(CurrentMemoryContext != HypoMemoryContext);

	if (list_length(parent->children) == 0)
		return NIL;

	oidarr = (Oid *) palloc(list_length(parent->children) * sizeof(Oid));
	numoids = 0;

	foreach(lc, parent->children)
	{
		Oid		childid = lfirst_oid(lc);

		oidarr[numoids++] = childid;
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
	 * Build the result list.
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
 * Returns a list of relation OIDs including the given rel plus
 * all relations that inherit from it, directly or indirectly.
 * Optionally, it also returns the number of parents found for
 * each such relation within the inheritance tree rooted at the
 * given rel.  This is heavily inspired on find_all_inheritors().
 */
#if PG_VERSION_NUM < 110000
static List *
hypo_find_all_inheritors(Oid parentrelId, List **numparents)
{
	/* hash table for O(1) rel_oid -> rel_numparents cell lookup */
	HTAB	   *seen_rels;
	HASHCTL		ctl;
	List	   *rels_list,
			   *rel_numparents;
	ListCell   *l;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SeenRelsEntry);
	ctl.hcxt = CurrentMemoryContext;

	seen_rels = hash_create("find_all_inheritors temporary table",
							32, /* start small and extend */
							&ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * We build a list starting with the given rel and adding all direct and
	 * indirect children.  We can use a single list as both the record of
	 * already-found rels and the agenda of rels yet to be scanned for more
	 * children.  This is a bit tricky but works because the foreach() macro
	 * doesn't fetch the next list element until the bottom of the loop.
	 */
	rels_list = list_make1_oid(parentrelId);
	rel_numparents = list_make1_int(0);

	foreach(l, rels_list)
	{
		Oid			currentrel = lfirst_oid(l);
		List	   *currentchildren;
		ListCell   *lc;
		hypoTable  *parent = hypo_find_table(currentrel, false);

		/* Get the direct children of this rel */
		currentchildren = hypo_find_inheritance_children(parent);

		/*
		 * Add to the queue only those children not already seen. This avoids
		 * making duplicate entries in case of multiple inheritance paths from
		 * the same parent.  (It'll also keep us from getting into an infinite
		 * loop, though theoretically there can't be any cycles in the
		 * inheritance graph anyway.)
		 */
		foreach(lc, currentchildren)
		{
			Oid			child_oid = lfirst_oid(lc);
			bool		found;
			SeenRelsEntry *hash_entry;

			hash_entry = hash_search(seen_rels, &child_oid, HASH_ENTER, &found);
			if (found)
			{
				/* if the rel is already there, bump number-of-parents counter */
				lfirst_int(hash_entry->numparents_cell)++;
			}
			else
			{
				/* if it's not there, add it. expect 1 parent, initially. */
				rels_list = lappend_oid(rels_list, child_oid);
				rel_numparents = lappend_int(rel_numparents, 1);
				hash_entry->numparents_cell = rel_numparents->tail;
			}
		}
	}

	if (numparents)
		*numparents = rel_numparents;
	else
		list_free(rel_numparents);

	hash_destroy(seen_rels);

	return rels_list;
}
#endif


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

#if PG_VERSION_NUM >= 110000
	int			default_index = -1;

	/* Hash partitioning specific */
	PartitionHashBound **hbounds = NULL;
#endif

	/* List partitioning specific */
	PartitionListValue **all_values = NULL;
	int			null_index = -1;

	/* Range partitioning specific */
	PartitionRangeBound **rbounds = NULL;

	Assert(CurrentMemoryContext != HypoMemoryContext);
	Assert(key);

	/* Get partition oids from pg_inherits */
	inhoids = hypo_find_inheritance_children(parent);

	/* Collect bound spec nodes in a list */
	i = 0;
	partoids = NIL;
	foreach(cell, inhoids)
	{
		Oid			inhrelid = lfirst_oid(cell);
		Node	   *boundspec;

		boundspec = (Node *) hypo_get_boundspec(inhrelid);

#if PG_VERSION_NUM >= 110000
		/*
		 * Sanity check: If the PartitionBoundSpec says this is the default
		 * partition, its OID should correspond to whatever's stored in
		 * pg_partitioned_table.partdefid; if not, the catalog is corrupt.
		 */
		if (castNode(PartitionBoundSpec, boundspec)->is_default)
		{
			Oid			partdefid;

			partdefid = hypo_get_default_partition_oid(parent);
			if (partdefid != inhrelid)
				elog(ERROR, "expected partdefid %u, but got %u",
					 inhrelid, partdefid);
		}
#endif

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
#if PG_VERSION_NUM >= 110000
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
		else
#endif
		if (key->strategy == PARTITION_STRATEGY_LIST)
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

#if PG_VERSION_NUM >= 110000
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
#endif

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

#if PG_VERSION_NUM >= 110000
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
#endif

#if PG_VERSION_NUM < 110000
				lower = make_one_range_bound(key, i, spec->lowerdatums,
											 true);
				upper = make_one_range_bound(key, i, spec->upperdatums,
											 false);
#else
				lower = make_one_partition_rbound(key, i, spec->lowerdatums,
											 true);
				upper = make_one_partition_rbound(key, i, spec->upperdatums,
											 false);
#endif
				all_bounds[ndatums++] = lower;
				all_bounds[ndatums++] = upper;
				i++;
			}

			Assert(ndatums == nparts * 2
#if PG_VERSION_NUM >= 110000
					||
				   (default_index != -1 && ndatums == (nparts - 1) * 2)
#endif
				   );

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
#if PG_VERSION_NUM >= 110000
		boundinfo->default_index = -1;
#endif
		boundinfo->ndatums = ndatums;
		boundinfo->null_index = -1;
		boundinfo->datums = (Datum **) palloc0(ndatums * sizeof(Datum *));

		/* Initialize mapping array with invalid values */
		mapping = (int *) palloc(sizeof(int) * nparts);
		for (i = 0; i < nparts; i++)
			mapping[i] = -1;

		switch (key->strategy)
		{
#if PG_VERSION_NUM >= 110000
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
#endif

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

#if PG_VERSION_NUM >= 110000
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
#endif

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

#if PG_VERSION_NUM >= 110000
					/* Assign mapped index for the default partition. */
					if (default_index != -1)
					{
						Assert(default_index >= 0 && mapping[default_index] == -1);
						mapping[default_index] = next_index++;
						boundinfo->default_index = mapping[default_index];
					}
#endif
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

#if PG_VERSION_NUM >= 110000
	/* determine support function number to search for */
	procnum = (key->strategy == PARTITION_STRATEGY_HASH) ?
		HASHEXTENDED_PROC : BTORDER_PROC;
#else
	procnum = BTORDER_PROC;
#endif

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
#if PG_VERSION_NUM >= 110000
							(key->strategy == PARTITION_STRATEGY_HASH) ?
							"hash" : "btree",
#else
							"btree",
#endif
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

#if PG_VERSION_NUM >= 110000
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
#endif		/* pg11+ */

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

#if PG_VERSION_NUM >= 110000

/*
 * Return the Oid of the default partition if any, otherwise return InvalidOid
 */
static Oid
hypo_get_default_partition_oid(hypoTable *parent)
{
	ListCell *lc;

	foreach(lc, parent->children)
	{
		hypoTable  *entry = hypo_find_table(lfirst_oid(lc), false);

		if (entry->boundspec->is_default)
			return entry->oid;
	}

	return InvalidOid;
}
#endif

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

#if PG_VERSION_NUM >= 110000
	if (spec->is_default)
	{
		appendStringInfoString(buf, "DEFAULT");
		return buf->data;
	}
#endif

	switch (spec->strategy)
	{
#if PG_VERSION_NUM >= 110000
		case PARTITION_STRATEGY_HASH:
			Assert(spec->modulus > 0 && spec->remainder >= 0);
			Assert(spec->modulus > spec->remainder);

			appendStringInfoString(buf, "FOR VALUES");
			appendStringInfo(buf, " WITH (modulus %d, remainder %d)",
							 spec->modulus, spec->remainder);
			break;
#endif

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
#if PG_VERSION_NUM >= 110000
		case PARTITION_STRATEGY_HASH:
			appendStringInfo(&buf, "HASH");
			break;
#endif
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

#if PG_VERSION_NUM < 110000
			attname = get_attname(relid, attnum);
#else
			attname = get_attname(relid, attnum, false);
#endif
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

#if PG_VERSION_NUM >= 110000
/*
 * check if the wanted  PARTITION BY clause is compatible with any exiting
 * unique/pk index.
 *
 * Heavily inspired on get_relation_info and DefineIndex
 */
static void
hypo_table_check_constraints_compatibility(hypoTable *table)
{
	Relation		rel = heap_open(table->rootid, AccessShareLock);
	List		   *idxlist = RelationGetIndexList(rel);
	PartitionKey	key = table->partkey;
	ListCell	   *lc;

	/* The partey should have already been generated */
	Assert(key);

	/* adapted from DefineIndex */
	foreach(lc, idxlist)
	{
		Relation		idxRel = index_open(lfirst_oid(lc), AccessShareLock);
		IndexInfo	   *indexInfo = BuildIndexInfo(idxRel);
		Form_pg_index	index;
		int				i;

		index = idxRel->rd_index;

		if (!IndexIsValid(index))
		{
			index_close(idxRel, AccessShareLock);
			continue;
		}

		if (index->indisexclusion)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("exclusion constraints are not supported on hypothetically partitioned tables")));
		}

		if (!index->indisunique)
		{
			index_close(idxRel, AccessShareLock);
			continue;
		}

		/*
		 * A partitioned table can have unique indexes, as long as all the
		 * columns in the partition key appear in the unique key.  A
		 * partition-local index can enforce global uniqueness iff the PK
		 * value completely determines the partition that a row is in.
		 *
		 * Thus, verify that all the columns in the partition key appear in
		 * the unique key definition.
		 */
		for (i = 0; i < key->partnatts; i++)
		{
			bool		found = false;
			int			j;
			/* FIXME detect the real constraint type */
			const char *constraint_type = "unique";

			/*
			 * It may be possible to support UNIQUE constraints when partition
			 * keys are expressions, but is it worth it?  Give up for now.
			 */
			if (key->partattrs[i] == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypopg: unsupported %s constraint with hypothetical partition key definition",
								constraint_type),
						 errdetail("%s constraints cannot be used when hypothetical partition keys include expressions.",
								   constraint_type)));

			for (j = 0; j < indexInfo->ii_NumIndexAttrs; j++)
			{
				if (key->partattrs[i] == indexInfo->ii_IndexAttrNumbers[j])
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				Form_pg_attribute att;

				att = TupleDescAttr(RelationGetDescr(rel), key->partattrs[i] - 1);
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypopg: insufficient columns in %s constraint definition",
								constraint_type),
						 errdetail("%s constraint on table \"%s\" lacks column \"%s\" which is part of the hypothetical partition key.",
								   constraint_type, RelationGetRelationName(rel),
								   NameStr(att->attname))));
			}

		}

		index_close(idxRel, AccessShareLock);
	}

	/* same, but for hypothetical indexes */
	foreach(lc, hypoIndexes)
	{
		hypoIndex  *idx = (hypoIndex *) lfirst(lc);
		int			i;

		if ((idx->relid != table->rootid && idx->relid != table->oid) ||
				!idx->unique)
			continue;

		for (i = 0; i < key->partnatts; i++)
		{
			bool		found = false;
			int			j;
			/* FIXME detect the real constraint type */
			const char *constraint_type = "unique";

			if (key->partattrs[i] == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypopg: unsupported hypothetical %s constraint with hypothetical partition key definition",
								constraint_type),
						 errdetail("hypothetical %s constraints cannot be used when hypothetical partition keys include expressions.",
								   constraint_type)));

			for (j = 0; j < idx->nkeycolumns; j++)
			{
				if (key->partattrs[i] == idx->indexkeys[j])
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				Form_pg_attribute att;

				att = TupleDescAttr(RelationGetDescr(rel), key->partattrs[i] - 1);
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypopg: insufficient columns in %s hypothetical constraint definition",
								constraint_type),
						 errdetail("%s constraint on table \"%s\" lacks column \"%s\" which is part of the hypothetical partition key.",
								   constraint_type, RelationGetRelationName(rel),
								   NameStr(att->attname))));
			}

		}
	}

	heap_close(rel, AccessShareLock);
}
#endif

/*
 * palloc a new hypoTable, and give it a new OID, and some other global stuff.
 */
static hypoTable *
hypo_newTable(Oid parentid)
{
	Oid				entryid;
	hypoTable	   *entry;
	hypoTable	   *parent;
	bool			found;

	if (!hypoTables)
		hypo_initTablesHash();

	/*
	 * If the given root table oid isn't present in hypoTables, we're
	 * partitioning it, so keep its oid, otherwise generate a new oid
	 */
	parent= hypo_table_find_parent_oid(parentid);
	if (parent)
		entryid = hypo_getNewOid(parent->rootid);
	else
		entryid = parentid;

	entry = (hypoTable *) hash_search(hypoTables, &entryid, HASH_ENTER,
			&found);

	Assert(!found);

	memset(entry, 0, sizeof(hypoTable));

	entry->oid = entryid;

	entry->set_tuples = false; /* wil be generated later if needed */
	entry->tuples = 0; /* wil be generated later if needed */
	entry->children = NIL; /* maintained add child creation */
	entry->boundspec = NULL; /* wil be generated later if needed */
	entry->partkey = NULL; /* wil be generated later if needed */
	entry->valid = false; /* set to true when all initialization is done */

	if (parent)
	{
		entry->parentid = parent->oid;
		entry->rootid = parent->rootid;
	}
	else
	{
		entry->parentid = InvalidOid;
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
	return hypo_find_table(parentid, true);
}

/*
 * Return the stored hypothetical table corresponding to the Oid if any,
 * otherwise return NULL
 */
hypoTable *
hypo_find_table(Oid tableid, bool missing_ok)
{
	hypoTable  *entry;
	bool		found = false;

	if (hypoTables)
		entry = hash_search(hypoTables, &tableid, HASH_FIND, &found);

	if (found)
		return entry;

	if (!missing_ok)
		elog(ERROR, "hypopg: could not find entry for oid %d", tableid);

	return NULL;
}

/*
 * Return the hypothetical oid if  the given name is an hypothetical partition,
 * otherwise return InvalidOid
 */
hypoTable *
hypo_table_name_get_entry(const char *name)
{
	HASH_SEQ_STATUS	hash_seq;
	hypoTable	   *entry;

	hash_seq_init(&hash_seq, hypoTables);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (strcmp(entry->tablename, name) == 0)
		{
			hash_seq_term(&hash_seq);
			return entry;
		}
	}

	return NULL;
}

/*
 * Is the given relid an hypothetical partition ?
 */
bool
hypo_table_oid_is_hypothetical(Oid relid)
{
	bool		found;

	if (!hypoTables)
		return false;

	hash_search(hypoTables, &relid, HASH_FIND, &found);

	return found;
}

/* pfree all allocated memory for within an hypoTable and the entry itself. */
static void
hypo_table_pfree(hypoTable *entry, bool freeFieldsOnly)
{
	/* free all memory that has been allocated */
	list_free(entry->children);

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

	/* finally pfree the entry if asked */
	if (!freeFieldsOnly)
		pfree(entry);
}

/* ---------------
 * Remove an hypothetical table (or unpartition a previously hypothetically
 * partitioned table) from the list of hypothetical tables.  pfree (by calling
 * hypo_table_pfree) all memory that has been allocated.  Also free all stored
 * hypothetical statistics belonging to it if any.
 *
 * The deep parameter specifies whether the function should to perform the
 * same cleanup for all entries that depends on the given tableid (so its
 * inherited partitions if any).  All callers should pass true for this
 * parameter, except hypo_table_reset() which will sequentially iterate over
 * all entries.
 *
 * If you change the logic here, think to update the PG_CATCH block in
 * hypo_table_store_parsetree() too.
 */
bool
hypo_table_remove(Oid tableid, hypoTable *parent, bool deep)
{
	hypoTable  *entry;

	if(!hypoTables)
		return false;

	entry = hypo_find_table(tableid, true);

	if (!entry)
		return false;

	/* in deep mode, we need to process the inherited children first */
	if (deep)
	{
		ListCell *lc;

		/*
		 * The children will be removed during this loop, so we can't iterate
		 * using the standard foreach macro
		 */
		lc = list_head(entry->children);
		while (lc != NULL)
		{
			Oid childid = lfirst_oid(lc);

			/* get the next cell right now, before we remove the entry */
			lc = lnext(lc);

			hypo_table_remove(childid, entry, true);
		}

		/*
		 * then remove child reference in parent's list of partitions if it's
		 * not the root partition.  This is only done in deep mode because we
		 * then need to properly maintain the children list.
		 */
		if (OidIsValid(entry->parentid))
		{
			if (!parent)
				parent = hypo_find_table(entry->parentid, false);

			Assert(parent->children);
			Assert(list_member_oid(parent->children, tableid));

			parent->children = list_delete_oid(parent->children, tableid);
		}
	}

	/* free the stored fields and the entry itself */
	hypo_table_pfree(entry, true);
	/* remove the entry from the hash */
	hash_search(hypoTables, &tableid, HASH_REMOVE, NULL);

	return true;
}

/*
 * Remove cleanly all hypothetical tables by calling hypo_table_remove() on
 * each entry. hypo_table_remove() function pfree all allocated memory
 */
void
hypo_table_reset(void)
{
	HASH_SEQ_STATUS	hash_seq;
	hypoTable	   *entry;

	if (!hypoTables)
		return;

	hash_seq_init(&hash_seq, hypoTables);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
		hypo_table_remove(entry->oid, NULL, false);

	/* XXX: remove this if inval support for hypothetical indexes is added */
	hypo_clear_inval();

	return;
}

/*
 * Create an hypothetical partition from its CREATE TABLE parsetree.  This
 * function is where all the hypothetic partition creation is done, except the
 * partition size estimation.
 */
static hypoTable *
hypo_table_store_parsetree(CreateStmt *node, const char *queryString,
		hypoTable *parent, Oid rootid)
{
	/* must be declared "volatile", because used in a PG_CATCH() */
	hypoTable	   *volatile entry;
	List		   *stmts;
	CreateStmt	   *stmt = NULL;
	ListCell	   *lc;
	PartitionBoundSpec *boundspec;
	MemoryContext	oldcontext;

	Assert(CurrentMemoryContext != HypoMemoryContext);

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

	if (!stmt)
		elog(ERROR, "hypopg: wrong invocation of %s",
				(parent ? "hypopg_add_partition" : "hypopg_partition_table"));

	boundspec = stmt->partbound;

	/* now create the hypothetical table entry */
	if (parent)
		entry = hypo_newTable(parent->oid);
	else
		entry = hypo_newTable(rootid);

	PG_TRY();
	{
		strncpy(entry->tablename, node->relation->relname, NAMEDATALEN);

		/* The CreateStmt specified a PARTITION BY clause, store it */
		if (stmt->partspec)
		{
			hypo_generate_partkey(stmt, rootid, entry);
#if PG_VERSION_NUM >= 110000
			/*
			 * Make sure that the partitioning clause is compatible with
			 * existing constraints
			 */
			hypo_table_check_constraints_compatibility(entry);
#endif
		}

		if (boundspec)
		{
			ParseState *pstate;

			Assert(parent);

			pstate = make_parsestate(NULL);
			pstate->p_sourcetext = queryString;

			oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
			entry->boundspec = hypo_transformPartitionBound(pstate,
					parent, boundspec);
			MemoryContextSwitchTo(oldcontext);

			hypo_check_new_partition_bound(node->relation->relname,
					parent,
					entry->boundspec);
		}

		if (parent)
		{
			Assert(!list_member_oid(parent->children, entry->oid));

			oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
			parent->children = lappend_oid(parent->children, entry->oid);
			MemoryContextSwitchTo(oldcontext);
		}

		entry->valid = true;
	}
	PG_CATCH();
	{
		/* ---------------
		 * We may or may not have appended the oid to the parent's children
		 * list, so we do the required cleanup manually instead of calling
		 * hypo_table_remove(), which check for the oid presence in the
		 * children list.
		 * First, remove the children reference if it was present
		 */
		if (parent)
			parent->children = list_delete_oid(parent->children, entry->oid);

		/* then free the entry */
		hypo_table_pfree(entry, true);

		/* and finally remove the entry from the hash */
		hash_search(hypoTables, &entry->oid, HASH_REMOVE, NULL);

		PG_RE_THROW();
	}
	PG_END_TRY();

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

#if PG_VERSION_NUM >= 110000
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
	else
#endif
	if (strategy == PARTITION_STRATEGY_LIST)
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
#if PG_VERSION_NUM < 110000
			colname = get_attname(parent->rootid,
								  key->partattrs[0]);
#else
			colname = get_attname(parent->rootid,
								  key->partattrs[0], false);
#endif
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
#if PG_VERSION_NUM < 110000
				colname = get_attname(parent->rootid,
									  key->partattrs[i]);
#else
				colname = get_attname(parent->rootid,
									  key->partattrs[i], false);
#endif
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
#if PG_VERSION_NUM < 110000
		List     *partitioned_child_rels = NIL;
		PartitionedChildRelInfo *pcinfo;
#endif

		parentrel = heap_open(relationObjectId, AccessShareLock);
		hypo_expand_partitioned_entry(root, relationObjectId, rel, parentrel, NULL,
									  root->simple_rel_array_size, -1
#if PG_VERSION_NUM < 110000
									  ,&partitioned_child_rels
#endif
			);

#if PG_VERSION_NUM < 110000
		/* create pcinfo for this relation */
		if (partitioned_child_rels != NIL)
		{
			pcinfo = makeNode(PartitionedChildRelInfo);
			pcinfo->parent_relid = rel->relid;
			pcinfo->child_rels = partitioned_child_rels;
			root->pcinfo_list = lappend(root->pcinfo_list, pcinfo);
		}
#endif
		heap_close(parentrel, NoLock);
	}

	/*
	 * If this rel is a partition, we set rel->pages and rel->tuples.
	 *
	 * When hypoTable->set_tuples is true, pages and tuples are set
	 * according to hypoTable->tuples.  Otherwise, we will estimate pages
	 * and tuples here according to its partition bound and root table's
	 * pages and tuples as follows:
	 * In the case of RANGE/LIST partitioning, we will compute selectivity
	 * according to the partition constraints including its ancestors'.
	 * On the other hand, in the case of HASH partitioning, we will multiply
	 * the number of partitions including its ancestors'.  After that we will
	 * compute pages and tuples using the selectivity and the product of the
	 * number of partitions.
	 */
	if (rel->reloptkind != RELOPT_BASEREL
		&&HYPO_RTI_IS_TAGGED(rel->relid,root))
	{
		Oid partoid;
		hypoTable *part;
#if PG_VERSION_NUM >= 110000
		hypoTable *cur_part;
#endif
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
		Selectivity selectivity;
		double pages;
		int total_modulus = 1;

		Assert(HYPO_TABLE_RTE_HAS_HYPOOID(rte));
		partoid = HYPO_TABLE_RTE_GET_HYPOOID(rte);
		part = hypo_find_table(partoid, false);
#if PG_VERSION_NUM >= 110000
		/*
		 * there's no need to estimate branch partitions pages and tuples, but
		 * we have to setup their partitioning data if the partition has
		 * children
		 */
		if (part->partkey && rte->inh)
		{
			hypo_set_relation_partition_info(root, rel, part);
			return;
		}

		/*
		 * Selectivity for hash partitions cannot be done using the standard
		 * clauselist_selectivity(), because the underlying constraint is using
		 * the satisfies_hash_partition() function, for which we won't be able
		 * to get sensible estimation.  Instead, compute a fraction based on
		 * this partition modulus, multiplied by all its hash partitions'
		 * ancestor modulus if any and use it to correct the estimation we find
		 * for all non-hash partitions
		 */
		cur_part = part;
		for(;;)
		{
			if (cur_part->boundspec->strategy == PARTITION_STRATEGY_HASH)
				total_modulus *= cur_part->boundspec->modulus;

			cur_part = hypo_find_table(cur_part->parentid, false);

			/* exit when we find the root partition */
			if (!OidIsValid(cur_part->parentid))
				break;
		}
		elog(DEBUG1, "hypopg: total modulus for partition %s: %d",
				part->tablename, total_modulus);
#else
		Assert(total_modulus == 1);
#endif

		if (part->set_tuples)
		{
			/*
			 * pages and tuples are set according to part->tuples got by
			 * hypopg_analyze function.  But we need compute them again
			 * using total_modulus for hash partitioning, since hypopg_analyze
			 * cannot run on hash partitioning
			 */
			pages = ceil(rel->pages * part->tuples / rel->tuples / total_modulus);
			rel->pages = (BlockNumber) pages;
			rel->tuples = clamp_row_est(part->tuples / total_modulus);
		}
		else
		{
			/*
			 * hypo_clauselist_selectivity will retrieve the constraints for this
			 * partition and all its ancestors
			 */
			selectivity = hypo_clauselist_selectivity(root, rel, NIL,
													  part->rootid, part->parentid);

			elog(DEBUG1, "hypopg: selectivity for partition \"%s\": %lf",
				 hypo_find_table(HYPO_TABLE_RTE_GET_HYPOOID(rte), false)->tablename,
				 selectivity);

			/* compute pages and tuples using selectivity and total_modulus */
			pages = ceil(rel->pages * selectivity / total_modulus);
			rel->pages = (BlockNumber) pages;
			rel->tuples = clamp_row_est(rel->tuples * selectivity / total_modulus);
		}
	}

#if PG_VERSION_NUM >= 110000
	/*
	 * This is done in query_planner just before add_base_rels_to_query() is
	 * called, so before get_relation_info_hook is called and setup
	 * root->append_rel_list
	 *
	 * FIXME find a way to call it once per planning and not one per
	 * get_relation_info_hook call.
	 */
	if (root->append_rel_array)
	{
		pfree(root->append_rel_array);
		root->append_rel_array = NULL;
	}
	setup_append_rel_array(root);
#endif
}

#if PG_VERSION_NUM < 110000
/*
 * If this rel is need not be scanned, we have to mark it as dummy to omit it
 * from the appendrel
 *
 * It is inspired on relation_excluded_by_constraints
 */
void
hypo_markDummyIfExcluded(PlannerInfo *root, RelOptInfo *rel,
							  Index rti, RangeTblEntry *rte)
{
	List *constraints;
	List *safe_constraints = NIL;
	ListCell *lc;
	Oid  partoid = HYPO_TABLE_RTE_GET_HYPOOID(rte);
	hypoTable *part = hypo_find_table(partoid, false);
	hypoTable *parent = hypo_find_table(part->parentid, false);

	Assert(hypo_table_oid_is_hypothetical(rte->relid));
	Assert(rte->relkind == 'r');

	/* get its partition constraints */
	constraints = hypo_get_partition_constraints(root, rel, parent, false);

	/*
	 * We do not currently enforce that CHECK constraints contain only
	 * immutable functions, so it's necessary to check here. We daren't draw
	 * conclusions from plan-time evaluation of non-immutable functions. Since
	 * they're ANDed, we can just ignore any mutable constraints in the list,
	 * and reason about the rest.
	 */
	foreach(lc, constraints)
	{
		Node       *pred = (Node *) lfirst(lc);

		if (!contain_mutable_functions(pred))
			safe_constraints = lappend(safe_constraints, pred);
	}

	/*
	 * if this partition need not be scanned, we call the set_dummy_rel_pathlist()
	 * to mark it as dummy
	 */
	if (predicate_refuted_by(safe_constraints, rel->baserestrictinfo, false))
		set_dummy_rel_pathlist(rel);

	/*
    TODO: re-estimate parent size just like set_append_rel_size()
	*/


}
#endif

#if PG_VERSION_NUM >= 110000
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
		rel->partition_qual = hypo_get_partition_constraints(root, rel, parent,
				false);
	}
}
#endif

/*
 * Given a rel corresponding to a hypothetically partitioned table, returns a
 * List of partition constraints for this partition, including all its
 * ancestors if any.  The main processing is done in
 * hypo_get_partition_quals_inh.
 *
 * The force_generation is used to make sure that hypo_clauselist_selectivity()
 * will get all the constraints, since it's required for a proper selectivity
 * estimation.
 *
 * It is inspired on get_relation_constraints()
 */
List *
hypo_get_partition_constraints(PlannerInfo *root, RelOptInfo *rel,
		hypoTable *parent, bool force_generation)
{
	Index varno = rel->relid;
	Oid childOid;
	hypoTable *child;
	List *pcqual;

	childOid = HYPO_TABLE_RTI_GET_HYPOOID(rel->relid, root);
	child = hypo_find_table(childOid, false);

	Assert(child->parentid == parent->oid);

	/* get its partition constraints */
	pcqual = hypo_get_partition_quals_inh(child, parent);

#if PG_VERSION_NUM >= 110000
	Assert(
			(force_generation &&
			 list_length(root->parse->rtable) == 1 &&
			 root->parse->commandType == CMD_UNKNOWN &&
			 hypo_table_oid_is_hypothetical(root->simple_rte_array[1]->relid))
			|| !force_generation
	);

	/*
	 * For selects, partition pruning uses the parent table's partition bound
	 * descriptor, instead of constraint exclusion which is driven by the
	 * individual partition's partition constraint.
	 */
	if ((enable_partition_pruning && root->parse->commandType != CMD_SELECT)
			|| force_generation)
	{
#endif
	if (pcqual)
	{
		/*
		 * Run the partition quals through const-simplification similar to
		 * check constraints.  We skip canonicalize_qual, though, because
		 * partition quals should be in canonical form already; also, since
		 * the qual is in implicit-AND format, we'd have to explicitly convert
		 * it to explicit-AND format and back again.
		 */
		pcqual = (List *) eval_const_expressions(root, (Node *) pcqual);

		/* Fix Vars to have the desired varno */
		if (varno != 1)
			ChangeVarNodes((Node *) pcqual, 1, varno, 0);
	}
#if PG_VERSION_NUM >= 110000
	}
#endif

	return pcqual;
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

#if PG_VERSION_NUM >= 110000
	case PARTITION_STRATEGY_HASH:
		Assert(spec->strategy == PARTITION_STRATEGY_HASH);
		/*
		 * Do not add the qual for hash partitioning, see comment in
		 * hypo_injectHypotheticalPartitioning about hash partitioning
		 * selectivity estimation
		 */
		break;
#endif

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

#if PG_VERSION_NUM >= 110000
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
#endif
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

#if PG_VERSION_NUM >= 110000
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
#endif

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

#if PG_VERSION_NUM >= 110000
	if (spec->is_default)
	{
		List	   *or_expr_args = NIL;
		PartitionDesc pdesc = hypo_generate_partitiondesc(parent);
		Oid		   *inhoids = pdesc->oids;
		int			nparts = pdesc->nparts,
					i;

		for (i = 0; i < nparts; i++)
		{
			Oid			inhrelid = inhoids[i];
			hypoTable   *part;
			PartitionBoundSpec *bspec;

			/*
			 * get each partition's boundspec from hypoTable entry
			 * instead of catalog
			 */
			part = hypo_find_table(inhrelid, false);
			bspec = part->boundspec;

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
#endif

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

/*
 * Checks if the new partition's bound overlaps any of the existing partitions
 * of parent.  Also performs additional checks as necessary per strategy.
 *
 * Heavily inspired on check_new_partition_bound
 */
static void
hypo_check_new_partition_bound(char *relname, hypoTable *parent,
						  PartitionBoundSpec *spec)
{
	PartitionKey key = parent->partkey;
	PartitionDesc partdesc = hypo_generate_partitiondesc(parent);
	PartitionBoundInfo boundinfo = partdesc->boundinfo;
	ParseState *pstate = make_parsestate(NULL);
	int			with = -1;
	bool		overlap = false;

#if PG_VERSION_NUM >= 110000
	if (spec->is_default)
	{
		/*
		 * The default partition bound never conflicts with any other
		 * partition's; if that's what we're attaching, the only possible
		 * problem is that one already exists, so check for that and we're
		 * done.
		 */
		if (boundinfo == NULL || !partition_bound_has_default(boundinfo))
			return;

		/* Default partition already exists, error out. */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("hypopg: partition \"%s\" conflicts with existing default partition \"%s\"",
						relname, get_rel_name(partdesc->oids[boundinfo->default_index])),
				 parser_errposition(pstate, spec->location)));
	}
#endif

	switch (key->strategy)
	{
#if PG_VERSION_NUM >= 110000
		case PARTITION_STRATEGY_HASH:
			{
				Assert(spec->strategy == PARTITION_STRATEGY_HASH);
				Assert(spec->remainder >= 0 && spec->remainder < spec->modulus);

				if (partdesc->nparts > 0)
				{
					Datum	  **datums = boundinfo->datums;
					int			ndatums = boundinfo->ndatums;
					int			greatest_modulus;
					int			remainder;
					int			offset;
					bool		valid_modulus = true;
					int			prev_modulus,	/* Previous largest modulus */
								next_modulus;	/* Next largest modulus */

					/*
					 * Check rule that every modulus must be a factor of the
					 * next larger modulus.  For example, if you have a bunch
					 * of partitions that all have modulus 5, you can add a
					 * new partition with modulus 10 or a new partition with
					 * modulus 15, but you cannot add both a partition with
					 * modulus 10 and a partition with modulus 15, because 10
					 * is not a factor of 15.
					 *
					 * Get the greatest (modulus, remainder) pair contained in
					 * boundinfo->datums that is less than or equal to the
					 * (spec->modulus, spec->remainder) pair.
					 */
					offset = partition_hash_bsearch(boundinfo,
													spec->modulus,
													spec->remainder);
					if (offset < 0)
					{
						next_modulus = DatumGetInt32(datums[0][0]);
						valid_modulus = (next_modulus % spec->modulus) == 0;
					}
					else
					{
						prev_modulus = DatumGetInt32(datums[offset][0]);
						valid_modulus = (spec->modulus % prev_modulus) == 0;

						if (valid_modulus && (offset + 1) < ndatums)
						{
							next_modulus = DatumGetInt32(datums[offset + 1][0]);
							valid_modulus = (next_modulus % spec->modulus) == 0;
						}
					}

					if (!valid_modulus)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
								 errmsg("hypopg: every hash partition modulus must be a factor of the next larger modulus")));

					greatest_modulus = get_hash_partition_greatest_modulus(boundinfo);
					remainder = spec->remainder;

					/*
					 * Normally, the lowest remainder that could conflict with
					 * the new partition is equal to the remainder specified
					 * for the new partition, but when the new partition has a
					 * modulus higher than any used so far, we need to adjust.
					 */
					if (remainder >= greatest_modulus)
						remainder = remainder % greatest_modulus;

					/* Check every potentially-conflicting remainder. */
					do
					{
						if (boundinfo->indexes[remainder] != -1)
						{
							overlap = true;
							with = boundinfo->indexes[remainder];
							break;
						}
						remainder += spec->modulus;
					} while (remainder < greatest_modulus);
				}

				break;
			}
#endif

		case PARTITION_STRATEGY_LIST:
			{
				Assert(spec->strategy == PARTITION_STRATEGY_LIST);

				if (partdesc->nparts > 0)
				{
					ListCell   *cell;

					Assert(boundinfo &&
						   boundinfo->strategy == PARTITION_STRATEGY_LIST &&
						   (boundinfo->ndatums > 0 ||
							partition_bound_accepts_nulls(boundinfo)
#if PG_VERSION_NUM >= 110000
							|| partition_bound_has_default(boundinfo)
#endif
						   ));

					foreach(cell, spec->listdatums)
					{
						Const	   *val = castNode(Const, lfirst(cell));

						if (!val->constisnull)
						{
							int			offset;
							bool		equal;

#if PG_VERSION_NUM < 110000
							offset = partition_bound_bsearch(key, boundinfo,
															&val->constvalue,
															true, &equal);
#else
							offset = partition_list_bsearch(&key->partsupfunc[0],
															key->partcollation,
															boundinfo,
															val->constvalue,
															&equal);
#endif
							if (offset >= 0 && equal)
							{
								overlap = true;
								with = boundinfo->indexes[offset];
								break;
							}
						}
						else if (partition_bound_accepts_nulls(boundinfo))
						{
							overlap = true;
							with = boundinfo->null_index;
							break;
						}
					}
				}

				break;
			}

		case PARTITION_STRATEGY_RANGE:
			{
				PartitionRangeBound *lower,
						   *upper;

				Assert(spec->strategy == PARTITION_STRATEGY_RANGE);
#if PG_VERSION_NUM < 110000
				lower = make_one_range_bound(key, -1, spec->lowerdatums, true);
				upper = make_one_range_bound(key, -1, spec->upperdatums, false);
#else
				lower = make_one_partition_rbound(key, -1, spec->lowerdatums, true);
				upper = make_one_partition_rbound(key, -1, spec->upperdatums, false);
#endif

				/*
				 * First check if the resulting range would be empty with
				 * specified lower and upper bounds
				 */
#if PG_VERSION_NUM < 110000
				if (partition_rbound_cmp(key, lower->datums,
										 lower->kind, true, upper) >= 0)
#else
				if (partition_rbound_cmp(key->partnatts, key->partsupfunc,
										 key->partcollation, lower->datums,
										 lower->kind, true, upper) >= 0)
#endif
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
							 errmsg("hypopg: empty range bound specified for partition \"%s\"",
									relname),
							 errdetail("hypopg: Specified lower bound %s is greater than or equal to upper bound %s.",
									   get_range_partbound_string(spec->lowerdatums),
									   get_range_partbound_string(spec->upperdatums)),
							 parser_errposition(pstate, spec->location)));
				}

				if (partdesc->nparts > 0)
				{
					int			offset;
					bool		equal;

					Assert(boundinfo &&
						   boundinfo->strategy == PARTITION_STRATEGY_RANGE &&
#if PG_VERSION_NUM < 110000
						   boundinfo->ndatums > 0
#else
						   (boundinfo->ndatums > 0 ||
							partition_bound_has_default(boundinfo))
#endif
						   );

					/*
					 * Test whether the new lower bound (which is treated
					 * inclusively as part of the new partition) lies inside
					 * an existing partition, or in a gap.
					 *
					 * If it's inside an existing partition, the bound at
					 * offset + 1 will be the upper bound of that partition,
					 * and its index will be >= 0.
					 *
					 * If it's in a gap, the bound at offset + 1 will be the
					 * lower bound of the next partition, and its index will
					 * be -1. This is also true if there is no next partition,
					 * since the index array is initialised with an extra -1
					 * at the end.
					 */
#if PG_VERSION_NUM < 110000
					offset = partition_bound_bsearch(key, boundinfo, lower,
													 true, &equal);
#else
					offset = partition_range_bsearch(key->partnatts,
													 key->partsupfunc,
													 key->partcollation,
													 boundinfo, lower,
													 &equal);
#endif

					if (boundinfo->indexes[offset + 1] < 0)
					{
						/*
						 * Check that the new partition will fit in the gap.
						 * For it to fit, the new upper bound must be less
						 * than or equal to the lower bound of the next
						 * partition, if there is one.
						 */
						if (offset + 1 < boundinfo->ndatums)
						{
							int32		cmpval;
#if PG_VERSION_NUM < 110000
							cmpval = partition_bound_cmp(key, boundinfo,
														  offset + 1, upper,
														  true);
#else
							Datum	   *datums;
							PartitionRangeDatumKind *kind;
							bool		is_lower;

							datums = boundinfo->datums[offset + 1];
							kind = boundinfo->kind[offset + 1];
							is_lower = (boundinfo->indexes[offset + 1] == -1);

							cmpval = partition_rbound_cmp(key->partnatts,
														  key->partsupfunc,
														  key->partcollation,
														  datums, kind,
														  is_lower, upper);
#endif
							if (cmpval < 0)
							{
								/*
								 * The new partition overlaps with the
								 * existing partition between offset + 1 and
								 * offset + 2.
								 */
								overlap = true;
								with = boundinfo->indexes[offset + 2];
							}
						}
					}
					else
					{
						/*
						 * The new partition overlaps with the existing
						 * partition between offset and offset + 1.
						 */
						overlap = true;
						with = boundinfo->indexes[offset + 1];
					}
				}

				break;
			}

		default:
			elog(ERROR, "hypopg: unexpected partition strategy: %d",
				 (int) key->strategy);
	}

	if (overlap)
	{
		hypoTable *table = hypo_find_table(partdesc->oids[with], false);

		Assert(with >= 0);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("hypopg: partition \"%s\" would overlap partition \"%s\"",
						relname, table->tablename),
				 parser_errposition(pstate, spec->location)));
	}
}

#endif		/* pg10+ (~l. 81) */

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
	hypoTable  *parent;
	Oid			parentid, rootid;
	hypoTable  *entry;
	List	   *parsetree_list;
	RawStmt	   *raw_stmt;
	CreateStmt *stmt;
	RangeVar   *rv;
	TupleDesc	tupdesc;
	Datum		values[HYPO_ADD_PART_COLS];
	bool		nulls[HYPO_ADD_PART_COLS];
	int			i = 0;

	/* Process any pending invalidation */
	hypo_process_inval();

	if (!hypoTables)
		hypo_initTablesHash();

	if (!PG_ARGISNULL(2))
		partition_by = TextDatumGetCString(PG_GETARG_TEXT_PP(2));

	if (RelnameGetRelid(partname) != InvalidOid)
		elog(ERROR, "hypopg: real table %s already exists",
				quote_identifier(partname));

	if (hypo_table_name_get_entry(partname) != NULL)
		elog(ERROR, "hypopg: hypothetical table %s already exists",
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
		parent = hypo_find_table(parentid, false);
	}

	entry = hypo_table_store_parsetree((CreateStmt *) stmt, sql.data,
			parent, rootid);

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

	/* Process any pending invalidation */
	hypo_process_inval();

	if (!hypo_table_oid_is_hypothetical(tableid))
		elog(ERROR, "hypopg: Oid %d is not a hypothetically partitioned table",
				tableid);

	PG_RETURN_BOOL(hypo_table_remove(tableid, NULL, true));
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
	hypoTable *entry;
	hypoTable	   *root;
	char		   *root_name = NULL;
	char		   *nspname = NULL;
	bool			found = false;
	HeapTuple		tp;
	Relation		relation;
	List		   *children = NIL;
	List		   *parsetree_list;
	StringInfoData	sql;
	RawStmt		   *raw_stmt;

	/* Process any pending invalidation */
	hypo_process_inval();

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

	root = hypo_find_table(tableid, true);
	if (root)
	{
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
			NULL, tableid);

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
	/* Process any pending invalidation */
	hypo_process_inval();

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
	HASH_SEQ_STATUS	hash_seq;
	hypoTable	   *entry;

	/* Process any pending invalidation */
	hypo_process_inval();

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

	hash_seq_init(&hash_seq, hypoTables);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
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
