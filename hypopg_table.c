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

#include "postgres.h"
#include "fmgr.h"

#include "funcapi.h"
#include "miscadmin.h"

#if PG_VERSION_NUM >= 100000
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_utilcmd.h"
#include "rewrite/rewriteManip.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#endif

#include "include/hypopg.h"
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
static List *hypo_find_inheritance_children(hypoTable *parent);
static PartitionDesc hypo_generate_partitiondesc(hypoTable *parent);
static void hypo_generate_partkey(CreateStmt *stmt, Oid parentid,
		hypoTable *entry);
static PartitionScheme hypo_generate_part_scheme(CreateStmt *stmt,
		PartitionKey partkey, Oid parentid);
static void hypo_generate_partition_key_exprs(hypoTable *entry,
		RelOptInfo *rel);
static PartitionBoundSpec *hypo_get_boundspec(Oid tableid);
static Oid hypo_get_default_partition_oid(Oid parentid);
static char *hypo_get_partbounddef(hypoTable *entry);
static char *hypo_get_partkeydef(hypoTable *entry);
static hypoTable *hypo_newTable(Oid parentid);
static Oid hypo_table_find_parent_entry(hypoTable *entry);
static Oid hypo_table_find_parent_oid(Oid parentid);
static Oid hypo_table_find_root_oid(hypoTable *entry);
static hypoTable *hypo_find_table(Oid tableid);
static bool hypo_table_name_is_hypothetical(const char *name);
static bool hypo_table_oid_is_hypothetical(Oid relid);
static void hypo_table_pfree(hypoTable *entry);
static bool hypo_table_remove(Oid tableid);
static const hypoTable *hypo_table_store_parsetree(CreateStmt *node,
						   const char *queryString, Oid parentid);
static PartitionBoundSpec *hypo_transformPartitionBound(ParseState *pstate,
		hypoTable *parent, PartitionBoundSpec *spec);


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

				lower = make_one_range_bound(key, i, spec->lowerdatums,
											 true);
				upper = make_one_range_bound(key, i, spec->upperdatums,
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


	key->strategy = strategy;
	key->partexprs = partexprs;
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
 * Given a CreateStmt, containing a PartitionSpec clause, generate a
 * RelOptInfo's PartitionScheme structure, or return NULL if no partspec clause
 * was provided.
 */
static PartitionScheme
hypo_generate_part_scheme(CreateStmt *stmt, PartitionKey partkey, Oid
		parentid)
{
	MemoryContext oldcontext;
	PartitionScheme part_scheme;
	int			partnatts;

	if (!stmt->partspec)
		return NULL;

	Assert(CurrentMemoryContext != HypoMemoryContext);

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);

	/* adapted from plancat.c / find_partition_scheme() */
	Assert(partkey != NULL);
	partnatts = partkey->partnatts;

	part_scheme = (PartitionScheme) palloc0(sizeof(PartitionSchemeData));
	part_scheme->strategy = partkey->strategy;
	part_scheme->partnatts = partkey->partnatts;

	part_scheme->partopfamily = (Oid *) palloc(sizeof(Oid) * partnatts);
	memcpy(part_scheme->partopfamily, partkey->partopfamily,
		   sizeof(Oid) * partnatts);

	part_scheme->partopcintype = (Oid *) palloc(sizeof(Oid) * partnatts);
	memcpy(part_scheme->partopcintype, partkey->partopcintype,
		   sizeof(Oid) * partnatts);

	part_scheme->parttypcoll = (Oid *) palloc(sizeof(Oid) * partnatts);
	memcpy(part_scheme->parttypcoll, partkey->parttypcoll,
		   sizeof(Oid) * partnatts);

	part_scheme->parttyplen = (int16 *) palloc(sizeof(int16) * partnatts);
	memcpy(part_scheme->parttyplen, partkey->parttyplen,
		   sizeof(int16) * partnatts);

	part_scheme->parttypbyval = (bool *) palloc(sizeof(bool) * partnatts);
	memcpy(part_scheme->parttypbyval, partkey->parttypbyval,
		   sizeof(bool) * partnatts);

	MemoryContextSwitchTo(oldcontext);

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
	hypoTable *entry = hypo_find_table(tableid);

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

	relid = hypo_table_find_root_oid(entry);

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
	MemoryContext	oldcontext;

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);

	entry = (hypoTable *) palloc0(sizeof(hypoTable));

	entry->tablename = palloc0(NAMEDATALEN);
	entry->part_scheme = NULL; /* wil be generated later if needed */
	entry->boundspec = NULL; /* wil be generated later if needed */
	entry->partkey = NULL; /* wil be generated later if needed */

	MemoryContextSwitchTo(oldcontext);

	/*
	 * If the given root table oid isn't present in hypoTables, we're
	 * partitioning it, so keep its oid, otherwise generate a new oid
	 */
	entry->parentid = hypo_table_find_parent_oid(parentid);

	if (entry->parentid != InvalidOid)
		entry->oid = hypo_getNewOid(parentid);
	else
		entry->oid = parentid;

	return entry;
}

/*
 * Find the direct parent oid of a hypothetical partition entry.  Return NULL
 * is it's a top level table.
 */
static Oid
hypo_table_find_parent_entry(hypoTable *entry)
{
	return hypo_table_find_parent_oid(entry->parentid);
}

/*
 * Find the direct parent oid of a hypothetical partition given it's parentid
 * field.  Return InvalidOid is it's a top level table.
 */
static Oid
hypo_table_find_parent_oid(Oid parentid)
{
	ListCell *lc;

	foreach(lc, hypoTables)
	{
		hypoTable *entry = (hypoTable *) lfirst(lc);

		if (entry->oid == parentid)
			return entry->oid;
	}

	return InvalidOid;
}

/*
 * Find the root table identifier of an hypothetical table
 */
static Oid
hypo_table_find_root_oid(hypoTable *entry)
{
	Oid parent, last;
	if (entry->parentid == InvalidOid)
		return entry->oid;

	parent = hypo_table_find_parent_entry(entry);

	while (parent != InvalidOid)
	{
		last = parent;
		parent = hypo_table_find_parent_entry(entry);
	}

	return last;
}

/*
 * Return the stored hypothetical table corresponding to the Oid if any,
 * otherwise return NULL
 */
static hypoTable *
hypo_find_table(Oid tableid)
{
	ListCell *lc;

	foreach(lc, hypoTables)
	{
		hypoTable *entry = (hypoTable *) lfirst(lc);

		if (entry->oid != tableid)
			continue;

		return entry;
	}

	return NULL;
}

/*
 * Is the given name an hypothetical partition ?
 */
static bool
hypo_table_name_is_hypothetical(const char *name)
{
	ListCell   *lc;

	foreach(lc, hypoTables)
	{
		hypoTable  *entry = (hypoTable *) lfirst(lc);

		if (strcmp(entry->tablename, name) == 0)
			return true;
	}

	return false;
}

/*
 * Is the given relid an hypothetical partition ?
 */
static bool
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

	if (entry->part_scheme)
	{
		pfree(entry->part_scheme->partopfamily);
		pfree(entry->part_scheme->partopcintype);
		pfree(entry->part_scheme->parttypcoll);
		pfree(entry->part_scheme->parttyplen);
		pfree(entry->part_scheme);
	}

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
 * hypo_table_pfree) all memory that has been allocated.
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
		Oid parentid)
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
					quote_identifier(hypo_find_table(defaultpart)->tablename));
		}
	}

	/* now create the hypothetical index entry */
	entry = hypo_newTable(parentid);

	strncpy(entry->tablename, node->relation->relname, NAMEDATALEN);

	/* The CreateStmt specified a PARTITION BY clause, store it */
	if (stmt->partspec)
	{
		hypo_generate_partkey(stmt, parentid, entry);
		entry->part_scheme = hypo_generate_part_scheme(stmt, entry->partkey,
				parentid);
	}

	if (boundspec)
	{
		MemoryContext oldcontext;
		ParseState *pstate;

		pstate = make_parsestate(NULL);
		pstate->p_sourcetext = queryString;

		oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
		entry->boundspec = hypo_transformPartitionBound(pstate,
				hypo_find_table(parentid), boundspec);
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
			colname = get_attname(parent->oid,
								  key->partattrs[0], false);
		else
			colname = deparse_expression((Node *) linitial(partexprs),
										 deparse_context_for(parent->tablename,
															 parent->oid),
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
				colname = get_attname(parent->oid,
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
	StringInfoData	sql;
	Oid			parentid;
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
	{
		elog(ERROR, "hypopg: multi-level partitioning is not supported yet");
	}

	if (hypo_table_name_is_hypothetical(partname))
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

	parsetree_list = pg_parse_query(sql.data);
	raw_stmt = (RawStmt *) linitial(parsetree_list);
	stmt = (CreateStmt *) raw_stmt->stmt;
	Assert(IsA(stmt, CreateStmt));

	if (!stmt->partbound || !stmt->inhRelations)
		elog(ERROR, "hypopg: you must specify a PARTITION OF clause");

	if (stmt->partspec)
		elog(ERROR, "hypopg: multi-level partitioning is not supported yet");

	/* Find the parent's oid */
	if (list_length(stmt->inhRelations) != 1)
		elog(ERROR, "hypopg: unexpected list lenght %d, expected 1",
				list_length(stmt->inhRelations));

	rv = (RangeVar *) linitial(stmt->inhRelations);
	/* FIXME change this when handling multi-level partitioning */
	parentid = RangeVarGetRelid(rv, AccessShareLock, false);

	if (!hypo_table_oid_is_hypothetical(parentid))
		elog(ERROR, "hypopg: %s must be hypothetically partitioned first",
				quote_identifier(rv->relname));

	entry = hypo_table_store_parsetree((CreateStmt *) stmt, sql.data,
			parentid);

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
			tableid);

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
