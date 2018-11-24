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

#include "postgres.h"
#include "fmgr.h"

#include "funcapi.h"
#include "miscadmin.h"

#ifdef _MSC_VER
#include <float.h>				/* for _isnan */
#endif
#include <math.h>

#include "access/hash.h"
#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#if PG_VERSION_NUM >= 120000
#include "utils/float.h"
#endif
#include "utils/guc.h"
#include "utils/lsyscache.h"
#if PG_VERSION_NUM >= 110000
#include "utils/partcache.h"
#endif
#if PG_VERSION_NUM >= 100000
#include "utils/ruleutils.h"
#endif
#include "utils/selfuncs.h"

#include "include/hypopg.h"
#include "include/hypopg_analyze.h"
#include "include/hypopg_table.h"

/*--- Variables exported ---*/

#if PG_VERSION_NUM >= 100000
HTAB *hypoStatsHash = NULL;

/* A few variables that don't seem worth passing around as parameters */
static MemoryContext anl_context = NULL;
#endif

/*--- Functions --- */

PG_FUNCTION_INFO_V1(hypopg_analyze);
PG_FUNCTION_INFO_V1(hypopg_statistic);

#if PG_VERSION_NUM >= 100000
static void hypo_do_analyze_partition(Relation onerel, Relation pgstats,
		hypoTable *entry, float4 fraction);
static void hypo_do_analyze_tree(Relation onerel, Relation pgstats,
		float4 fraction, hypoTable *parent);
static uint32 hypo_hash_fn(const void *key, Size keysize);
static void hypo_update_attstats(hypoTable *part, int natts,
		VacAttrStats **vacattrstats, Relation pgstats);
#endif


/*
 * Wrapper on top on clauselist_selectivity.  If clauses is NIL, it's a
 * hypothetical partition selectivity estimation and the clauses will be
 * retrieved from the stored partition bounds.
 */
Selectivity
hypo_clauselist_selectivity(PlannerInfo *root, RelOptInfo *rel, List *clauses,
		Oid table_relid, Oid parent_oid)
{
	Selectivity selectivity;
	PlannerInfo *root_dummy;
	PlannerGlobal *glob;
	Query	   *parse;
	List	   *rtable = NIL;
	RangeTblEntry *rte;
	int         save_relid;

	/* create a fake minimal PlannerInfo */
	root_dummy = makeNode(PlannerInfo);

	glob = makeNode(PlannerGlobal);
	glob->boundParams = NULL;
	root_dummy->glob = glob;

	/* only 1 table: the original table */
	rte = makeNode(RangeTblEntry);
	rte->relkind = RTE_RELATION;
	rte->relid = table_relid;
	rte->inh = false;		/* don't include inherited children */
	rtable = list_make1(rte);

	parse = makeNode(Query);
	/*
	 * makeNode() does a palloc0fast, so the commandType should already be
	 * CMD_UNKNOWN, but force it for safety.
	 */
	parse->commandType = CMD_UNKNOWN;
	parse->rtable = rtable;
	root_dummy->parse = parse;

	/*
	 * allocate simple_rel_arrays and simple_rte_arrays. This function
	 * will also setup simple_rte_arrays with the previous rte.
	 */
	setup_simple_rel_arrays(root_dummy);

	/* also add our table info */
	save_relid = rel->relid;
	rel->relid = 1;
	root_dummy->simple_rel_array[1] = rel;

#if PG_VERSION_NUM >= 100000
	/* Are we estimating selectivity for hypothetical partitioning? */
	if (clauses == NIL)
	{
		hypoTable *part = hypo_find_table(parent_oid, false);
		RangeTblEntry *save_rte = planner_rt_fetch(save_relid, root);

		Assert(root != NULL);
		Assert(part->partkey);

		/* add the hypothetical partition oid to be able to get the
		 * constraints */
		HYPO_TABLE_RTE_COPY_HYPOOID_FROM_RTE(rte, save_rte);
		root_dummy->parse->rtable = list_make1(rte);

		/* get the partition constraints, setup for a rel with relid 1 */
		clauses = hypo_get_partition_constraints(root_dummy, rel, part, true);

		/*
		 * and remove the hypothetical oid to avoid computing selectivity with
		 * hypothetical statistics
		 */
		HYPO_TABLE_RTE_CLEAR_HYPOOID(rte);
		root_dummy->parse->rtable = list_make1(rte);
	}
	else
	{	/* We estimates selectivity for hypothetical indexes */
		RangeTblEntry *rt = NULL;
		RangeTblEntry *dummyrte = planner_rt_fetch(1, root_dummy);

		if (root)
			rt = planner_rt_fetch(save_relid, root);

		/* modify RangeTableEntry to be able to get correct oid */
		if (HYPO_TABLE_RTE_HAS_HYPOOID(rt))  /* Is this a hypothetical partition? */
			HYPO_TABLE_RTE_COPY_HYPOOID_FROM_RTE(dummyrte, rt);
		else if (rt)
			dummyrte->relid = rt->relid;
		else
		{
			Assert(save_relid == 1);
		}
	}
#else
	Assert(clauses != NIL);
#endif

	/*
	 * per comment on clause_selectivity(), JOIN_INNER must be passed if
	 * the clause isn't a join clause, which is our case, and passing 0 to
	 * varRelid is appropriate for restriction clause.
	 */
	selectivity = clauselist_selectivity(root_dummy,
			clauses,
			0,
			JOIN_INNER,
			NULL);

	/* restore the original rel's relid */
	rel->relid = save_relid;

	elog(DEBUG2, "hypopg: selectivity: %lf", selectivity);

	return selectivity;
}

#if PG_VERSION_NUM >= 100000
/*
 * Heavily inspired on do_analyze_rel().
 *
 * Perform an analyze-like on the rows belonging to the given hypoTable entry
 * to compute statistics at partition level, and store them in the local hash
 * table.  Caller is responsible for calling SPI_connect() and SPI_close()
 * before and after this function.
 */
static void hypo_do_analyze_partition(Relation onerel, Relation pgstats,
		hypoTable *part, float4 fraction)
{
	int			attr_cnt,
				tcnt,
				i;
	VacAttrStats **vacattrstats;
	int			numrows;
	double		totalrows;
	MemoryContext caller_context;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	Oid			root_tableid = part->rootid;
	List	   *context = deparse_context_for(get_rel_name(root_tableid),
			root_tableid);
	StringInfoData buf;
	bool		inh = false;
	List	   *constraints;
	char	   *str;
	int			ret;

	Assert(hypoStatsHash);

#if PG_VERSION_NUM >= 110000
	/*
	 * We can't use the same heuristics for hash partitions selectivity
	 * estimation, because its constraint is using satisfies_hash_partition(),
	 * for which the selectivity estimation won't have any knowledge and will
	 * therefore apply some default selectivty which will be totally wrong.
	 * Instead, we'll just compute the selectivity of hash partitions using the
	 * modulus, so don't bother computing and storing statistics;
	 */
	if (part->boundspec->strategy == PARTITION_STRATEGY_HASH)
	{
		elog(NOTICE, "hypothetical partition \"%s\" is a hash partition,"
				" skipping", part->tablename);
		return;
	}
#endif

	/*
	 * Set up a working context so that we can easily free whatever junk gets
	 * created.
	 */
	anl_context = AllocSetContextCreate(CurrentMemoryContext,
										"Analyze",
										ALLOCSET_DEFAULT_SIZES);
	caller_context = MemoryContextSwitchTo(anl_context);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(onerel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * Determine which columns to analyze
	 *
	 * Since we're getting statistic for a hypothetical partition, simply get
	 * all the columns of the parent's table.
	 */
	attr_cnt = onerel->rd_att->natts;
	vacattrstats = (VacAttrStats **)
		palloc(attr_cnt * sizeof(VacAttrStats *));
	tcnt = 0;
	for (i = 1; i <= attr_cnt; i++)
	{
		vacattrstats[tcnt] = examine_attribute(onerel, i, NULL);
		if (vacattrstats[tcnt] != NULL)
			tcnt++;
	}
	attr_cnt = tcnt;

	/* TODO Handle indexes and hypothetical indexes */

	/*
	 * Acquire the sample rows
	 */
	constraints = hypo_get_partition_quals_inh(part, NULL);
	constraints = (List *) make_ands_explicit(constraints);
	str = deparse_expression((Node *) constraints, context, false, false);

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT * FROM %s.%s TABLESAMPLE SYSTEM(%.2f)",
		quote_identifier(get_namespace_name(get_rel_namespace(root_tableid))),
		quote_identifier(get_rel_name(root_tableid)),
		fraction);

	appendStringInfo(&buf, " WHERE %s",
		str);

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "hypopg: Could not sample rows for hypothetical "
				"partition \"%s\": SPI_execute returned %d",
				part->tablename, ret);

	/*
	 * Use a lower bound of 100 rows to avoid possible overflow in Vitter's
	 * algorithm.
	 */
	if (SPI_processed < 100)
		elog(ERROR, "hypopg: the %f fraction of rows to analyze is too low."
				" At least 100 rows must be computed.",
				fraction);

	/* And make sure we don't overflow the compute_stats function */
	if (SPI_processed >= INT32_MAX)
		elog(ERROR, "hypopg: the %f fraction of rows to analyze is too high."
				" At most %d rows must be computed.",
				fraction, INT32_MAX);

	numrows = (int) SPI_processed;
	/*
	 * FIXME do a better estimation of the total number of rows for the
	 * partition
	 * */
	totalrows = SPI_processed * 100 / fraction;
	part->tuples = (int) totalrows;
	part->set_tuples = true;

	/*
	 * Compute the statistics.  Temporary results during the calculations for
	 * each column are stored in a child context.  The calc routines are
	 * responsible to make sure that whatever they store into the VacAttrStats
	 * structure is allocated in anl_context.
	 */
	if (numrows >= 100 && numrows < INT32_MAX)
	{
		MemoryContext col_context,
					old_context;

		col_context = AllocSetContextCreate(anl_context,
											"Analyze Column",
											ALLOCSET_DEFAULT_SIZES);
		old_context = MemoryContextSwitchTo(col_context);

		for (i = 0; i < attr_cnt; i++)
		{
			VacAttrStats *stats = vacattrstats[i];
			AttributeOpts *aopt;

			stats->rows = SPI_tuptable->vals;
			stats->tupDesc = onerel->rd_att;
			stats->compute_stats(stats,
								 std_fetch_func,
								 numrows,
								 totalrows);

			/*
			 * If the appropriate flavor of the n_distinct option is
			 * specified, override with the corresponding value.
			 */
			aopt = get_attribute_options(onerel->rd_id, stats->attr->attnum);
			if (aopt != NULL)
			{
				float8		n_distinct;

				n_distinct = inh ? aopt->n_distinct_inherited : aopt->n_distinct;
				if (n_distinct != 0.0)
					stats->stadistinct = n_distinct;
			}

			MemoryContextResetAndDeleteChildren(col_context);
		}

		/* TODO Handle indexes and hypothetical indexes */

		MemoryContextSwitchTo(old_context);
		MemoryContextDelete(col_context);

		hypo_update_attstats(part, attr_cnt, vacattrstats, pgstats);
	}

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Restore current context and release memory */
	MemoryContextSwitchTo(caller_context);
	MemoryContextDelete(anl_context);
	anl_context = NULL;
}

static void hypo_do_analyze_tree(Relation onerel, Relation pgstats,
		float4 fraction, hypoTable *parent)
{
	ListCell *lc;

	Assert(hypoTables);

	foreach(lc, parent->children)
	{
		hypoTable  *part = hypo_find_table(lfirst_oid(lc), false);

		hypo_do_analyze_partition(onerel, pgstats, part, fraction);
		hypo_do_analyze_tree(onerel, pgstats, fraction, part);
	}
}

static uint32 hypo_hash_fn(const void *key, Size keysize)
{
	const hypoStatsKey *k = (const hypoStatsKey *) key;

	return hash_uint32((uint32) k->relid ^
		   (uint32) k->attnum);
}

/*
 * Remove all stored stats for a given hypothetical partition
 */
void
hypo_stat_remove(Oid partid)
{
	HASH_SEQ_STATUS hash_seq;
	hypoStatsEntry *stat;

	if (!hypoStatsHash)
		return;

	hash_seq_init(&hash_seq, hypoStatsHash);
	while((stat = hash_seq_search(&hash_seq)) != NULL)
	{
		if (stat->key.relid == partid)
		{
			pfree(stat->statsTuple);
			hash_search(hypoStatsHash, &stat->key, HASH_REMOVE, NULL);
		}
	}
}

/*
 * Heavily inspired on update_attstats().
 *
 * Form pg_statistic rows from the computed stats, and store them in the local
 * hash.
 */
static void hypo_update_attstats(hypoTable *part, int natts,
		VacAttrStats **vacattrstats, Relation pgstats)
{
	int			attno;
	hypoStatsKey key;
	bool		found;
	hypoStatsEntry *s;
	MemoryContext	oldcontext;

	for (attno = 0; attno < natts; attno++)
	{
		VacAttrStats *stats = vacattrstats[attno];
		int			i,
					k,
					n;
		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];

		/* Ignore attr if we weren't able to collect stats */
		if (!stats->stats_valid)
			continue;

		/*
		 * Construct a new pg_statistic tuple
		 */
		for (i = 0; i < Natts_pg_statistic; ++i)
		{
			nulls[i] = false;
		}

		values[Anum_pg_statistic_starelid - 1] = ObjectIdGetDatum(part->oid);
		values[Anum_pg_statistic_staattnum - 1] = Int16GetDatum(stats->attr->attnum);
		values[Anum_pg_statistic_stainherit - 1] = BoolGetDatum(false);
		values[Anum_pg_statistic_stanullfrac - 1] = Float4GetDatum(stats->stanullfrac);
		values[Anum_pg_statistic_stawidth - 1] = Int32GetDatum(stats->stawidth);
		values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(stats->stadistinct);
		i = Anum_pg_statistic_stakind1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = Int16GetDatum(stats->stakind[k]); /* stakindN */
		}
		i = Anum_pg_statistic_staop1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			values[i++] = ObjectIdGetDatum(stats->staop[k]);	/* staopN */
		}
		i = Anum_pg_statistic_stanumbers1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			int			nnum = stats->numnumbers[k];

			if (nnum > 0)
			{
				Datum	   *numdatums = (Datum *) palloc(nnum * sizeof(Datum));
				ArrayType  *arry;

				for (n = 0; n < nnum; n++)
					numdatums[n] = Float4GetDatum(stats->stanumbers[k][n]);
				/* XXX knows more than it should about type float4: */
				arry = construct_array(numdatums, nnum,
						FLOAT4OID,
						sizeof(float4), FLOAT4PASSBYVAL, 'i');
				values[i++] = PointerGetDatum(arry);	/* stanumbersN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}
		i = Anum_pg_statistic_stavalues1 - 1;
		for (k = 0; k < STATISTIC_NUM_SLOTS; k++)
		{
			if (stats->numvalues[k] > 0)
			{
				ArrayType  *arry;

				arry = construct_array(stats->stavalues[k],
						stats->numvalues[k],
						stats->statypid[k],
						stats->statyplen[k],
						stats->statypbyval[k],
						stats->statypalign[k]);
				values[i++] = PointerGetDatum(arry);	/* stavaluesN */
			}
			else
			{
				nulls[i] = true;
				values[i++] = (Datum) 0;
			}
		}

		/* Store the statistics in local hash */
		memset(&key, 0, sizeof(hypoStatsKey));
		key.relid = part->oid;
		key.attnum = stats->attr->attnum;

		s = hash_search(hypoStatsHash, &key, HASH_ENTER, &found);

		/* Free tuple if one existed */
		if (found)
		{
			pfree(s->statsTuple);
			s->statsTuple = NULL;
		}

		oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
		s->statsTuple = heap_form_tuple(RelationGetDescr(pgstats),
				values, nulls);
		MemoryContextSwitchTo(oldcontext);
	}
}
#endif

/*
 * SQL wrapper to perform an ANALYZE-like operation.
 */
Datum
hypopg_analyze(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	Oid				root_tableid = PG_GETARG_OID(0);
	float4			fraction = PG_GETARG_FLOAT4(1);
	hypoTable	   *root_entry;
	Relation		onerel;
	Relation		pgstats;
	int				ret;

	/* Process any pending invalidation */
	hypo_process_inval();

	root_entry = hypo_find_table(root_tableid, true);

	if (!root_entry)
		elog(ERROR, "hypopg: this table is not hypothetically partitioned");

	if (isnan(fraction) || fraction == get_float4_infinity()
			|| fraction <= 0 || fraction > 100)
		elog(ERROR, "hypopg: invalid fraction: %f", fraction);

	/* Connect to SPI manager */
	if ((ret = SPI_connect()) < 0)
		/* internal error */
		elog(ERROR, "hypopg: SPI_connect returned %d", ret);

	if (!hypoStatsHash)
	{
		HASHCTL info;

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(hypoStatsKey);
		info.entrysize = sizeof(hypoStatsEntry);
		info.hash = hypo_hash_fn;
		info.hcxt = HypoMemoryContext;
		hypoStatsHash = hash_create("hypo_stats",
				500,
				&info,
				HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}

	onerel = heap_open(root_tableid, AccessShareLock);
	pgstats = heap_open(StatisticRelationId, AccessShareLock);

	hypo_do_analyze_tree(onerel, pgstats, fraction, root_entry);

	/* release SPI related resources (and return to caller's context) */
	SPI_finish();
	relation_close(onerel, AccessShareLock);
	relation_close(pgstats, AccessShareLock);


	return (Datum) 0;
#endif
}


/*
 * SQL wrapper to retrieve the list of stored statistics.
 */
Datum
hypopg_statistic(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM < 100000
HYPO_PARTITION_NOT_SUPPORTED();
#else
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	HASH_SEQ_STATUS hash_seq;
	hypoStatsEntry *entry;
	Relation		pgstats;

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

	if (!hypoStatsHash)
		return (Datum) 0;

	pgstats = heap_open(StatisticRelationId, AccessShareLock);

	hash_seq_init(&hash_seq, hypoStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[Natts_pg_statistic];
		bool		nulls[Natts_pg_statistic];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		heap_deform_tuple(entry->statsTuple, RelationGetDescr(pgstats),
				values, nulls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	relation_close(pgstats, AccessShareLock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
#endif
}
