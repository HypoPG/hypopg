/*-------------------------------------------------------------------------
 *
 * hypopg.c: Implementation of hypothetical indexes for PostgreSQL
 *
 * Some functions are imported from PostgreSQL source code, theses are present
 * in hypopg_import.* files.
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
#include "miscadmin.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#if PG_VERSION_NUM >= 100000
#include "access/xact.h"
#endif
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "parser/parsetree.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

#include "include/hypopg.h"
#include "include/hypopg_analyze.h"
#include "include/hypopg_import.h"
#include "include/hypopg_index.h"
#include "include/hypopg_table.h"

PG_MODULE_MAGIC;

/*--- Macros ---*/
#define HYPO_ENABLED() (isExplain && hypo_is_enabled)

typedef struct hypoWalkerContext
{
	bool explain_found;
} hypoWalkerContext;

/*--- Variables exported ---*/

bool isExplain;
bool hypo_is_enabled;
MemoryContext HypoMemoryContext;

/*--- Variables not exported ---*/

static List *pending_invals = NIL; /* List of interesting OID for which we
									  received inval messages that need to be
									  processed. */

/*--- Functions --- */

void		_PG_init(void);
void		_PG_fini(void);

Datum		hypopg_reset(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hypopg_reset);

static void
hypo_utility_hook(
#if PG_VERSION_NUM >= 100000
				  PlannedStmt *pstmt,
#else
				  Node *parsetree,
#endif
				  const char *queryString,
#if PG_VERSION_NUM >= 90300
				  ProcessUtilityContext context,
#endif
				  ParamListInfo params,
#if PG_VERSION_NUM >= 100000
				  QueryEnvironment *queryEnv,
#endif
#if PG_VERSION_NUM < 90300
				  bool isTopLevel,
#endif
				  DestReceiver *dest,
				  char *completionTag);
static ProcessUtility_hook_type prev_utility_hook = NULL;

static void hypo_executorEnd_hook(QueryDesc *queryDesc);
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;


static void hypo_get_relation_info_hook(PlannerInfo *root,
							Oid relationObjectId,
							bool inhparent,
							RelOptInfo *rel);
static get_relation_info_hook_type prev_get_relation_info_hook = NULL;

static bool hypo_get_relation_stats_hook(PlannerInfo *root,
		RangeTblEntry *rte,
		AttrNumber attnum,
		VariableStatData *vardata);
static get_relation_stats_hook_type prev_get_relation_stats_hook = NULL;
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
static void hypo_set_rel_pathlist_hook(PlannerInfo *root,
									   RelOptInfo *rel,
									   Index rti,
									   RangeTblEntry *rte);
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
#endif

static bool hypo_query_walker(Node *node, hypoWalkerContext *context);
static void hypo_CacheRelCallback(Datum arg, Oid relid);

void
_PG_init(void)
{
	/* Install hooks */
	prev_utility_hook = ProcessUtility_hook;
	ProcessUtility_hook = hypo_utility_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = hypo_executorEnd_hook;

	prev_get_relation_info_hook = get_relation_info_hook;
	get_relation_info_hook = hypo_get_relation_info_hook;

	prev_explain_get_index_name_hook = explain_get_index_name_hook;
	explain_get_index_name_hook = hypo_explain_get_index_name_hook;

	prev_get_relation_stats_hook = get_relation_stats_hook;
	get_relation_stats_hook = hypo_get_relation_stats_hook;
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = hypo_set_rel_pathlist_hook;
#endif
	isExplain = false;
	hypoIndexes = NIL;
#if PG_VERSION_NUM >= 100000
	hypoTables = NULL;
#endif

	HypoMemoryContext = AllocSetContextCreate(TopMemoryContext,
			"HypoPG context",
#if PG_VERSION_NUM >= 90600
			ALLOCSET_DEFAULT_SIZES
#else
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE
#endif
			);

	DefineCustomBoolVariable("hypopg.enabled",
							 "Enable / Disable hypopg",
							 NULL,
							 &hypo_is_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	CacheRegisterRelcacheCallback(hypo_CacheRelCallback, (Datum) 0);
}

void
_PG_fini(void)
{
	/* uninstall hooks */
	ProcessUtility_hook = prev_utility_hook;
	ExecutorEnd_hook = prev_ExecutorEnd_hook;
	get_relation_info_hook = prev_get_relation_info_hook;
	explain_get_index_name_hook = prev_explain_get_index_name_hook;
	get_relation_stats_hook = prev_get_relation_stats_hook;
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
#endif
}

/*---------------------------------
 * Wrapper around GetNewRelFileNode
 * Return a new OID for an hypothetical index.
 */
Oid
hypo_getNewOid(Oid relid)
{
	Relation	pg_class;
	Relation	relation;
	Oid			newoid;
	Oid			reltablespace;
	char		relpersistence;

	/* Open the relation on which we want a new OID */
	relation = heap_open(relid, AccessShareLock);

	reltablespace = relation->rd_rel->reltablespace;
	relpersistence = relation->rd_rel->relpersistence;

	/* Close the relation and release the lock now */
	heap_close(relation, AccessShareLock);

	/* Open pg_class to aks a new OID */
	pg_class = heap_open(RelationRelationId, RowExclusiveLock);

	/* ask for a new relfilenode */
	newoid = GetNewRelFileNode(reltablespace, pg_class, relpersistence);

	/* Close pg_class and release the lock now */
	heap_close(pg_class, RowExclusiveLock);

	return newoid;
}

/* This function setup the "isExplain" flag for next hooks.
 * If this flag is setup, we can add hypothetical indexes.
 */
void
hypo_utility_hook(
#if PG_VERSION_NUM >= 100000
				  PlannedStmt *pstmt,
#else
				  Node *parsetree,
#endif
				  const char *queryString,
#if PG_VERSION_NUM >= 90300
				  ProcessUtilityContext context,
#endif
				  ParamListInfo params,
#if PG_VERSION_NUM >= 100000
				  QueryEnvironment *queryEnv,
#endif
#if PG_VERSION_NUM < 90300
				  bool isTopLevel,
#endif
				  DestReceiver *dest,
				  char *completionTag)
{
	hypoWalkerContext hypo_context = { 0 };

	hypo_query_walker(
#if PG_VERSION_NUM >= 100000
						    (Node *) pstmt,
#else
						    parsetree,
#endif
						    &hypo_context);

	isExplain = hypo_context.explain_found;

	/*
	 * Process pending invalidation.  For now, just do it if the current query
	 * might try to acess stored hypothetical objects
	 */
	if (isExplain && list_length(pending_invals) != 0)
		hypo_process_inval();

	if (prev_utility_hook)
		prev_utility_hook(
#if PG_VERSION_NUM >= 100000
						  pstmt,
#else
						  parsetree,
#endif
						  queryString,
#if PG_VERSION_NUM >= 90300
						  context,
#endif
						  params,
#if PG_VERSION_NUM >= 100000
						  queryEnv,
#endif
#if PG_VERSION_NUM < 90300
						  isTopLevel,
#endif
						  dest, completionTag);
	else
		standard_ProcessUtility(
#if PG_VERSION_NUM >= 100000
								pstmt,
#else
								parsetree,
#endif
								queryString,
#if PG_VERSION_NUM >= 90300
								context,
#endif
								params,
#if PG_VERSION_NUM >= 100000
						  queryEnv,
#endif
#if PG_VERSION_NUM < 90300
								isTopLevel,
#endif
								dest, completionTag);

}

/* Detect if the current utility command is compatible with hypothetical indexes
 * i.e. an EXPLAIN, no ANALYZE
 */
static bool
hypo_query_walker(Node *node, hypoWalkerContext *context)
{
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_PlannedStmt:
			{
				Node *stmt = ((PlannedStmt *) node)->utilityStmt;
				return query_or_expression_tree_walker(stmt, hypo_query_walker,
						context, QTW_IGNORE_RANGE_TABLE);
			}
		case T_ExplainStmt:
			{
				ExplainStmt *stmt = (ExplainStmt *) node;
				ListCell   *lc;

				foreach(lc, stmt->options)
				{
					DefElem    *opt = (DefElem *) lfirst(lc);

					if (strcmp(opt->defname, "analyze") == 0)
						return false;
				}

				context->explain_found = true;
#if PG_VERSION_NUM >= 100000
				/*
				 * No point in looking for unhandled command type if there are
				 * no hypothetical partitions
				 */
				if (!hypoTables)
					return true;

				return hypo_query_walker(stmt->query, context);
#else
				return true;
#endif
			}
			break;
#if PG_VERSION_NUM >= 100000
		case T_Query:
			{
				Query *query = (Query *) node;

				Assert(context->explain_found);

				if (context->explain_found &&
						(query->commandType == CMD_UPDATE ||
						 query->commandType == CMD_DELETE)
				)
				{
					RangeTblEntry *rte = rt_fetch(query->resultRelation,
							query->rtable);

					if (hypo_table_oid_is_hypothetical(rte->relid))
						elog(ERROR, "hypopg: UPDATE and DELETE on hypothetically"
								" partitioned tables are not supported");
				}

				if (query->cteList)
				{
					ListCell *lc;

					foreach(lc, query->cteList)
					{
						CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
						hypo_query_walker(cte->ctequery, context);
					}
				}
				return query_or_expression_tree_walker(node, hypo_query_walker,
						context, QTW_IGNORE_RANGE_TABLE);
			}
			break;
#endif
		default:
			return false;
	}

	return query_or_expression_tree_walker(node, hypo_query_walker, context,
			QTW_IGNORE_RANGE_TABLE);
}

/*
 * Callback for relcache inval message.  Detect if the given relid correspond
 * to something we should take care of.  For now, we only care of table being
 * dropped for which we have hypothetical partitioning information, thus
 * needing to remove relevant hypoTable entries.  At this point, we can't
 * detect if the inval message is due to table dropping or not, because any
 * cache access require a valid transaction, and we don't have a guarantee that
 * it's the case at this point.  Instead, maintain a deduplicated list of
 * interesting OID that will be processed before usage of hypothetical
 * partitioned object.
 */
static void
hypo_CacheRelCallback(Datum arg, Oid relid)
{
#if PG_VERSION_NUM >= 100000
	hypoTable *entry;

	entry = hypo_find_table(relid, true);
	if (entry)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
		pending_invals = list_append_unique_oid(pending_invals, relid);
		MemoryContextSwitchTo(oldcontext);
	}
#endif
}

/* Process any RelCache invalidation we previously received.  We have to
 * process them asynchronously, because we have to process it only if the
 * invalidation message was due to the original table being dropped.  We try to
 * detect this case by comparing the relid'd relname if it exists, and this
 * require a valid snapshot if may not be the case at the moment we receive the
 * inval message.
 */
void
hypo_process_inval(void)
{
#if PG_VERSION_NUM >= 100000
	ListCell *lc;

	Assert(IsTransactionState());

	/* XXX: remove this if support for hypothetical indexes is added */
	if (!hypoTables)
	{
		pending_invals = NIL;
		return;
	}

	if (pending_invals == NIL)
		return;

	foreach(lc, pending_invals)
	{
		Oid			relid = lfirst_oid(lc);
		hypoTable  *entry = hypo_find_table(relid, false);
		char   *relname = get_rel_name(relid);
		bool	found = false;

		/*
		 * The pending invalidations should be filtered and recorded after
		 * removing an entry, and should always be processed before any attempt
		 * to remove a hypothetical object, so we shoudl always find a
		 * hypoTable at this point.
		 */
		Assert(entry);

		if (!relname || (strcmp(relname, entry->tablename) != 0))
			found = hypo_table_remove(relid, NULL, true);

		if (found)
			elog(DEBUG1, "hypopg: hypo_process_inval removed table %s (%d)",
					relname, relid);
	}

	list_free(pending_invals);
	pending_invals = NIL;
#endif
}

/*
 * Clear all pending invalidations.  This is required when dropping all
 * hypoTable entries.
 */
void
hypo_clear_inval(void)
{
#if PG_VERSION_NUM >= 100000
	pending_invals = NIL;
#endif
}

/* Reset the isExplain flag after each query */
static void
hypo_executorEnd_hook(QueryDesc *queryDesc)
{
	isExplain = false;

	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * This function will execute hypo_injectHypotheticalIndex() or
 * hypo_injectHypotheticalPartitioning() for every matching
 * hypothetical entry that was previously declared,  if the isExplain flag is
 * setup.
 */
static void
hypo_get_relation_info_hook(PlannerInfo *root,
			    Oid relationObjectId,
			    bool inhparent,
			    RelOptInfo *rel)
{
	Relation	relation;
#if PG_VERSION_NUM >= 100000
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	bool  hypopart = false;
#endif

	if (HYPO_ENABLED())
	{

#if PG_VERSION_NUM >= 100000
		hypopart = hypo_table_oid_is_hypothetical(relationObjectId);
		/*
		 * If this relation is table we want to partition hypothetical,
		 * inject hypothetical partitioning
		 */
		if (hypopart)
			hypo_injectHypotheticalPartitioning(root, relationObjectId, rel);
#endif

		/* Open the current relation */
		relation = heap_open(relationObjectId, AccessShareLock);

		if (relation->rd_rel->relkind == RELKIND_RELATION
#if PG_VERSION_NUM >= 90300
			|| relation->rd_rel->relkind == RELKIND_MATVIEW
#endif
			)
		{
			ListCell  *lc;
			Oid  parentId = relationObjectId;

#if PG_VERSION_NUM >= 100000
			/*
			 * If this rel is a partition, get root table oid to look for
			 * hypothetical indexes.
			 */
			if (rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
			{
				if (!hypopart)
				{
					/*
					 * when this is a real partition, we have to search root
					 * table from PlannerInfo to get root table oid.  when this
					 * is a hypothetical partition, root table oid is equal to
					 * relationObjectId, so nothing to do
					 */
					AppendRelInfo *appinfo;
					RelOptInfo *parentrel = rel;
					do
					{
#if PG_VERSION_NUM >= 110000
						appinfo = root->append_rel_array[parentrel->relid];
#else
						appinfo = find_childrel_appendrelinfo(root, parentrel);
#endif		/* pg10 only */
						parentrel = find_base_rel(root, appinfo->parent_relid);
					} while (parentrel->reloptkind == RELOPT_OTHER_MEMBER_REL);
					parentId = appinfo->parent_reloid;
				}
			}
#endif
			foreach(lc, hypoIndexes)
			{
				hypoIndex  *entry = (hypoIndex *) lfirst(lc);
				bool		oid = InvalidOid;

				/*
				 * check for hypothetical index on regular table or
				 * hypothetical index on root partitioning tree
				 */
				if (entry->relid == parentId
#if PG_VERSION_NUM >= 110000
					&& !rel->part_scheme
#endif
				)
				{
					oid = parentId;
				}
#if PG_VERSION_NUM >= 100000
				/* check for hypothetical index on real partition */
				else if (entry->relid == relationObjectId)
				{
					oid = relationObjectId;
				}
				/*
				 * check for hypothetical index on hypothetical leaf partition
				 */
				else if (hypo_table_oid_is_hypothetical(relationObjectId) &&
						HYPO_TABLE_RTE_HAS_HYPOOID(rte) &&
						entry->relid == HYPO_TABLE_RTE_GET_HYPOOID(rte)
				)
				{
					oid = HYPO_TABLE_RTE_GET_HYPOOID(rte);
				}
#endif
				/*
				 * hypothetical index found, add it to the relation's
				 * indextlist
				 */
				if (OidIsValid(oid))
					hypo_injectHypotheticalIndex(root, oid,
												 inhparent, rel, relation, entry);
			}
		}
		/* Close the relation and keep the lock, it might be reopened later */
		heap_close(relation, NoLock);
	}
	if (prev_get_relation_info_hook)
		prev_get_relation_info_hook(root, relationObjectId, inhparent, rel);
}

static bool
hypo_get_relation_stats_hook(PlannerInfo *root,
		RangeTblEntry *rte,
		AttrNumber attnum,
		VariableStatData *vardata)
{
#if PG_VERSION_NUM < 100000
	return false;
#else
	Oid poid = InvalidOid;
	hypoStatsKey key;
	hypoStatsEntry *entry;
	bool found;

	/* Nothing to do if it's not a plain relation */
	if (rte->rtekind != RTE_RELATION)
		return false;

	/*
	 * If this is a root table hypothetically partitioned, we have to retrieve
	 * the pg_statistic row ourselves, even if no hypopg_analyze has been
	 * performed yet, because postgres will search for an entry with stainherit
	 * = true, which won't exist.
	 */
	if (HYPO_RTE_IS_TAGGED(rte) && (!HYPO_TABLE_RTE_HAS_HYPOOID(rte)))
	{
		vardata->statsTuple = SearchSysCache3(STATRELATTINH,
											  ObjectIdGetDatum(rte->relid),
											  Int16GetDatum(attnum),
											  BoolGetDatum(false));
		vardata->freefunc = ReleaseSysCache;

		if (HeapTupleIsValid(vardata->statsTuple))
		{
			/* check if user has permission to read this column */
			vardata->acl_ok =
				(pg_class_aclcheck(rte->relid, GetUserId(),
								   ACL_SELECT) == ACLCHECK_OK) ||
				(pg_attribute_aclcheck(rte->relid, attnum, GetUserId(),
									   ACL_SELECT) == ACLCHECK_OK);
		}
		else
		{
			/* suppress any possible leakproofness checks later */
			vardata->acl_ok = true;
		}

		return true;
	}

	/* Fast exit if the local hash hasn't been created yet */
	if (!hypoStatsHash)
		return false;

	/* Nothing to do if it's not a hypothetical partition */
	if (!HYPO_TABLE_RTE_HAS_HYPOOID(rte))
		return false;

	/* At this point, we have a hypothetical partition.  Get its oid */
	poid = HYPO_TABLE_RTE_GET_HYPOOID(rte);
	if (poid == InvalidOid)
		/* This should not happen */
		return false;

	/* Retrieve the pg_statistic stored row */
	memset(&key, 0, sizeof(hypoStatsKey));
	key.relid = poid;
	key.attnum = attnum;
	entry = hash_search(hypoStatsHash, &key, HASH_FIND, &found);

	/* XXX should we warn about possible very bad estimation? */
	if (!found)
		return false;

	vardata->statsTuple = heap_copytuple(entry->statsTuple);
	vardata->freefunc = (void *) pfree;

	return true;
#endif
}

#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
/*
 * if this child relation is excluded by constraints, call set_dummy_rel_pathlist
 */
static void
hypo_set_rel_pathlist_hook(PlannerInfo *root,
						   RelOptInfo *rel,
						   Index rti,
						   RangeTblEntry *rte)
{
	if(HYPO_ENABLED() && hypo_table_oid_is_hypothetical(rte->relid) && rte->relkind == 'r')
		hypo_markDummyIfExcluded(root,rel,rti,rte);

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);
}
#endif

/*
 * Reset all stored entries.
 */
Datum
hypopg_reset(PG_FUNCTION_ARGS)
{
	hypo_index_reset();
#if PG_VERSION_NUM >= 100000
	hypo_table_reset();
#endif
	PG_RETURN_VOID();
}
