/*-------------------------------------------------------------------------
 *
 * hypopg_analyze.c: Bounded sampled statistics for hypothetical indexes
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>

#include "catalog/namespace.h"
#include "access/detoast.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#if PG_VERSION_NUM >= 90500
#include "utils/ruleutils.h"
#endif

#include "include/hypopg.h"
#include "include/hypopg_analyze.h"

#define HYPO_ANALYZE_DEFAULT_FRACTION 1.0f
#define HYPO_ANALYZE_MAX_ROWS 1024
#define HYPO_ANALYZE_MIN_ROWS 32

typedef struct HypoAnalyzeRelation
{
	Oid			relid;
	float4		fraction;
	int			natts;
	int			rows;
	HypoAnalyzeStats *attrs;
} HypoAnalyzeRelation;

typedef struct HypoAnalyzeExpression
{
	Oid			relid;
	float4		fraction;
	char	   *expression;
	HypoAnalyzeStats stats;
} HypoAnalyzeExpression;

static List *hypoAnalyzeRelations = NIL;
static List *hypoAnalyzeExpressions = NIL;

static bool hypo_analyze_sample(Relation relation, const char *target,
								float4 fraction,
								HypoAnalyzeValueCallback callback, void *arg,
								HypoAnalyzeStats *stats);
static bool hypo_analyze_relation_internal(Relation relation, float4 fraction,
										 HypoAnalyzeRelation **result);
static int hypo_analyze_datum_width(Datum value, Form_pg_attribute attr);
static bool hypo_analyze_fraction(float4 fraction);

PG_FUNCTION_INFO_V1(hypopg_analyze);

void
hypo_analyze_reset(void)
{
	ListCell   *lc;

	foreach(lc, hypoAnalyzeRelations)
	{
		HypoAnalyzeRelation *entry = (HypoAnalyzeRelation *) lfirst(lc);

		pfree(entry->attrs);
		pfree(entry);
	}
	foreach(lc, hypoAnalyzeExpressions)
	{
		HypoAnalyzeExpression *entry = (HypoAnalyzeExpression *) lfirst(lc);

		pfree(entry->expression);
		pfree(entry);
	}
	list_free(hypoAnalyzeRelations);
	list_free(hypoAnalyzeExpressions);
	hypoAnalyzeRelations = NIL;
	hypoAnalyzeExpressions = NIL;
}

static bool
hypo_analyze_fraction(float4 fraction)
{
	return isfinite(fraction) && fraction > 0.0f && fraction <= 100.0f;
}

static int
hypo_analyze_datum_width(Datum value, Form_pg_attribute attr)
{
	Size		width;

	if (attr->attlen > 0)
		return attr->attlen;
	if (attr->attlen == -2)
		return strlen(DatumGetCString(value)) + 1;

	width = toast_raw_datum_size(value);
	return Min(width, (Size) PG_INT32_MAX);
}

static bool
hypo_analyze_sample(Relation relation, const char *target, float4 fraction,
					HypoAnalyzeValueCallback callback, void *arg,
					HypoAnalyzeStats *stats)
{
#if PG_VERSION_NUM < 90500
	(void) relation;
	(void) target;
	(void) fraction;
	(void) callback;
	(void) arg;
	(void) stats;
	return false;
#else
	StringInfoData query;
	TupleDesc	tupdesc;
	Form_pg_attribute attr;
	MemoryContext oldcontext;
	Oid		save_userid;
	int		save_sec_context;
	int		save_nestlevel;
	bool		save_enabled;
	bool		connected = false;
	int			ret;
	uint64		nullrows = 0;
	uint64		width = 0;
	uint64		row;

	if (!hypo_analyze_fraction(fraction))
		return false;

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT %s FROM %s.%s TABLESAMPLE SYSTEM (%.6g) LIMIT %d",
					 target,
					 quote_identifier(get_namespace_name(RelationGetNamespace(relation))),
					 quote_identifier(RelationGetRelationName(relation)),
					 fraction, HYPO_ANALYZE_MAX_ROWS);

	stats->rows = 0;
	stats->width = 0;
	stats->nullfrac = 0.0f;
	oldcontext = MemoryContextSwitchTo(CurrentMemoryContext);
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	save_nestlevel = NewGUCNestLevel();
	save_enabled = hypo_is_enabled;

	PG_TRY();
	{
		SetUserIdAndSecContext(RelationGetForm(relation)->relowner,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		RestrictSearchPath();
		hypo_is_enabled = false;

		ret = SPI_connect();
		if (ret != SPI_OK_CONNECT)
			ereport(ERROR,
					(errmsg("hypopg: SPI connect failed while sampling relation")));
		connected = true;

		ret = SPI_execute(query.data, true, HYPO_ANALYZE_MAX_ROWS);
		if (ret != SPI_OK_SELECT || SPI_tuptable == NULL)
			ereport(ERROR,
					(errmsg("hypopg: TABLESAMPLE query failed while estimating index size")));

		tupdesc = SPI_tuptable->tupdesc;
		attr = TupleDescAttr(tupdesc, 0);
		for (row = 0; row < SPI_processed; row++)
		{
			bool		isnull;
			Datum		value;

			value = SPI_getbinval(SPI_tuptable->vals[row], tupdesc, 1,
								  &isnull);
			if (isnull)
				nullrows++;
			else
				width += hypo_analyze_datum_width(value, attr);
			if (callback != NULL)
				callback(value, isnull, attr->atttypid, attr->attcollation, arg);
		}

		stats->rows = SPI_processed;
		if (stats->rows > 0)
		{
			stats->width = (int32) Min(width / stats->rows,
										 (uint64) PG_INT32_MAX);
			stats->nullfrac = (float4) nullrows / stats->rows;
		}

		SPI_finish();
		connected = false;
	}
	PG_CATCH();
	{
		ErrorData *edata;

		if (connected)
			SPI_finish();
		AtEOXact_GUC(false, save_nestlevel);
		SetUserIdAndSecContext(save_userid, save_sec_context);
		hypo_is_enabled = save_enabled;
		MemoryContextSwitchTo(oldcontext);

		edata = CopyErrorData();
		FlushErrorState();
		elog(DEBUG1, "hypopg: sampled-size fallback unavailable: %s",
			 edata->message);
		FreeErrorData(edata);
		return false;
	}
	PG_END_TRY();

	AtEOXact_GUC(false, save_nestlevel);
	SetUserIdAndSecContext(save_userid, save_sec_context);
	hypo_is_enabled = save_enabled;
	MemoryContextSwitchTo(oldcontext);

	return stats->rows >= HYPO_ANALYZE_MIN_ROWS && stats->width > 0;
#endif
}

static bool
hypo_analyze_relation_internal(Relation relation, float4 fraction,
								HypoAnalyzeRelation **result)
{
	HypoAnalyzeRelation *entry;
	TupleDesc		tupdesc;
	StringInfoData query;
	int				attno;
	int				row;
	uint64			*widths;
	uint64			nulls[MaxHeapAttributeNumber];
	MemoryContext oldcontext;

	{
		ListCell   *lc;

		foreach(lc, hypoAnalyzeRelations)
		{
			entry = (HypoAnalyzeRelation *) lfirst(lc);

			if (entry->relid == RelationGetRelid(relation) &&
				entry->fraction == fraction)
			{
				*result = entry;
				return true;
			}
		}
	}

	entry = MemoryContextAllocZero(HypoMemoryContext,
									  sizeof(HypoAnalyzeRelation));
	entry->relid = RelationGetRelid(relation);
	entry->fraction = fraction;
	entry->natts = RelationGetNumberOfAttributes(relation);
	entry->attrs = MemoryContextAllocZero(HypoMemoryContext,
										 entry->natts * sizeof(HypoAnalyzeStats));
	widths = palloc0(entry->natts * sizeof(uint64));
	memset(nulls, 0, sizeof(nulls));

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT * FROM %s.%s TABLESAMPLE SYSTEM (%.6g) LIMIT %d",
					 quote_identifier(get_namespace_name(RelationGetNamespace(relation))),
					 quote_identifier(RelationGetRelationName(relation)),
					 fraction, HYPO_ANALYZE_MAX_ROWS);

#if PG_VERSION_NUM < 90500
	pfree(widths);
	pfree(query.data);
	return false;
#else
	{
		bool		connected = false;
		bool		save_enabled = hypo_is_enabled;
		Oid		save_userid;
		int		save_sec_context;
		int		save_nestlevel;
		MemoryContext oldcontext = MemoryContextSwitchTo(CurrentMemoryContext);
		int			ret;

		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		save_nestlevel = NewGUCNestLevel();
		PG_TRY();
		{
			SetUserIdAndSecContext(RelationGetForm(relation)->relowner,
								   save_sec_context | SECURITY_RESTRICTED_OPERATION);
			RestrictSearchPath();
			hypo_is_enabled = false;
			ret = SPI_connect();
			if (ret != SPI_OK_CONNECT)
				ereport(ERROR, (errmsg("hypopg: SPI connect failed while sampling relation")));
			connected = true;
			ret = SPI_execute(query.data, true, HYPO_ANALYZE_MAX_ROWS);
			if (ret != SPI_OK_SELECT || SPI_tuptable == NULL)
				ereport(ERROR, (errmsg("hypopg: TABLESAMPLE query failed while sampling relation")));

			tupdesc = SPI_tuptable->tupdesc;
			for (row = 0; row < SPI_processed; row++)
			{
				for (attno = 0; attno < entry->natts; attno++)
				{
					bool		isnull;
					Datum		value = SPI_getbinval(SPI_tuptable->vals[row],
												 tupdesc, attno + 1, &isnull);

					if (isnull)
						nulls[attno]++;
					else
						widths[attno] += hypo_analyze_datum_width(value,
							TupleDescAttr(tupdesc, attno));
				}
			}
			entry->rows = SPI_processed;
			SPI_finish();
			connected = false;
		}
		PG_CATCH();
		{
			ErrorData *edata;

			if (connected)
				SPI_finish();
			AtEOXact_GUC(false, save_nestlevel);
			SetUserIdAndSecContext(save_userid, save_sec_context);
			hypo_is_enabled = save_enabled;
			MemoryContextSwitchTo(oldcontext);
			pfree(widths);
			pfree(query.data);
			pfree(entry->attrs);
			pfree(entry);
			edata = CopyErrorData();
			FlushErrorState();
			elog(DEBUG1, "hypopg: sampled relation unavailable: %s", edata->message);
			FreeErrorData(edata);
			return false;
		}
		PG_END_TRY();

		AtEOXact_GUC(false, save_nestlevel);
		SetUserIdAndSecContext(save_userid, save_sec_context);
		hypo_is_enabled = save_enabled;
		MemoryContextSwitchTo(oldcontext);
	}
#endif

	for (attno = 0; attno < entry->natts; attno++)
	{
		if (entry->rows > 0)
		{
			entry->attrs[attno].rows = entry->rows;
			entry->attrs[attno].width = (int32) Min(widths[attno] / entry->rows,
											(uint64) PG_INT32_MAX);
			entry->attrs[attno].nullfrac = (float4) nulls[attno] / entry->rows;
		}
	}

	pfree(widths);
	pfree(query.data);

	if (entry->rows < HYPO_ANALYZE_MIN_ROWS)
	{
		pfree(entry->attrs);
		pfree(entry);
		return false;
	}

	oldcontext = MemoryContextSwitchTo(HypoMemoryContext);
	hypoAnalyzeRelations = lappend(hypoAnalyzeRelations, entry);
	MemoryContextSwitchTo(oldcontext);
	*result = entry;
	return true;
}

bool
hypo_analyze_relation(Oid relid, float4 fraction, HypoAnalyzeStats *stats)
{
	Relation	relation;
	HypoAnalyzeRelation *entry;

	if (!hypo_analyze_fraction(fraction))
		return false;

	relation = table_open(relid, AccessShareLock);
	if (!hypo_analyze_relation_internal(relation, fraction, &entry))
	{
		table_close(relation, AccessShareLock);
		return false;
	}
	*stats = entry->attrs[0];
	table_close(relation, AccessShareLock);
	return true;
}

bool
hypo_analyze_attribute(Oid relid, AttrNumber attnum, float4 fraction,
						HypoAnalyzeStats *stats)
{
	Relation	relation;
	HypoAnalyzeRelation *entry;

	if (!hypo_analyze_fraction(fraction) || attnum <= 0)
		return false;

	relation = table_open(relid, AccessShareLock);
	if (attnum > RelationGetNumberOfAttributes(relation) ||
		!hypo_analyze_relation_internal(relation, fraction, &entry))
	{
		table_close(relation, AccessShareLock);
		return false;
	}
	*stats = entry->attrs[attnum - 1];
	table_close(relation, AccessShareLock);
	return stats->rows >= HYPO_ANALYZE_MIN_ROWS && stats->width > 0;
}

bool
hypo_analyze_attribute_callback(Oid relid, AttrNumber attnum, float4 fraction,
								 HypoAnalyzeValueCallback callback, void *arg,
								 HypoAnalyzeStats *stats)
{
#if PG_VERSION_NUM < 90500
	(void) relid;
	(void) attnum;
	(void) fraction;
	(void) callback;
	(void) arg;
	(void) stats;
	return false;
#else
	Relation		 relation;
	Form_pg_attribute attr;
	const char		*target;
	bool				 result;

	if (callback == NULL || !hypo_analyze_fraction(fraction) || attnum <= 0)
		return false;

	relation = table_open(relid, AccessShareLock);
	if (attnum > RelationGetNumberOfAttributes(relation))
	{
		table_close(relation, AccessShareLock);
		return false;
	}

	attr = TupleDescAttr(relation->rd_att, attnum - 1);
	if (attr->attisdropped)
	{
		table_close(relation, AccessShareLock);
		return false;
	}

	target = quote_identifier(NameStr(attr->attname));
	result = hypo_analyze_sample(relation, target, fraction, callback, arg,
								 stats);
	table_close(relation, AccessShareLock);
	return result;
#endif
}

bool
hypo_analyze_expression(Oid relid, Node *expr, float4 fraction,
						HypoAnalyzeStats *stats)
{
#if PG_VERSION_NUM < 90500
	(void) relid;
	(void) expr;
	(void) fraction;
	(void) stats;
	return false;
#else
	Relation	relation;
	List	   *context;
	char	   *deparsed;
	HypoAnalyzeExpression *cached;

	if (!hypo_analyze_fraction(fraction))
		return false;

	relation = table_open(relid, AccessShareLock);
	context = deparse_context_for(RelationGetRelationName(relation), relid);
	deparsed = deparse_expression(expr, context, false, false);
	{
		ListCell   *lc;

		foreach(lc, hypoAnalyzeExpressions)
		{
			cached = (HypoAnalyzeExpression *) lfirst(lc);

			if (cached->relid == relid && cached->fraction == fraction &&
				strcmp(cached->expression, deparsed) == 0)
			{
				*stats = cached->stats;
				pfree(deparsed);
				table_close(relation, AccessShareLock);
				return stats->rows >= HYPO_ANALYZE_MIN_ROWS && stats->width > 0;
			}
		}
	}

	if (!hypo_analyze_sample(relation, deparsed, fraction, NULL, NULL,
							 stats))
	{
		table_close(relation, AccessShareLock);
		return false;
	}

	cached = MemoryContextAllocZero(HypoMemoryContext,
									 sizeof(HypoAnalyzeExpression));
	cached->relid = relid;
	cached->fraction = fraction;
	cached->expression = MemoryContextStrdup(HypoMemoryContext, deparsed);
	cached->stats = *stats;
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(HypoMemoryContext);

		hypoAnalyzeExpressions = lappend(hypoAnalyzeExpressions, cached);
		MemoryContextSwitchTo(oldcontext);
	}
	pfree(deparsed);
	table_close(relation, AccessShareLock);
	return true;
#endif
}

Datum
hypopg_analyze(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	float4		fraction = PG_ARGISNULL(1) ?
			HYPO_ANALYZE_DEFAULT_FRACTION : PG_GETARG_FLOAT4(1);
	Relation	relation;
	HypoAnalyzeRelation *entry;

	if (!hypo_analyze_fraction(fraction))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hypopg: sample fraction must be greater than 0 and at most 100")));

	relation = table_open(relid, AccessShareLock);
	if (!hypo_analyze_relation_internal(relation, fraction, &entry))
	{
		table_close(relation, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hypopg: could not collect at least %d sampled rows",
						HYPO_ANALYZE_MIN_ROWS)));
	}
	table_close(relation, AccessShareLock);

	PG_RETURN_VOID();
}
