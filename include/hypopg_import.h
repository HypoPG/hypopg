/*-------------------------------------------------------------------------
 *
 * hypopg_import.h: Import of some PostgreSQL private fuctions.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (c) 2008-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef _HYPOPG_IMPORT_H_
#define _HYPOPG_IMPORT_H_

#include "commands/vacuum.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "optimizer/pathnode.h"
#if PG_VERSION_NUM >= 110000
#include "partitioning/partbounds.h"
#endif
#include "utils/rel.h"


/* adapted from nbtinsert.h */
#define HYPO_BTMaxItemSize \
	MAXALIGN_DOWN((BLCKSZ - \
				MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData)) - \
				MAXALIGN(sizeof(BTPageOpaqueData))) / 3)

extern List *build_index_tlist(PlannerInfo *root, IndexOptInfo *index,
				  Relation heapRelation);
extern Oid GetIndexOpClass(List *opclass, Oid attrType,
				char *accessMethodName, Oid accessMethodId);

extern void CheckPredicate(Expr *predicate);
extern bool CheckMutability(Expr *expr);
#if PG_VERSION_NUM < 90500
extern char *get_am_name(Oid amOid);
#endif
extern void get_opclass_name(Oid opclass, Oid actual_datatype, StringInfo buf);

#if PG_VERSION_NUM >= 100000

/* pg10 only imports */
#if PG_VERSION_NUM < 110000
/*
 * Imported from src/backend/catalog/partition.c, not exported
 */
typedef struct PartitionBoundInfoData
{
	char		strategy;		/* hash, list or range? */
	int			ndatums;		/* Length of the datums following array */
	Datum	  **datums;
	PartitionRangeDatumKind **kind; /* The kind of each range bound datum;
									 * NULL for hash and list partitioned
									 * tables */
	int		   *indexes;		/* Partition indexes */
	int			null_index;		/* Index of the null-accepting partition; -1
								 * if there isn't one */
} PartitionBoundInfoData;

/* One bound of a hash partition */
typedef struct PartitionHashBound
{
	int			modulus;
	int			remainder;
	int			index;
} PartitionHashBound;

/* One value coming from some (index'th) list partition */
typedef struct PartitionListValue
{
	int			index;
	Datum		value;
} PartitionListValue;

/* One bound of a range partition */
typedef struct PartitionRangeBound
{
	int			index;
	Datum	   *datums;			/* range bound datums */
	PartitionRangeDatumKind *kind;	/* the kind of each datum */
	bool		lower;			/* this is the lower (vs upper) bound */
} PartitionRangeBound;

/*
 * Entry of a hash table used in find_all_inheritors. See below.
 */
typedef struct SeenRelsEntry
{
	Oid         rel_id;         /* relation oid */
	ListCell   *numparents_cell;    /* corresponding list cell */
} SeenRelsEntry;

PartitionRangeBound *make_one_range_bound(PartitionKey key, int index,
		List *datums, bool lower);
int partition_bound_bsearch(PartitionKey key, PartitionBoundInfo boundinfo,
						void *probe, bool probe_is_bound, bool *is_equal);
int32 partition_bound_cmp(PartitionKey key,
					PartitionBoundInfo boundinfo,
					int offset, void *probe, bool probe_is_bound);

int32 partition_rbound_cmp(PartitionKey key,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2);
#endif		/* pg10 only imports */

/* Context info needed for invoking a recursive querytree display routine */
typedef struct
{
	StringInfo	buf;			/* output buffer to append to */
	// List	   *namespaces;		/* List of deparse_namespace nodes */
	// List	   *windowClause;	/* Current query level's WINDOW clause */
	// List	   *windowTList;	/* targetlist for resolving WINDOW clause */
	// int			prettyFlags;	/* enabling of pretty-print functions */
	// int			wrapColumn;		/* max line length, or -1 for no limit */
	// int			indentLevel;	/* current indent level for prettyprint */
	// bool		varprefix;		/* true to print prefixes on Vars */
	// ParseExprKind special_exprkind; /* set only for exprkinds needing special
	// 								 * handling */
} deparse_context;


PartitionSpec *transformPartitionSpec(Relation rel, PartitionSpec *partspec,
		char *strategy);
void ComputePartitionAttrs(Relation rel, List *partParams, AttrNumber
		*partattrs, List **partexprs, Oid *partopclass, Oid *partcollation,
		char strategy);
char *get_relation_name(Oid relid);
bool looks_like_function(Node *node);
int32 qsort_partition_hbound_cmp(const void *a, const void *b);
int32 qsort_partition_list_value_cmp(const void *a, const void *b, void *arg);
int32 qsort_partition_rbound_cmp(const void *a, const void *b, void *arg);
void get_const_expr(Const *constval, deparse_context *context, int
		showtype);
void get_const_collation(Const *constval, deparse_context *context);
void simple_quote_literal(StringInfo buf, const char *val);
Const *transformPartitionBoundValue(ParseState *pstate, A_Const *con,
		const char *colName, Oid colType, int32 colTypmod);
void validateInfiniteBounds(ParseState *pstate, List *blist);
Oid get_partition_operator(PartitionKey key, int col, StrategyNumber strategy,
			   bool *need_relabel);
Expr *make_partition_op_expr(PartitionKey key, int keynum,
			     uint16 strategy, Expr *arg1, Expr *arg2);
void get_range_key_properties(PartitionKey key, int keynum,
			      PartitionRangeDatum *ldatum,
			      PartitionRangeDatum *udatum,
			      ListCell **partexprs_item,
			      Expr **keyCol,
			      Const **lower_val, Const **upper_val);
List *get_range_nulltest(PartitionKey key);
void make_inh_translation_list(Relation oldrelation, Relation newrelation,
			       Index newvarno,
			       List **translated_vars);
VacAttrStats *examine_attribute(Relation onerel, int attnum, Node *index_expr);
Datum std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);
/* Copied from src/backend/catalog/partition.c, not exported */
#define partition_bound_accepts_nulls(bi) ((bi)->null_index != -1)

#endif
#endif
