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

#include "nodes/pg_list.h"
#include "optimizer/planner.h"
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
	int			default_index;	/* Index of the default partition; -1 if there
								 * isn't one */
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
int32 partition_hbound_cmp(int modulus1, int remainder1, int modulus2, int
		remainder2);
int32 qsort_partition_list_value_cmp(const void *a, const void *b, void *arg);
PartitionRangeBound *make_one_range_bound(PartitionKey key, int index, List
		*datums, bool lower);
int32 qsort_partition_rbound_cmp(const void *a, const void *b, void *arg);
int32 partition_rbound_cmp(PartitionKey key,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2);
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

/* Copied from src/backend/catalog/partition.c, not exported */
#define partition_bound_accepts_nulls(bi) ((bi)->null_index != -1)

#endif
#endif
