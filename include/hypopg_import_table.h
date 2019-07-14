/*-------------------------------------------------------------------------
 *
 * hypopg_import_table.h: Import of some PostgreSQL private fuctions, used
 *                          for hypothetical partitioning.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (c) 2008-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef _HYPOPG_IMPORT_TABLE_H_
#define _HYPOPG_IMPORT_TABLE_H_

#include "catalog/partition.h"

#if PG_VERSION_NUM < 100000
#error "This could should only be included on pg10+ code"
#endif

/* pg10 and pg12+ imports */
#if PG_VERSION_NUM < 110000 || PG_VERSION_NUM >= 120000

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

#endif		/* pg10 and pg12+ imports */

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
int32 partition_rbound_cmp(PartitionKey key,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2);
#endif

/* pg12+ only imports */
#if PG_VERSION_NUM >= 120000
extern PartitionRangeBound *
make_one_partition_rbound(PartitionKey key, int index, List *datums, bool lower);
extern int partition_range_bsearch(int partnatts, FmgrInfo *partsupfunc,
						Oid *partcollation,
						PartitionBoundInfo boundinfo,
						PartitionRangeBound *probe, bool *is_equal);
extern int32
partition_hbound_cmp(int modulus1, int remainder1, int modulus2, int remainder2);
extern int32
partition_rbound_cmp(int partnatts, FmgrInfo *partsupfunc,
					 Oid *partcollation,
					 Datum *datums1, PartitionRangeDatumKind *kind1,
					 bool lower1, PartitionRangeBound *b2);
#endif
/* end of pg12+ only imports */

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
/* Copied from src/backend/catalog/partition.c, not exported */
#define partition_bound_accepts_nulls(bi) ((bi)->null_index != -1)

#endif		/* _HYPOPG_IMPORT_TABLE_H_ */
