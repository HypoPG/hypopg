/*-------------------------------------------------------------------------
 *
 * hypopg_analyze.h: Generic sampled statistics for hypothetical indexes
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 *-------------------------------------------------------------------------
 */
#ifndef _HYPOPG_ANALYZE_H_
#define _HYPOPG_ANALYZE_H_

#include "nodes/nodes.h"

#define HYPO_ANALYZE_DEFAULT_FRACTION 1.0f

typedef struct HypoAnalyzeStats
{
	int			rows;
	int32		width;
	float4		nullfrac;
} HypoAnalyzeStats;

void		hypo_analyze_reset(void);
bool		hypo_analyze_relation(Oid relid, float4 fraction,
							HypoAnalyzeStats *stats);
bool		hypo_analyze_attribute(Oid relid, AttrNumber attnum,
							float4 fraction, HypoAnalyzeStats *stats);
bool		hypo_analyze_expression(Oid relid, Node *expr,
							float4 fraction, HypoAnalyzeStats *stats);
PGDLLEXPORT Datum hypopg_analyze(PG_FUNCTION_ARGS);

#endif							/* _HYPOPG_ANALYZE_H_ */
