/*-------------------------------------------------------------------------
 *
 * hypopg_import_analyze.h: Import of some PostgreSQL private fuctions, used
 *                          for hypothetical analyze.
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 * Copyright (c) 2008-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef _HYPOPG_IMPORT_ANALYZE_H_
#define _HYPOPG_IMPORT_ANALYZE_H_

#if PG_VERSION_NUM < 100000
#error "This could should only be included on pg10+ code"
#endif


VacAttrStats *examine_attribute(Relation onerel, int attnum, Node *index_expr);
Datum std_fetch_func(VacAttrStatsP stats, int rownum, bool *isNull);

#endif		/* _HYPOPG_IMPORT_ANALYZE_H_ */
