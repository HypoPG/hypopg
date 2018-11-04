-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2015-2018: Julien Rouhaud

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hypopg" to load this file. \quit

SET client_encoding = 'UTF8';

-- General functions
--

CREATE FUNCTION hypopg_reset()
    RETURNS void
    LANGUAGE C VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_reset';

-- Hypothetical indexes related functions
--

CREATE FUNCTION hypopg_reset_index()
    RETURNS void
    LANGUAGE C VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_reset_index';

CREATE FUNCTION
hypopg_create_index(IN sql_order text, OUT indexrelid oid, OUT indexname text)
    RETURNS SETOF record
    LANGUAGE C STRICT VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_create_index';

CREATE FUNCTION
hypopg_drop_index(IN indexid oid)
    RETURNS bool
    LANGUAGE C STRICT VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_drop_index';

CREATE FUNCTION hypopg(OUT indexname text, OUT indexrelid oid,
                       OUT indrelid oid, OUT innatts integer,
                       OUT indisunique boolean, OUT indkey int2vector,
                       OUT indcollation oidvector, OUT indclass oidvector,
                       OUT indoption oidvector, OUT indexprs pg_node_tree,
                       OUT indpred pg_node_tree, OUT amid oid)
    RETURNS SETOF record
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg';

CREATE FUNCTION hypopg_list_indexes(OUT indexrelid oid, OUT indexname text, OUT nspname name, OUT relname name, OUT amname name)
    RETURNS SETOF record
AS
$_$
    SELECT h.indexrelid, h.indexname, n.nspname, c.relname, am.amname
    FROM hypopg() h
    JOIN pg_class c ON c.oid = h.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = h.amid
$_$
LANGUAGE sql;

CREATE FUNCTION
hypopg_relation_size(IN indexid oid)
    RETURNS bigint
LANGUAGE C STRICT VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_relation_size';

CREATE FUNCTION
hypopg_get_indexdef(IN indexid oid)
    RETURNS text
LANGUAGE C STRICT VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_get_indexdef';

-- Hypothetical partitioning related functions
--

CREATE FUNCTION
hypopg_add_partition(IN partition_name name, IN partition_of_clause text,
    IN partition_by_clause text DEFAULT NULL,
    OUT relid oid, OUT tablename text)
    RETURNS SETOF record
    LANGUAGE C VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_add_partition';

CREATE FUNCTION
hypopg_partition_table(IN tablename regclass, IN partition_by_clause text)
    RETURNS bool
    LANGUAGE C STRICT VOLATILE COSt 100
AS '$libdir/hypopg', 'hypopg_partition_table';

CREATE FUNCTION hypopg_reset_table()
    RETURNS void
    LANGUAGE C VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_reset_table';

CREATE FUNCTION hypopg_drop_table(IN relid oid)
    RETURNS void
    LANGUAGE C VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_drop_table';

CREATE FUNCTION hypopg_table(OUT relid oid, OUT tablename text,
    OUT parentid oid, OUT rootid oid,
    OUT partition_by_clause text, OUT partition_bounds text)
    RETURNS SETOF record
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg_table';

CREATE FUNCTION hypopg_analyze(IN tablename regclass, IN fraction real = 1)
    RETURNS void
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg_analyze';

CREATE FUNCTION hypopg_statistic()
    RETURNS SETOF pg_catalog.pg_statistic
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg_statistic';

-- The original anyarray columns must be casted to text, because it's not
-- allowed to create a column of such a type.
DO
$_$
DECLARE
  v_has_rls bool;
BEGIN
    SELECT COUNT(*) = 1 INTO v_has_rls
    FROM pg_class c
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE c.relname = 'pg_class'
    AND a.attname = 'relrowsecurity';

    IF v_has_rls THEN
        CREATE VIEW hypopg_stats
        WITH (security_barrier = true)
        AS
        SELECT n.nspname AS schemaname,
            t.tablename AS tablename,
            a.attname,
            s.stainherit AS inherited,
            s.stanullfrac AS null_frac,
            s.stawidth AS avg_width,
            s.stadistinct AS n_distinct,
                CASE
                    WHEN s.stakind1 = 1 THEN s.stavalues1::text
                    WHEN s.stakind2 = 1 THEN s.stavalues2::text
                    WHEN s.stakind3 = 1 THEN s.stavalues3::text
                    WHEN s.stakind4 = 1 THEN s.stavalues4::text
                    WHEN s.stakind5 = 1 THEN s.stavalues5::text
                    ELSE NULL::text
                END AS most_common_vals,
                CASE
                    WHEN s.stakind1 = 1 THEN s.stanumbers1
                    WHEN s.stakind2 = 1 THEN s.stanumbers2
                    WHEN s.stakind3 = 1 THEN s.stanumbers3
                    WHEN s.stakind4 = 1 THEN s.stanumbers4
                    WHEN s.stakind5 = 1 THEN s.stanumbers5
                    ELSE NULL::real[]
                END AS most_common_freqs,
                CASE
                    WHEN s.stakind1 = 2 THEN s.stavalues1::text
                    WHEN s.stakind2 = 2 THEN s.stavalues2::text
                    WHEN s.stakind3 = 2 THEN s.stavalues3::text
                    WHEN s.stakind4 = 2 THEN s.stavalues4::text
                    WHEN s.stakind5 = 2 THEN s.stavalues5::text
                    ELSE NULL::text
                END AS histogram_bounds,
                CASE
                    WHEN s.stakind1 = 3 THEN s.stanumbers1[1]
                    WHEN s.stakind2 = 3 THEN s.stanumbers2[1]
                    WHEN s.stakind3 = 3 THEN s.stanumbers3[1]
                    WHEN s.stakind4 = 3 THEN s.stanumbers4[1]
                    WHEN s.stakind5 = 3 THEN s.stanumbers5[1]
                    ELSE NULL::real
                END AS correlation,
                CASE
                    WHEN s.stakind1 = 4 THEN s.stavalues1::text
                    WHEN s.stakind2 = 4 THEN s.stavalues2::text
                    WHEN s.stakind3 = 4 THEN s.stavalues3::text
                    WHEN s.stakind4 = 4 THEN s.stavalues4::text
                    WHEN s.stakind5 = 4 THEN s.stavalues5::text
                    ELSE NULL::text
                END AS most_common_elems,
                CASE
                    WHEN s.stakind1 = 4 THEN s.stanumbers1
                    WHEN s.stakind2 = 4 THEN s.stanumbers2
                    WHEN s.stakind3 = 4 THEN s.stanumbers3
                    WHEN s.stakind4 = 4 THEN s.stanumbers4
                    WHEN s.stakind5 = 4 THEN s.stanumbers5
                    ELSE NULL::real[]
                END AS most_common_elem_freqs,
                CASE
                    WHEN s.stakind1 = 5 THEN s.stanumbers1
                    WHEN s.stakind2 = 5 THEN s.stanumbers2
                    WHEN s.stakind3 = 5 THEN s.stanumbers3
                    WHEN s.stakind4 = 5 THEN s.stanumbers4
                    WHEN s.stakind5 = 5 THEN s.stanumbers5
                    ELSE NULL::real[]
                END AS elem_count_histogram
            FROM hypopg_statistic() s
                JOIN hypopg_table() t ON t.relid = s.starelid
                JOIN pg_class c ON c.oid = t.rootid
                JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = s.staattnum
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE NOT a.attisdropped
                AND has_column_privilege(c.oid, a.attnum, 'select'::text)
                AND (c.relrowsecurity = false OR NOT row_security_active(c.oid));
    ELSE
        CREATE VIEW hypopg_stats
        WITH (security_barrier = true)
        AS
        SELECT n.nspname AS schemaname,
            t.tablename AS tablename,
            a.attname,
            s.stainherit AS inherited,
            s.stanullfrac AS null_frac,
            s.stawidth AS avg_width,
            s.stadistinct AS n_distinct,
                CASE
                    WHEN s.stakind1 = 1 THEN s.stavalues1::text
                    WHEN s.stakind2 = 1 THEN s.stavalues2::text
                    WHEN s.stakind3 = 1 THEN s.stavalues3::text
                    WHEN s.stakind4 = 1 THEN s.stavalues4::text
                    WHEN s.stakind5 = 1 THEN s.stavalues5::text
                    ELSE NULL::text
                END AS most_common_vals,
                CASE
                    WHEN s.stakind1 = 1 THEN s.stanumbers1
                    WHEN s.stakind2 = 1 THEN s.stanumbers2
                    WHEN s.stakind3 = 1 THEN s.stanumbers3
                    WHEN s.stakind4 = 1 THEN s.stanumbers4
                    WHEN s.stakind5 = 1 THEN s.stanumbers5
                    ELSE NULL::real[]
                END AS most_common_freqs,
                CASE
                    WHEN s.stakind1 = 2 THEN s.stavalues1::text
                    WHEN s.stakind2 = 2 THEN s.stavalues2::text
                    WHEN s.stakind3 = 2 THEN s.stavalues3::text
                    WHEN s.stakind4 = 2 THEN s.stavalues4::text
                    WHEN s.stakind5 = 2 THEN s.stavalues5::text
                    ELSE NULL::text
                END AS histogram_bounds,
                CASE
                    WHEN s.stakind1 = 3 THEN s.stanumbers1[1]
                    WHEN s.stakind2 = 3 THEN s.stanumbers2[1]
                    WHEN s.stakind3 = 3 THEN s.stanumbers3[1]
                    WHEN s.stakind4 = 3 THEN s.stanumbers4[1]
                    WHEN s.stakind5 = 3 THEN s.stanumbers5[1]
                    ELSE NULL::real
                END AS correlation,
                CASE
                    WHEN s.stakind1 = 4 THEN s.stavalues1::text
                    WHEN s.stakind2 = 4 THEN s.stavalues2::text
                    WHEN s.stakind3 = 4 THEN s.stavalues3::text
                    WHEN s.stakind4 = 4 THEN s.stavalues4::text
                    WHEN s.stakind5 = 4 THEN s.stavalues5::text
                    ELSE NULL::text
                END AS most_common_elems,
                CASE
                    WHEN s.stakind1 = 4 THEN s.stanumbers1
                    WHEN s.stakind2 = 4 THEN s.stanumbers2
                    WHEN s.stakind3 = 4 THEN s.stanumbers3
                    WHEN s.stakind4 = 4 THEN s.stanumbers4
                    WHEN s.stakind5 = 4 THEN s.stanumbers5
                    ELSE NULL::real[]
                END AS most_common_elem_freqs,
                CASE
                    WHEN s.stakind1 = 5 THEN s.stanumbers1
                    WHEN s.stakind2 = 5 THEN s.stanumbers2
                    WHEN s.stakind3 = 5 THEN s.stanumbers3
                    WHEN s.stakind4 = 5 THEN s.stanumbers4
                    WHEN s.stakind5 = 5 THEN s.stanumbers5
                    ELSE NULL::real[]
                END AS elem_count_histogram
            FROM hypopg_statistic() s
                JOIN hypopg_table() t ON t.relid = s.starelid
                JOIN pg_class c ON c.oid = t.rootid
                JOIN pg_attribute a ON c.oid = a.attrelid AND a.attnum = s.staattnum
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE NOT a.attisdropped
                AND has_column_privilege(c.oid, a.attnum, 'select'::text);
    END IF;
END;
$_$ language plpgsql;
