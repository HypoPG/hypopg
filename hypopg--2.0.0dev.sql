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

CREATE FUNCTION hypopg_table(OUT relid oid, OUT tablename text,
    OUT parentid oid,
    OUT partition_by_clause text, OUT partition_of_clause text)
    RETURNS SETOF record
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg_table';

CREATE FUNCTION hypopg_analyze(IN tablename regclass, IN fraction real = 1)
    RETURNS void
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg_analyze';
