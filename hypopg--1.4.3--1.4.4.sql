-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2015-2026: Julien Rouhaud

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION hypopg" to load this file. \quit

DROP VIEW hypopg_hidden_indexes;
DROP VIEW hypopg_list_indexes;
DROP FUNCTION hypopg();
CREATE FUNCTION hypopg(OUT invalidated bool,
                       OUT indexname text, OUT indexrelid oid,
                       OUT indrelid oid, OUT innatts integer,
                       OUT indisunique boolean, OUT indkey int2vector,
                       OUT indcollation oidvector, OUT indclass oidvector,
                       OUT indoption oidvector, OUT indexprs pg_node_tree,
                       OUT indpred pg_node_tree, OUT amid oid)
    RETURNS SETOF record
    LANGUAGE c COST 100
AS '$libdir/hypopg', 'hypopg_144';

CREATE VIEW hypopg_list_indexes
AS
    SELECT h.invalidated,
    h.indexrelid, h.indexname AS index_name,
    coalesce(n.nspname, '<invalidated>') AS schema_name,
    coalesce(c.relname, '<invalidated>') AS table_name,
    coalesce(am.amname, '<invalidated>') AS am_name
    FROM hypopg() h
    LEFT JOIN pg_catalog.pg_class c ON c.oid = h.indrelid
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_am am ON am.oid = h.amid;

CREATE VIEW hypopg_hidden_indexes
AS
    SELECT false AS invalidated,
           h.indexid AS indexrelid,
           i.relname AS index_name,
           n.nspname AS schema_name,
           t.relname AS table_name,
           m.amname  AS am_name,
           false     AS is_hypo
    FROM hypopg_hidden_indexes() h
        JOIN pg_index x ON x.indexrelid = h.indexid
        JOIN pg_class i ON i.oid = h.indexid
        JOIN pg_namespace n ON n.oid = i.relnamespace
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_am m ON m.oid = i.relam
    UNION ALL
    SELECT hl.*, true AS is_hypo
    FROM hypopg_hidden_indexes() hi
        JOIN hypopg_list_indexes hl on hl.indexrelid = hi.indexid
    ORDER BY index_name;
