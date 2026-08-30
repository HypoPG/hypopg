-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2015-2026: Julien Rouhaud

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION hypopg" to load this file. \quit

CREATE FUNCTION hypopg_analyze(IN relation regclass, IN fraction real DEFAULT 1.0)
    RETURNS void
    LANGUAGE C VOLATILE COST 100
AS '$libdir/hypopg', 'hypopg_analyze';
