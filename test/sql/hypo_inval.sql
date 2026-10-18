-- Test cache invalidation

-- Remove all the hypothetical indexes if any
SELECT hypopg_reset();

CREATE SCHEMA nsp_hypo;
CREATE TABLE nsp_hypo.hypo2(id integer, val text);

SELECT COUNT(*) AS NB
FROM hypopg_create_index('CREATE INDEX ON hypo(id)');
SELECT COUNT(*) AS NB
FROM hypopg_create_index('CREATE INDEX ON nsp_hypo.hypo2(id)');

-- hypothetical index should be usable
SELECT COUNT(*) FROM do_explain($$
    SELECT *
    FROM hypo h
    WHERE id = 1
$$) e
WHERE e ~ 'Index.*<\d+>btree_hypo';

-- no hypothetical index should have been invalidated
SELECT invalidated, table_name
FROM hypopg_list_indexes
ORDER BY indexrelid;

-- VACUUM should not be a problem, but it still sends an invalidation message
-- so rely on that to check for hypothetical index invalidation on the
-- underlying relation.
VACUUM pg_class;

-- no hypothetical index should not be invalidated after vacuuming an unrelated
-- table
SELECT invalidated, table_name
FROM hypopg_list_indexes
ORDER BY indexrelid;

-- hypothetical index should still be usable
SELECT COUNT(*) FROM do_explain($$
    SELECT *
    FROM hypo h
    WHERE id = 1
$$) e
WHERE e ~ 'Index.*<\d+>btree_hypo';

VACUUM hypo;

-- only the index on hypo should be invalidated
SELECT invalidated, table_name
FROM hypopg_list_indexes
ORDER BY indexrelid;

-- hypothetical index on hypo should not be usable anymore
SELECT COUNT(*) FROM do_explain($$
    SELECT *
    FROM hypo h
    WHERE id = 1
$$) e
WHERE e ~ 'Seq Scan on hypo';

-- we shouldn't be able to get the index DDL or estimated size anymore
SELECT hypopg_get_indexdef(indexrelid), hypopg_relation_size(indexrelid)
FROM hypopg() h
WHERE h.invalidated;

DROP SCHEMA nsp_hypo CASCADE;

-- the index on nsp_hypo.hypo2 should now be invalidated
SELECT invalidated, count(*) FROM hypopg() GROUP BY 1 ORDER BY 1;

------------------------------------------------------------------------
-- Function dependencies in all index expressions and predicate clauses.
------------------------------------------------------------------------
CREATE TABLE hypo_inval_functions(a integer, b integer, c integer);
CREATE FUNCTION hypo_inval_f1(integer) RETURNS integer
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 1';
CREATE FUNCTION hypo_inval_f2(integer) RETURNS integer
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 2';
CREATE FUNCTION hypo_inval_pred(integer) RETURNS boolean
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 > 0';
CREATE FUNCTION hypo_inval_unrelated(integer) RETURNS integer
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 3';
CREATE FUNCTION hypo_inval_op(integer, integer) RETURNS boolean
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 = $2';
CREATE OPERATOR === (
    LEFTARG = integer, RIGHTARG = integer, PROCEDURE = hypo_inval_op
);

-- Use different functions in the two columns to detect a skipped expression.
-- These SQL functions can be inlined during the index's immutability checks;
-- their dependencies must still be collected from the original expressions.
DO $$
DECLARE
    func text;
    definition text;
    idx oid;
BEGIN
    -- Each row pairs the function to alter with an index that depends on it.
    FOR func, definition IN
        SELECT * FROM (VALUES
            -- Function in the first index column.
            ('hypo_inval_f1(integer)',
             '(hypo_inval_f1(a), hypo_inval_f2(b)) WHERE a > 0 OR hypo_inval_pred(c)'),
            -- A different function in the second column must also be tracked.
            ('hypo_inval_f2(integer)',
             '(hypo_inval_f1(a), hypo_inval_f2(b)) WHERE a > 0 OR hypo_inval_pred(c)'),
            -- Function inside an OR predicate alongside column expressions.
            ('hypo_inval_pred(integer)',
             '(hypo_inval_f1(a), hypo_inval_f2(b)) WHERE a > 0 OR hypo_inval_pred(c)'),
            -- Column expression with no predicate.
            ('hypo_inval_f1(integer)', '(hypo_inval_f1(a))'),
            -- Predicate with no column expressions.
            ('hypo_inval_pred(integer)', '(a) WHERE hypo_inval_pred(c)'),
            -- Operator's implementation function, without a direct call.
            ('hypo_inval_op(integer, integer)', '((a === b))')
        ) AS cases(func, definition)
    LOOP
        -- Start fresh so each function must trigger invalidation on its own.
        PERFORM hypopg_reset();

        -- These indexes must stay valid: they don't use a user defined function.
        PERFORM hypopg_create_index('CREATE INDEX ON hypo_inval_functions (a)');
        PERFORM hypopg_create_index('CREATE INDEX ON hypo_inval_functions (abs(b))');

        -- Save the dependent index's OID to check which index is invalidated.
        SELECT indexrelid INTO idx FROM hypopg_create_index(
            'CREATE INDEX ON hypo_inval_functions ' || definition);

        -- An unrelated function change must leave all three indexes valid.
        ALTER FUNCTION hypo_inval_unrelated(integer) COST 200;
        IF EXISTS (SELECT 1 FROM hypopg() WHERE invalidated) THEN
            RAISE WARNING 'unrelated function invalidated an index: %', definition;
        END IF;

        -- Changing COST sends a function invalidation
        EXECUTE 'ALTER FUNCTION ' || func || ' COST 200';

        -- Only the index saved above should now be invalidated.
        IF (SELECT count(*) FROM hypopg() WHERE invalidated) <> 1 OR
           NOT (SELECT invalidated FROM hypopg() WHERE indexrelid = idx) THEN
            RAISE WARNING 'incorrect invalidation for %: %', func, definition;
        END IF;

        -- Definition and size lookup must stop using the invalidated index.
        IF hypopg_get_indexdef(idx) <> '<invalidated>' OR
           hypopg_relation_size(idx) IS NOT NULL THEN
            RAISE WARNING 'invalidated index remains accessible: %', definition;
        END IF;
    END LOOP;
END
$$;

-- DROP must also invalidate the dependent index and leave the plain one valid.
SELECT hypopg_reset();
SELECT count(*) FROM hypopg_create_index(
    'CREATE INDEX ON hypo_inval_functions (a)');
SELECT count(*) FROM hypopg_create_index(
    'CREATE INDEX ON hypo_inval_functions (hypo_inval_f1(a))');

-- Both indexes are valid before the function is dropped.
SELECT invalidated, indexname ~ '_expr$' AS expression
FROM hypopg() ORDER BY indexrelid;

DROP FUNCTION hypo_inval_f1(integer);

-- Only the expression index should be invalidated.
SELECT invalidated, indexname ~ '_expr$' AS expression
FROM hypopg() ORDER BY indexrelid;
SELECT hypopg_get_indexdef(indexrelid), hypopg_relation_size(indexrelid)
FROM hypopg() WHERE invalidated;

-- Remove the remaining indexes before dropping the test objects.
SELECT hypopg_reset();

DROP OPERATOR === (integer, integer);
DROP FUNCTION hypo_inval_op(integer, integer);
DROP FUNCTION hypo_inval_f2(integer);
DROP FUNCTION hypo_inval_pred(integer);
DROP FUNCTION hypo_inval_unrelated(integer);
DROP TABLE hypo_inval_functions;

------------------------------------------------------------------------
-- Operator class dependencies on every key column, including expressions.
------------------------------------------------------------------------
CREATE TABLE hypo_inval_opclasses(a integer, b integer);
CREATE FUNCTION hypo_inval_opclass_func(integer) RETURNS integer
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 1';
CREATE OPERATOR CLASS hypo_inval_opclass1 FOR TYPE integer USING btree AS
    OPERATOR 1 < (integer, integer),
    OPERATOR 2 <= (integer, integer),
    OPERATOR 3 = (integer, integer),
    OPERATOR 4 >= (integer, integer),
    OPERATOR 5 > (integer, integer),
    FUNCTION 1 btint4cmp(integer, integer);
CREATE OPERATOR CLASS hypo_inval_opclass2 FOR TYPE integer USING btree AS
    OPERATOR 1 < (integer, integer),
    OPERATOR 2 <= (integer, integer),
    OPERATOR 3 = (integer, integer),
    OPERATOR 4 >= (integer, integer),
    OPERATOR 5 > (integer, integer),
    FUNCTION 1 btint4cmp(integer, integer);
CREATE OPERATOR CLASS hypo_inval_opclass_unrelated FOR TYPE integer USING btree AS
    OPERATOR 1 < (integer, integer),
    OPERATOR 2 <= (integer, integer),
    OPERATOR 3 = (integer, integer),
    OPERATOR 4 >= (integer, integer),
    OPERATOR 5 > (integer, integer),
    FUNCTION 1 btint4cmp(integer, integer);
DO $$
DECLARE
    action text;
    plain_idx oid;
    first_idx oid;
    second_idx oid;
    expr_idx oid;
    expected_invalid oid[];
BEGIN
    FOREACH action IN ARRAY ARRAY['first', 'second', 'function', 'drop']
    LOOP
        PERFORM hypopg_reset();
        SELECT indexrelid INTO plain_idx FROM hypopg_create_index(
            'CREATE INDEX ON hypo_inval_opclasses (a, b)');
        SELECT indexrelid INTO first_idx FROM hypopg_create_index(
            'CREATE INDEX ON hypo_inval_opclasses (a hypo_inval_opclass1, b hypo_inval_opclass2)');
        SELECT indexrelid INTO second_idx FROM hypopg_create_index(
            'CREATE INDEX ON hypo_inval_opclasses (a, b hypo_inval_opclass2)');
        SELECT indexrelid INTO expr_idx FROM hypopg_create_index(
            'CREATE INDEX ON hypo_inval_opclasses (hypo_inval_opclass_func(a) hypo_inval_opclass1)');

        -- Neither unrelated nor built-in operator classes are dependencies.
        ALTER OPERATOR CLASS hypo_inval_opclass_unrelated USING btree
            RENAME TO hypo_inval_opclass_renamed;
        ALTER OPERATOR CLASS hypo_inval_opclass_renamed USING btree
            RENAME TO hypo_inval_opclass_unrelated;
        UPDATE pg_catalog.pg_opclass SET opcname = opcname
            WHERE oid < 16384 AND opcname = 'int4_ops';
        IF EXISTS (SELECT 1 FROM hypopg() WHERE invalidated) THEN
            RAISE WARNING 'unrelated or core operator class invalidated an index: %', action;
        END IF;

        IF action = 'first' THEN
            ALTER OPERATOR CLASS hypo_inval_opclass1 USING btree
                RENAME TO hypo_inval_opclass_renamed;
            expected_invalid := ARRAY[first_idx, expr_idx];
        ELSIF action = 'second' THEN
            ALTER OPERATOR CLASS hypo_inval_opclass2 USING btree
                RENAME TO hypo_inval_opclass_renamed;
            expected_invalid := ARRAY[first_idx, second_idx];
        ELSIF action = 'function' THEN
            ALTER FUNCTION hypo_inval_opclass_func(integer) COST 200;
            expected_invalid := ARRAY[expr_idx];
        ELSE
            DROP OPERATOR CLASS hypo_inval_opclass1 USING btree;
            expected_invalid := ARRAY[first_idx, expr_idx];
        END IF;

        -- Check every index, including the plain_idx using built-in classes.
        IF (SELECT count(*) FROM hypopg()) <> 4 OR EXISTS (
            SELECT 1 FROM hypopg()
            WHERE invalidated IS DISTINCT FROM (indexrelid = ANY(expected_invalid))
        ) THEN
            RAISE WARNING 'incorrect operator class/function invalidation: %', action;
        END IF;

        PERFORM hypopg_reset();
        IF action = 'first' THEN
            ALTER OPERATOR CLASS hypo_inval_opclass_renamed USING btree
                RENAME TO hypo_inval_opclass1;
        ELSIF action = 'second' THEN
            ALTER OPERATOR CLASS hypo_inval_opclass_renamed USING btree
                RENAME TO hypo_inval_opclass2;
        END IF;
    END LOOP;
END
$$;

RESET enable_seqscan;
DROP OPERATOR FAMILY hypo_inval_opclass1 USING btree;
DROP OPERATOR FAMILY hypo_inval_opclass2 USING btree;
DROP OPERATOR FAMILY hypo_inval_opclass_unrelated USING btree;
DROP FUNCTION hypo_inval_opclass_func(integer);
DROP TABLE hypo_inval_opclasses;

------------------------------------------------------------------------
-- Relation references in expressions and predicates, including OID constants.
------------------------------------------------------------------------
CREATE TABLE hypo_inval_relations(a integer, b integer);
INSERT INTO hypo_inval_relations SELECT i, i FROM generate_series(1, 1000) i;
ANALYZE hypo_inval_relations;
SET enable_seqscan = off;
DO $do$
DECLARE
    action text;
    plain_idx oid;
    expr_idx oid;
    pred_idx oid;
    oid_idx oid;
    expr_name text;
    expected_invalid oid[];
BEGIN
    FOREACH action IN ARRAY ARRAY['first', 'second']
    LOOP
        PERFORM hypopg_reset();
        CREATE TABLE hypo_inval_ref1(id integer);
        CREATE TABLE hypo_inval_ref2(id integer);
        CREATE TABLE hypo_inval_ref_unrelated(id integer);

        -- This index references only the indexed table and must remain valid
        -- when either of the other relations is dropped.
        SELECT indexrelid INTO plain_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_relations (b)$$);
        -- Each reference must independently invalidate this index.
        SELECT indexrelid, indexname INTO expr_idx, expr_name FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_relations
                (a, ('hypo_inval_ref1'::regclass), ('hypo_inval_ref2'::regclass))$$);
        SELECT indexrelid INTO pred_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_relations (b)
                WHERE a::oid = 'hypo_inval_ref1'::regclass$$);
        -- The upstream extractor conservatively tracks plain OID constants too.
        SELECT indexrelid INTO oid_idx FROM hypopg_create_index(format(
            $$CREATE INDEX ON hypo_inval_relations (b, (%L::oid))$$,
            'hypo_inval_ref1'::regclass::oid));

        DROP TABLE hypo_inval_ref_unrelated;
        IF EXISTS (SELECT 1 FROM hypopg() WHERE invalidated) THEN
            RAISE WARNING 'unrelated relation invalidated an index: %', action;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM do_explain('SELECT * FROM hypo_inval_relations WHERE a = 1')
            WHERE position(expr_name in a) > 0
        ) THEN
            RAISE WARNING 'referencing index was not used before invalidation: %', action;
        END IF;

        IF action = 'first' THEN
            DROP TABLE hypo_inval_ref1;
            expected_invalid := ARRAY[expr_idx, pred_idx, oid_idx];
        ELSE
            DROP TABLE hypo_inval_ref2;
            expected_invalid := ARRAY[expr_idx];
        END IF;

        IF (SELECT count(*) FROM hypopg()) <> 4 OR EXISTS (
            SELECT 1 FROM hypopg()
            WHERE invalidated IS DISTINCT FROM (indexrelid = ANY(expected_invalid))
        ) THEN
            RAISE WARNING 'incorrect referenced relation invalidation: %', action;
        END IF;
        IF EXISTS (
            SELECT 1 FROM hypopg() WHERE indexrelid = ANY(expected_invalid)
            AND (hypopg_get_indexdef(indexrelid) IS DISTINCT FROM '<invalidated>'
                 OR hypopg_relation_size(indexrelid) IS NOT NULL)
        ) THEN
            RAISE WARNING 'invalidated referencing index remains accessible: %', action;
        END IF;
        IF EXISTS (
            SELECT 1 FROM do_explain('SELECT * FROM hypo_inval_relations WHERE a = 1')
            WHERE position(expr_name in a) > 0
        ) THEN
            RAISE WARNING 'invalidated referencing index remains in plan: %', action;
        END IF;

        PERFORM hypopg_reset();
        IF action = 'first' THEN
            DROP TABLE hypo_inval_ref2;
        ELSE
            DROP TABLE hypo_inval_ref1;
        END IF;
    END LOOP;
END
$do$;
RESET enable_seqscan;
DROP TABLE hypo_inval_relations;

------------------------------------------------------------------------
-- Type dependencies in columns, nested expressions, predicates and constants.
------------------------------------------------------------------------
CREATE DOMAIN hypo_inval_type1 AS integer;
CREATE DOMAIN hypo_inval_type2 AS integer;
CREATE DOMAIN hypo_inval_column_type AS integer;
CREATE TYPE hypo_inval_referenced_type AS ENUM ('x', 'y');
CREATE TABLE hypo_inval_types(a integer, b integer, c hypo_inval_column_type);
CREATE FUNCTION hypo_inval_type_func(integer) RETURNS integer
    LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 1';
DO $do$
DECLARE
    action text;
    plain_idx oid;
    nested_idx oid;
    pred_idx oid;
    column_idx oid;
    ref_idx oid;
    ref_pred_idx oid;
    oid_idx oid;
    mixed_idx oid;
    expected_invalid oid[];
BEGIN
    FOREACH action IN ARRAY ARRAY['first', 'second', 'column', 'regtype', 'function', 'drop']
    LOOP
        PERFORM hypopg_reset();
        CREATE DOMAIN hypo_inval_unrelated_type AS integer;
        CREATE TABLE hypo_inval_unrelated_table(id integer);

        -- A built-in column type must not depend on any of the custom types.
        SELECT indexrelid INTO plain_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types (a)$$);
        -- Both intermediate domain types matter even though the result is int.
        -- CASE also exercises traversal through branches and their conditions.
        SELECT indexrelid INTO nested_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types
                ((CASE WHEN a > 0 THEN ((a::hypo_inval_type1)::hypo_inval_type2)::integer
                       ELSE 0 END))$$);
        -- A predicate can contain a type absent from every index key.
        SELECT indexrelid INTO pred_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types (b) WHERE a::hypo_inval_type2 > 0$$);
        -- Simple columns have no expression tree, so collect their types too.
        SELECT indexrelid INTO column_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types (c)$$);
        -- The constant's value names a type distinct from its own regtype type.
        SELECT indexrelid INTO ref_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types (b, ('hypo_inval_referenced_type'::regtype))$$);
        -- An OID comparison may coerce a regtype constant in a predicate.
        SELECT indexrelid INTO ref_pred_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types (b)
                WHERE a::oid = 'hypo_inval_referenced_type'::regtype$$);
        -- Coercion can erase the regtype label; track matching OIDs conservatively.
        SELECT indexrelid INTO oid_idx FROM hypopg_create_index(format(
            $$CREATE INDEX ON hypo_inval_types (b, (%L::oid))$$,
            'hypo_inval_referenced_type'::regtype::oid));
        -- Type collection must preserve the existing function dependencies.
        SELECT indexrelid INTO mixed_idx FROM hypopg_create_index(
            $$CREATE INDEX ON hypo_inval_types
                (((hypo_inval_type_func(a)::hypo_inval_type1)::integer))$$);

        DROP DOMAIN hypo_inval_unrelated_type;
        DROP TABLE hypo_inval_unrelated_table;
        IF EXISTS (SELECT 1 FROM hypopg() WHERE invalidated) THEN
            RAISE WARNING 'unrelated type or table invalidated an index: %', action;
        END IF;

        IF action = 'first' THEN
            ALTER DOMAIN hypo_inval_type1 RENAME TO hypo_inval_type_renamed;
            expected_invalid := ARRAY[nested_idx, mixed_idx];
        ELSIF action = 'second' THEN
            ALTER DOMAIN hypo_inval_type2 RENAME TO hypo_inval_type_renamed;
            expected_invalid := ARRAY[nested_idx, pred_idx];
        ELSIF action = 'column' THEN
            ALTER DOMAIN hypo_inval_column_type RENAME TO hypo_inval_type_renamed;
            expected_invalid := ARRAY[column_idx];
        ELSIF action = 'regtype' THEN
            ALTER TYPE hypo_inval_referenced_type RENAME TO hypo_inval_type_renamed;
            expected_invalid := ARRAY[ref_idx, ref_pred_idx, oid_idx];
        ELSIF action = 'function' THEN
            ALTER FUNCTION hypo_inval_type_func(integer) COST 200;
            expected_invalid := ARRAY[mixed_idx];
        ELSE
            DROP TYPE hypo_inval_referenced_type;
            expected_invalid := ARRAY[ref_idx, ref_pred_idx, oid_idx];
        END IF;

        IF (SELECT count(*) FROM hypopg()) <> 8 OR EXISTS (
            SELECT 1 FROM hypopg()
            WHERE invalidated IS DISTINCT FROM (indexrelid = ANY(expected_invalid))
        ) THEN
            RAISE WARNING 'incorrect type/function invalidation: %', action;
        END IF;
        IF EXISTS (
            SELECT 1 FROM hypopg() WHERE indexrelid = ANY(expected_invalid)
            AND (hypopg_get_indexdef(indexrelid) IS DISTINCT FROM '<invalidated>'
                 OR hypopg_relation_size(indexrelid) IS NOT NULL)
        ) THEN
            RAISE WARNING 'invalidated type-dependent index remains accessible: %', action;
        END IF;

        PERFORM hypopg_reset();
        IF action = 'first' THEN
            ALTER DOMAIN hypo_inval_type_renamed RENAME TO hypo_inval_type1;
        ELSIF action = 'second' THEN
            ALTER DOMAIN hypo_inval_type_renamed RENAME TO hypo_inval_type2;
        ELSIF action = 'column' THEN
            ALTER DOMAIN hypo_inval_type_renamed RENAME TO hypo_inval_column_type;
        ELSIF action = 'regtype' THEN
            ALTER TYPE hypo_inval_type_renamed RENAME TO hypo_inval_referenced_type;
        END IF;
    END LOOP;
END
$do$;
DROP FUNCTION hypo_inval_type_func(integer);
DROP TABLE hypo_inval_types;
DROP DOMAIN hypo_inval_type1;
DROP DOMAIN hypo_inval_type2;
DROP DOMAIN hypo_inval_column_type;

-- Track the type of an included column even when every key is built-in.
DO $do$
DECLARE
    included_idx oid;
BEGIN
    -- INCLUDE is only supported on PostgreSQL 11 and later.
    IF current_setting('server_version_num')::integer < 110000 THEN
        RETURN;
    END IF;

    CREATE DOMAIN hypo_inval_include_type AS integer;
    CREATE TABLE hypo_inval_include_types(a integer, b hypo_inval_include_type);
    PERFORM hypopg_reset();
    -- The plain index has no dependency on the included column's type.
    PERFORM hypopg_create_index($$CREATE INDEX ON hypo_inval_include_types (a)$$);
    SELECT indexrelid INTO included_idx FROM hypopg_create_index(
        $$CREATE INDEX ON hypo_inval_include_types (a) INCLUDE (b)$$);

    ALTER DOMAIN hypo_inval_include_type RENAME TO hypo_inval_include_type_renamed;
    IF (SELECT count(*) FROM hypopg()) <> 2 OR EXISTS (
        SELECT 1 FROM hypopg()
        WHERE invalidated IS DISTINCT FROM (indexrelid = included_idx)
    ) THEN
        RAISE WARNING 'incorrect included column type invalidation';
    END IF;
    PERFORM hypopg_reset();
    DROP TABLE hypo_inval_include_types;
    DROP DOMAIN hypo_inval_include_type_renamed;
END
$do$;
