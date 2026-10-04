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
