-- More AM-agnostic infrastructure tests, using more complex queries

-- Remove all the hypothetical indexes if any
SELECT hypopg_reset();

-------------------
-- JOIN with an SRF
-------------------

-- Query should do a seq scan on hypo
SELECT COUNT(*) FROM do_explain($$
    SELECT *
    FROM hypo h
    JOIN generate_series(1, 10) f(g) ON h.id = f.g
$$) e
WHERE e ~ 'Seq Scan on hypo';

SELECT COUNT(*) AS NB
FROM hypopg_create_index('CREATE INDEX ON hypo(id)');

-- Should use the hypothetical index
SELECT COUNT(*) FROM do_explain($$
    SELECT *
    FROM hypo h
    JOIN generate_series(1, 10) f(g) ON h.id = f.g
$$) e
WHERE e ~ 'Index.*<\d+>btree_hypo';
