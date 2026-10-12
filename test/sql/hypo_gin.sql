-- Hypothetical GIN indexes with sampled estimates (PostgreSQL 9.5+)

SELECT hypopg_reset();

CREATE TABLE hypo_gin (
	id integer,
	tags integer[],
	tsv tsvector,
	j jsonb,
	body text,
	j_dense jsonb,
	j_sparse jsonb
);

INSERT INTO hypo_gin
SELECT i,
	ARRAY[i % 50, (i * 3) % 50],
	to_tsvector('simple', 'token' || (i % 50)),
	(('{ "k": ' || (i % 50) || ' }')::jsonb),
	'token' || (i % 50),
	jsonb_build_object(
		'a', i % 10, 'b', i % 11, 'c', i % 12, 'd', i % 13,
		'e', i % 14, 'f', i % 15, 'g', i % 16, 'h', i % 17),
	to_jsonb(repeat('x', 140))
FROM generate_series(1, 10000) AS g(i);

ANALYZE hypo_gin;

-- array
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index('CREATE INDEX ON hypo_gin USING gin (tags)');

SET enable_seqscan = 0;
SELECT COUNT(*) FROM do_explain('SELECT * FROM hypo_gin WHERE tags @> ARRAY[1]') e
WHERE e ~ 'Bitmap Index Scan.*<\d+>gin_hypo_gin.*';

SELECT hypopg_get_indexdef(indexrelid) FROM hypopg();
SELECT hypopg_relation_size(indexrelid) > 0 FROM hypopg();

SELECT hypopg_reset();

-- tsvector
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index('CREATE INDEX ON hypo_gin USING gin (tsv)');

SELECT COUNT(*) FROM do_explain($$SELECT * FROM hypo_gin WHERE tsv @@ to_tsquery('simple', 'token1')$$) e
WHERE e ~ 'Bitmap Index Scan.*<\d+>gin_hypo_gin.*';

SELECT hypopg_reset();

-- jsonb
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index('CREATE INDEX ON hypo_gin USING gin (j)');

SELECT COUNT(*) FROM do_explain($$SELECT * FROM hypo_gin WHERE j @> '{"k": 1}'$$) e
WHERE e ~ 'Bitmap Index Scan.*<\d+>gin_hypo_gin.*';

SELECT hypopg_reset();

-- expression and partial predicate are applied by the sample query
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index(
	$$CREATE INDEX ON hypo_gin USING gin
	  (to_tsvector('simple', body)) WHERE id < 5000$$);

SELECT COUNT(*) FROM do_explain(
	$$SELECT * FROM hypo_gin
	  WHERE id < 5000
	  AND to_tsvector('simple', body) @@ to_tsquery('simple', 'token1')$$) e
WHERE e ~ 'Bitmap Index Scan.*<\d+>gin_hypo_gin.*';

SELECT hypopg_reset();

-- Similar-width jsonb values with different extracted-key density should
-- produce different estimates.  A width heuristic cannot distinguish these.
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index(
	'CREATE INDEX ON hypo_gin USING gin (j_dense)');
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index(
	'CREATE INDEX ON hypo_gin USING gin (j_sparse)');

SELECT
	(SELECT hypopg_relation_size(indexrelid)
	 FROM hypopg()
	 WHERE indexname LIKE '%j_dense') >
	(SELECT hypopg_relation_size(indexrelid)
	 FROM hypopg()
	 WHERE indexname LIKE '%j_sparse') AS extract_value_matters;

SELECT hypopg_reset();

-- An expression error during sampling falls back without corrupting the
-- backend's exception stack.
SELECT COUNT(*) AS nb
FROM public.hypopg_create_index(
	'CREATE INDEX ON hypo_gin USING gin ((ARRAY[1 / (id - id)]))');
SELECT hypopg_relation_size(indexrelid) > 0 AS used_fallback FROM hypopg();
SELECT 1 AS backend_still_healthy;

RESET enable_seqscan;
SELECT hypopg_reset();
DROP TABLE hypo_gin;
