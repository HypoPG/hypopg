CREATE EXTENSION hypopg;

CREATE TABLE hypo_analyze (id integer, value text);
INSERT INTO hypo_analyze
SELECT i, 'value ' || i FROM generate_series(1, 100000) AS g(i);

SELECT hypopg_analyze('hypo_analyze');
SELECT hypopg_analyze('hypo_analyze', 0);
