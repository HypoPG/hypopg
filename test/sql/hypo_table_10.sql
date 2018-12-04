-- Creating real and hypothetical tables"
-- ====================================="

-- Real tables
-- -----------
-- 1. Range partition
DROP TABLE IF EXISTS part_range;
CREATE TABLE part_range (id integer, val text) PARTITION BY RANGE (id);
CREATE TABLE part_range_1_10000 PARTITION OF part_range FOR VALUES FROM (1) TO (10000);
CREATE TABLE part_range_10000_20000 PARTITION OF part_range FOR VALUES FROM (10000) TO (20000);
CREATE TABLE part_range_20000_30000 PARTITION OF part_range FOR VALUES FROM (20000) TO (30000);
INSERT INTO part_range SELECT i, 'line ' || i FROM generate_series(1, 29999) i;
-- 2. List partitioning
DROP TABLE IF EXISTS part_list;
CREATE TABLE part_list (id integer, id_key integer, val text) PARTITION BY LIST (id_key);
CREATE TABLE part_list_4_5_6_8_10 PARTITION OF part_list FOR VALUES IN (4, 5, 6, 8, 10);
CREATE TABLE part_list_7_9 PARTITION OF part_list FOR VALUES IN (7, 9);
CREATE TABLE part_list_1_2_3 PARTITION OF part_list FOR VALUES IN (1, 2, 3);
INSERT INTO part_list SELECT i, (i % 9) + 1, 'line ' || i FROM generate_series(1, 50000) i;
-- 3. Multi level range
DROP TABLE IF EXISTS part_multi;
CREATE TABLE part_multi(dpt smallint, dt date, val text) PARTITION BY LIST (dpt);
CREATE TABLE part_multi_2 PARTITION OF part_multi FOR VALUES IN (2) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_2_q1 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_2_q2 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$);
CREATE TABLE part_multi_2_q3 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$);
CREATE TABLE part_multi_2_q4 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$);
CREATE TABLE part_multi_1 PARTITION OF part_multi FOR VALUES IN (1) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_1_q2 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$);
CREATE TABLE part_multi_1_q3 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$);
CREATE TABLE part_multi_1_q4 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$);
CREATE TABLE part_multi_1_q1 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$) PARTITION BY RANGE (dt);
CREATE TABLE part_multi_1_q1_b PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-02-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_1_q1_a PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$);
CREATE TABLE part_multi_3 PARTITION OF part_multi FOR VALUES IN (3) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_3_q1 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_3_q2 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$);
CREATE TABLE part_multi_3_q3 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$);
CREATE TABLE part_multi_3_q4 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$);
INSERT INTO part_multi select (i%3)+1, '2015-01-01'::date + interval '1 day' * (i%365), 'val ' || i FROM generate_series(1,50000) i;

-- Hypothetical tables
-- -------------------
-- 0. Dropping any hypothetical object
SELECT * FROM hypopg_reset();
-- 1. Range partition
DROP TABLE IF EXISTS hypo_part_range;
CREATE TABLE hypo_part_range (id integer, val text);
INSERT INTO hypo_part_range SELECT i, 'line ' || i FROM generate_series(1, 29999) i;
SELECT * FROM hypopg_partition_table('hypo_part_range', 'PARTITION BY RANGE (id)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_1_10000', 'PARTITION OF hypo_part_range FOR VALUES FROM (1) TO (10000)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_10000_20000', 'PARTITION OF hypo_part_range FOR VALUES FROM (10000) TO (20000)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_20000_30000', 'PARTITION OF hypo_part_range FOR VALUES FROM (20000) TO (30000)');
-- 2. List partitioning
DROP TABLE IF EXISTS hypo_part_list;
CREATE TABLE hypo_part_list (id integer, id_key integer, val text);
INSERT INTO hypo_part_list SELECT i, (i % 9) + 1, 'line ' || i FROM generate_series(1, 50000) i;
SELECT * FROM hypopg_partition_table('hypo_part_list', 'PARTITION BY LIST (id_key)');
SELECT tablename FROM hypopg_add_partition('hypo_part_list_4_5_6_8_10', 'PARTITION OF hypo_part_list FOR VALUES IN (4, 5, 6, 8, 10)');
SELECT tablename FROM hypopg_add_partition('hypo_part_list_7_9', 'PARTITION OF hypo_part_list FOR VALUES IN (7, 9)');
SELECT tablename FROM hypopg_add_partition('hypo_part_list_1_2_3', 'PARTITION OF hypo_part_list FOR VALUES IN (1, 2, 3)');
-- 3. Multi level range
DROP TABLE IF EXISTS hypo_part_multi;
CREATE TABLE hypo_part_multi(dpt smallint, dt date, val text);
INSERT INTO hypo_part_multi select (i%3)+1, '2015-01-01'::date + interval '1 day' * (i%365), 'val ' || i FROM generate_series(1,50000) i;
SELECT * FROM hypopg_partition_table('hypo_part_multi', 'PARTITION BY LIST (dpt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2', 'PARTITION OF hypo_part_multi FOR VALUES IN (2)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q1', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q2', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q3', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q4', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1', 'PARTITION OF hypo_part_multi FOR VALUES IN (1)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q2', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q3', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q4', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)','PARTITION BY RANGE (dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_b', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-02-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_a', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3', 'PARTITION OF hypo_part_multi FOR VALUES IN (3)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q1', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q2', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q3', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q4', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$)');

-- Maintenance
-- -----------
VACUUM ANALYZE;
SELECT * FROM hypopg_analyze('hypo_part_range',100);
SELECT * FROM hypopg_analyze('hypo_part_list',100);
SELECT * FROM hypopg_analyze('hypo_part_multi',100);


-- Test deparsing
-- ==============
SELECT relid = rootid AS is_root, tablename, parentid IS NULL parentid_is_null,
    parentid IS NOT NULL AS parentid_is_not_null,
    partition_by_clause, partition_bounds
FROM hypopg_table()
ORDER BY tablename COLLATE "C";

-- Test hypothetical partitioning behavior
-- =======================================

-- Simple queries
-- --------------

-- Real tables
-- -----------
-- 1. Range partitioning
EXPLAIN (COSTS OFF) SELECT * FROM part_range;
EXPLAIN (COSTS OFF) SELECT * FROM part_range WHERE id = 42;
EXPLAIN (COSTS OFF) SELECT * FROM part_range WHERE id < 15000;
-- 2. List partitioning
EXPLAIN (COSTS OFF) SELECT * FROM part_list;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id < 42;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id_key = 7;
-- 3. Multi level range
EXPLAIN (COSTS OFF) SELECT * FROM part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM part_multi WHERE dpt = 2;
EXPLAIN (COSTS OFF) SELECT * FROM part_multi WHERE dt >= '2015-01-05' AND dt < '2015-01-10';

-- Hypothetical tables
-- -------------------
-- 1. Range partitioning
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range WHERE id = 42;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range WHERE id < 15000;
-- 2. List partitioning
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id < 42;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id_key = 7;
-- 3. Multi level range
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi WHERE dpt = 2;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi WHERE dt >= '2015-01-05' AND dt < '2015-01-10';


-- Join queries
-- ------------

-- Simple joins
-- ------------
-- 1. Real tables
EXPLAIN (COSTS OFF) SELECT * FROM part_range t1, part_range t2 WHERE t1.id = t2.id and t1.id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM part_list t1, part_list t2 WHERE t1.id_key = t2.id_key and t1.id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM part_multi t1, part_multi t2 WHERE t1.dpt = t2.dpt and t1.dpt = 2;
-- 2. Hypothetical tables
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range t1, hypo_part_range t2 WHERE t1.id = t2.id and t1.id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list t1, hypo_part_list t2 WHERE t1.id_key = t2.id_key and t1.id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi t1, hypo_part_multi t2 WHERE t1.dpt = t2.dpt and t1.dpt = 2;
-- 3. Real tables and hypothetical tables
EXPLAIN (COSTS OFF) SELECT * FROM part_range t1, hypo_part_range t2 WHERE t1.id = t2.id and t1.id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM part_list t1, hypo_part_list t2 WHERE t1.id_key = t2.id_key and t1.id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM part_multi t1, hypo_part_multi t2 WHERE t1.dpt = t2.dpt and t1.dpt = 2;

-- Tests for sanity checks
-- =======================

-- Duplicate name
CREATE TABLE part_range_1_10000 PARTITION OF part_range FOR VALUES FROM (1) TO (10000);
SELECT tablename FROM hypopg_add_partition('hypo_part_range_1_10000', 'PARTITION OF hypo_part_range FOR VALUES FROM (1) TO (10000)');
-- Overlapping range bounds
CREATE TABLE part_range_1_10000_dup PARTITION OF part_range FOR VALUES FROM (1) TO (10000);
SELECT tablename FROM hypopg_add_partition('hypo_part_range_1_10000_dup', 'PARTITION OF hypo_part_range FOR VALUES FROM (1) TO (10000)');
-- Overlapping list bounds
CREATE TABLE part_list_1_2_3_dup PARTITION OF part_list FOR VALUES IN (1, 2, 3);
SELECT tablename FROM hypopg_add_partition('hypo_part_list_1_2_3_dup', 'PARTITION OF hypo_part_list FOR VALUES IN (1, 2, 3)');
-- Overlapping range bounds, subpartition
CREATE TABLE part_multi_1_q1_a PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$);
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_a_dup', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$)');

-- relcache callback test
-- ======================

SELECT tablename FROM hypopg_table() WHERE tablename LIKE 'hypo_part_range%' ORDER BY tablename COLLATE "C";
DROP TABLE hypo_part_range;
SELECT tablename FROM hypopg_table() WHERE tablename LIKE 'hypo_part_range%' ORDER BY tablename COLLATE "C";
CREATE TABLE hypo_part_range (id integer, val text);
INSERT INTO hypo_part_range SELECT i, 'line ' || i FROM generate_series(1, 29999) i;
SELECT * FROM hypopg_partition_table('hypo_part_range', 'PARTITION BY RANGE (id)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_10000_20000', 'PARTITION OF hypo_part_range FOR VALUES FROM (10000) TO (20000)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_20000_30000', 'PARTITION OF hypo_part_range FOR VALUES FROM (20000) TO (30000)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_1_10000', 'PARTITION OF hypo_part_range FOR VALUES FROM (1) TO (10000)');
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range WHERE id = 42;

-- no UPDATE/DELETE test
-- =====================
-- simple UPDATE and DELETE on hypothetically partitioned table
EXPLAIN (COSTS OFF) UPDATE hypo_part_range set id = id;
EXPLAIN DELETE FROM hypo_part_range WHERE id = 42;
-- UPDATE and DELETE on hypothetically partitioned table inside CTE
EXPLAIN (COSTS OFF) WITH s AS (UPDATE hypo_part_range set id = id returning *) SELECT 1;
EXPLAIN (COSTS OFF) WITH s AS (DELETE FROM hypo_part_range WHERE id = 42 returning *) SELECT 1;
-- UPDATE and DELETE involving hypothetically partitioned table, but on regular
-- tables
CREATE TABLE foo(id integer);
-- UPDATE on non hypothetically partitioned table but having a hypothetically
-- partitioned table joined
EXPLAIN (COSTS OFF) WITH s AS (UPDATE foo SET id = 0 from hypo_part_range WHERE foo.id = hypo_part_range.id AND hypo_part_range.id > 25000 RETURNING *) SELECT 1;
-- same but with real table
EXPLAIN (COSTS OFF) WITH s AS (UPDATE foo SET id = 0 from part_range WHERE foo.id = part_range.id AND part_range.id > 25000 RETURNING *) SELECT 1;
-- DELETE on non hypothetically partitioned table but having a hypothetically
-- partitioned table joined
EXPLAIN (COSTS OFF) WITH s AS (DELETE FROM foo USING hypo_part_range WHERE foo.id = hypo_part_range.id AND hypo_part_range.id = 42 RETURNING *) SELECT 1;
-- same but with real table
EXPLAIN (COSTS OFF) WITH s AS (DELETE FROM foo USING part_range WHERE foo.id = part_range.id AND part_range.id = 42 RETURNING *) SELECT 1;

-- childless partitioning
-- ======================
SELECT * FROM hypopg_reset();
DROP TABLE part_multi;

CREATE TABLE part_multi(dpt smallint, dt date, val text) PARTITION BY LIST (dpt);
SELECT * FROM hypopg_partition_table('hypo_part_multi', 'PARTITION BY LIST (dpt)');

EXPLAIN (COSTS OFF) SELECT * FROM part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;

CREATE TABLE part_multi_2 PARTITION OF part_multi FOR VALUES IN (2) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_2_q1 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_1 PARTITION OF part_multi FOR VALUES IN (1) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_1_q1 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$) PARTITION BY RANGE (dt);
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2', 'PARTITION OF hypo_part_multi FOR VALUES IN (2)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q1', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1', 'PARTITION OF hypo_part_multi FOR VALUES IN (1)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)','PARTITION BY RANGE (dt)');

EXPLAIN (COSTS OFF) SELECT * FROM part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;

-- removing hypothetical partitions
-- ================================
CREATE TABLE part_multi_1_q1_b PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-02-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_1_q1_a PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$);
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_b', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-02-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_a', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$)');

DROP TABLE part_multi_1_q1_a;
SELECT hypopg_drop_table(relid) FROM hypopg_table() WHERE tablename = 'hypo_part_multi_1_q1_a';

EXPLAIN (COSTS OFF) SELECT * FROM part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;

DROP TABLE part_multi_1;
SELECT hypopg_drop_table(relid) FROM hypopg_table() WHERE tablename = 'hypo_part_multi_1';

EXPLAIN (COSTS OFF) SELECT * FROM part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;

SELECT hypopg_drop_table(relid) FROM hypopg_table() WHERE tablename = 'hypo_part_multi';

EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;

SELECT hypopg_drop_table(oid) FROM pg_class WHERE relname = 'pg_class';
SELECT hypopg_drop_table(1);
