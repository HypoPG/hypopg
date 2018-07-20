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
CREATE TABLE part_list_1_2_3 PARTITION OF part_list FOR VALUES IN (1, 2, 3);
CREATE TABLE part_list_4_5_6_8_10 PARTITION OF part_list FOR VALUES IN (4, 5, 6, 8, 10);
CREATE TABLE part_list_7_9 PARTITION OF part_list FOR VALUES IN (7, 9);
INSERT INTO part_list SELECT i, (i % 9) + 1, 'line ' || i FROM generate_series(1, 50000) i;
-- 3. Hash partitioning
DROP TABLE IF EXISTS part_hash;
CREATE TABLE part_hash (id integer, val text) PARTITION BY HASH (id);
CREATE TABLE part_hash_0 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 0);
CREATE TABLE part_hash_1 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 1);
CREATE TABLE part_hash_2 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 2);
CREATE TABLE part_hash_3 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 3);
CREATE TABLE part_hash_4 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 4);
CREATE TABLE part_hash_5 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 5);
CREATE TABLE part_hash_6 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 6);
CREATE TABLE part_hash_7 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 7);
CREATE TABLE part_hash_8 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 8);
CREATE TABLE part_hash_9 PARTITION OF part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 9);
INSERT INTO part_hash SELECT i, 'line ' || i FROM generate_series(1, 90000) i;
-- 4. Multi level range
DROP TABLE IF EXISTS part_multi;
CREATE TABLE part_multi(dpt smallint, dt date, val text) PARTITION BY LIST (dpt);
CREATE TABLE part_multi_1 PARTITION OF part_multi FOR VALUES IN (1) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_1_q1 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$) PARTITION BY RANGE (dt);
CREATE TABLE part_multi_1_q1_a PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$);
CREATE TABLE part_multi_1_q1_b PARTITION OF part_multi_1_q1 FOR VALUES FROM ($$2015-02-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_1_q2 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$);
CREATE TABLE part_multi_1_q3 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$);
CREATE TABLE part_multi_1_q4 PARTITION OF part_multi_1 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$);
CREATE TABLE part_multi_2 PARTITION OF part_multi FOR VALUES IN (2) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_2_q1 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_2_q2 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$);
CREATE TABLE part_multi_2_q3 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$);
CREATE TABLE part_multi_2_q4 PARTITION OF part_multi_2 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$);
CREATE TABLE part_multi_3 PARTITION OF part_multi FOR VALUES IN (3) PARTITION BY RANGE(dt);
CREATE TABLE part_multi_3_q1 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$);
CREATE TABLE part_multi_3_q2 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$);
CREATE TABLE part_multi_3_q3 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$);
CREATE TABLE part_multi_3_q4 PARTITION OF part_multi_3 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$);
INSERT INTO part_multi select (i%3)+1, '2015-01-01'::date + interval '1 day' * (i%365), 'val ' || i FROM generate_series(1,10000) i;

-- Hypothetical tables
-- -------------------
-- 0. Dropping any hypothetical object
SELECT * FROM hypopg_reset_index();
SELECT * FROM hypopg_reset_table();
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
SELECT tablename FROM hypopg_add_partition('hypo_part_list_1_2_3', 'PARTITION OF hypo_part_list FOR VALUES IN (1, 2, 3)');
SELECT tablename FROM hypopg_add_partition('hypo_part_list_4_5_6_8_10', 'PARTITION OF hypo_part_list FOR VALUES IN (4, 5, 6, 8, 10)');
SELECT tablename FROM hypopg_add_partition('hypo_part_list_7_9', 'PARTITION OF hypo_part_list FOR VALUES IN (7, 9)');
-- 3. Hash partitioning
DROP TABLE IF EXISTS hypo_part_hash;
CREATE TABLE hypo_part_hash (id integer, val text);
INSERT INTO hypo_part_hash SELECT i, 'line ' || i FROM generate_series(1, 90000) i;
SELECT * FROM hypopg_partition_table('hypo_part_hash', 'PARTITION BY HASH (id)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_0', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 0)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_1', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 1)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_2', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 2)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_3', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 3)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_4', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 4)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_5', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 5)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_6', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 6)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_7', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 7)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_8', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 8)');
SELECT tablename FROM hypopg_add_partition('hypo_part_hash_9', 'PARTITION OF hypo_part_hash FOR VALUES WITH (MODULUS 10, REMAINDER 9)');
-- 4. Multi level range
DROP TABLE IF EXISTS hypo_part_multi;
CREATE TABLE hypo_part_multi(dpt smallint, dt date, val text);
INSERT INTO hypo_part_multi select (i%3)+1, '2015-01-01'::date + interval '1 day' * (i%365), 'val ' || i FROM generate_series(1,10000) i;
SELECT * FROM hypopg_partition_table('hypo_part_multi', 'PARTITION BY LIST (dpt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1', 'PARTITION OF hypo_part_multi FOR VALUES IN (1)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)','PARTITION BY RANGE (dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_a', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-02-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q1_b', 'PARTITION OF hypo_part_multi_1_q1 FOR VALUES FROM ($$2015-02-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q2', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q3', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_1_q4', 'PARTITION OF hypo_part_multi_1 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2', 'PARTITION OF hypo_part_multi FOR VALUES IN (2)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q1', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q2', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q3', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_2_q4', 'PARTITION OF hypo_part_multi_2 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3', 'PARTITION OF hypo_part_multi FOR VALUES IN (3)', 'PARTITION BY RANGE(dt)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q1', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-01-01$$) TO ($$2015-04-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q2', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-04-01$$) TO ($$2015-07-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q3', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-07-01$$) TO ($$2015-10-01$$)');
SELECT tablename FROM hypopg_add_partition('hypo_part_multi_3_q4', 'PARTITION OF hypo_part_multi_3 FOR VALUES FROM ($$2015-10-01$$) TO ($$2016-01-01$$)');

-- Maintenance
-- -----------
VACUUM ANALYZE;

-- Test deparsing
-- ==============
SELECT relid = rootid AS is_root, tablename, parentid IS NULL parentid_is_null,
    parentid IS NOT NULL AS parentid_is_not_null,
    partition_by_clause, partition_bounds
FROM hypopg_table();

-- Test hypothetical partitioning behavior
-- =======================================

-- Simple queries
-- --------------

-- Real tables
-- -----------
-- 1. Range partition
EXPLAIN (COSTS OFF) SELECT * FROM part_range;
EXPLAIN (COSTS OFF) SELECT * FROM part_range WHERE id = 42;
EXPLAIN (COSTS OFF) SELECT * FROM part_range WHERE id < 15000;
-- 2. List partitioning
EXPLAIN (COSTS OFF) SELECT * FROM part_list;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id < 42;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM part_list WHERE id_key = 7;
-- 3. Hash partitioning
EXPLAIN (COSTS OFF) SELECT * FROM part_hash;
EXPLAIN (COSTS OFF) SELECT * FROM part_hash WHERE id = 42;
EXPLAIN (COSTS OFF) SELECT * FROM part_hash WHERE id < 15000;
-- 4. Multi level range
EXPLAIN (COSTS OFF) SELECT * FROM part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM part_multi WHERE dpt = 2;
EXPLAIN (COSTS OFF) SELECT * FROM part_multi WHERE dt >= '2015-01-05' AND dt < '2015-01-10';

-- Hypothetical tables
-- ------------------"
-- 1. Range partition
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range WHERE id = 42;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_range WHERE id < 15000;
-- 2. List partitioning
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id < 42;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id < 15000;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id_key < 5;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_list WHERE id_key = 7;
-- 3. Hash partitioning
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_hash;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_hash WHERE id = 42;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_hash WHERE id < 15000;
-- 4. Multi level range
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi WHERE dpt = 2;
EXPLAIN (COSTS OFF) SELECT * FROM hypo_part_multi WHERE dt >= '2015-01-05' AND dt < '2015-01-10';
