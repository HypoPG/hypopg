-- Test for pg versions that don't support declarative partitioning
-- ================================================================

-- 0. Dropping any hypothetical object
SELECT * FROM hypopg_reset();
-- 1. partition test
CREATE TABLE hypo_part_range (id integer, val text);
SELECT * FROM hypopg_partition_table('hypo_part_range', 'PARTITION BY RANGE (id)');
SELECT tablename FROM hypopg_add_partition('hypo_part_range_1_10000', 'PARTITION OF hypo_part_range FOR VALUES FROM (1) TO (10000)');
-- 2. other functions
SELECT * FROM hypopg_reset_table();
SELECT * FROM hypopg_drop_table('hypo_part_range'::regclass);
SELECT * FROM hypopg_table();
SELECT * FROM hypopg_analyze('hypo_part_range');
SELECT * FROM hypopg_statistic();
SELECT * FROM hypopg_stats;
