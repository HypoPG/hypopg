.. _usage:

Usage
=====

Introduction
------------

HypoPG is useful if you want to check if some index would help one or multiple
queries.  Therefore, you should already know what are the queries you need to
optimize, and ideas on which indexes you want to try.

Also, the hypothetical indexes that HypoPG will create are not stored in any
catalog, but in your connection private memory.  Therefore, it won't bloat any
table and won't impact any concurrent connection.

Also, since the hypothetical indexes doesn't really exists, HypoPG makes sure
they will only be used using a simple EXPLAIN statement (without the ANALYZE
option).

Install the extension
---------------------

As any other extension, you have to install it on all the databases where you
want to be able to use it.  This is simply done executing the following query,
connected on the database you want to install HypoPG with a user having enough
privileges:

.. code-block:: psql

  CREATE EXTENSION hypopg ;

HypoPG is now available.  You can check easily if the extension is present
using `psql <https://www.postgresql.org/docs/current/static/app-psql.html>`_:

.. code-block:: psql
  :emphasize-lines: 5

  \dx
                       List of installed extensions
    Name   | Version |   Schema   |             Description
  ---------+---------+------------+-------------------------------------
   hypopg  | 1.1.0   | public     | Hypothetical indexes for PostgreSQL
   plpgsql | 1.0     | pg_catalog | PL/pgSQL procedural language
  (2 rows)

As you can see, hypopg version 1.1.0 is installed.  If you need to check using
plain SQL, please refer to the `pg_extension table documentation
<https://www.postgresql.org/docs/current/static/catalog-pg-extension.html>`_.

Create a hypothetical index
---------------------------

.. note::

  Using HypoPG require some knowledge on the **EXPLAIN** command.  If you need
  more information about this command, you can check `the official
  documentation
  <https://www.postgresql.org/docs/current/static/using-explain.html>`_.  There
  are also a lot of very good resources available.

For clarity, let's see how it works with a very simple test case:

.. code-block:: psql

  CREATE TABLE hypo (id integer, line text) ;
  INSERT INTO hypo SELECT i, 'line ' || i FROM generate_series(1, 100000) i ;
  VACUUM ANALYZE hypo ;

This table doesn't have any index.  Let's assume we want to check if an index
would help a simple query.  First, let's see how it behaves:

.. code-block:: psql

  EXPLAIN SELECT val FROM hypo WHERE id = 1;
                         QUERY PLAN
  --------------------------------------------------------
   Seq Scan on hypo  (cost=0.00..1791.00 rows=1 width=14)
     Filter: (id = 1)
  (2 rows)

A plain sequential scan is used, since no index exists on the table.  A simple
btree index on the **id** column should help this query.  Let's check with
HypoPG.  The function **hypopg_create_index()** will accept any standard
**CREATE INDEX** statement(s) (any other statement passed to this function will be
ignored), and create a hypothetical index for each:

.. code-block:: psql

  SELECT * FROM hypopg_create_index('CREATE INDEX ON hypo (id)') ;
   indexrelid |      indexname
  ------------+----------------------
        18284 | <18284>btree_hypo_id
  (1 row)

The function returns two columns:

- the object identifier of the hypothetical index
- the generated hypothetical index name

We can run the EXPLAIN again to see if PostgreSQL would use this index:

.. code-block:: psql
  :emphasize-lines: 4

  EXPLAIN SELECT val FROM hypo WHERE id = 1;
                                      QUERY PLAN
  ----------------------------------------------------------------------------------
   Index Scan using <18284>btree_hypo_id on hypo  (cost=0.04..8.06 rows=1 width=10)
     Index Cond: (id = 1)
  (2 rows)

Yes, PostgreSQL would use such an index.  Just to be sure, let's check that the
hypothetical index won't be used to acually run the query:

.. code-block:: psql

  EXPLAIN ANALYZE SELECT val FROM hypo WHERE id = 1;
                                              QUERY PLAN
  ---------------------------------------------------------------------------------------------------
   Seq Scan on hypo  (cost=0.00..1791.00 rows=1 width=10) (actual time=0.046..46.390 rows=1 loops=1)
     Filter: (id = 1)
     Rows Removed by Filter: 99999
   Planning time: 0.160 ms
   Execution time: 46.460 ms
  (5 rows)

That's all you need to create hypothetical indexes and see if PostgreSQL would
use such indexes.

Manipulate hypothetical indexes
-------------------------------

Some other convenience functions are available:

- **hypopg_list_indexes()**: list all hypothetical indexes that have been
  created

.. code-block:: psql

  SELECT * FROM hypopg_list_indexes()
   indexrelid |      indexname       | nspname | relname | amname
  ------------+----------------------+---------+---------+--------
        18284 | <18284>btree_hypo_id | public  | hypo    | btree
  (1 row)

- **hypopg_get_indexdef(oid)**: get the CREATE INDEX statement that would
  recreate a stored hypothetical index

.. code-block:: psql

  SELECT indexname, hypopg_get_indexdef(indexrelid) FROM hypopg_list_indexes() ;
        indexname       |             hypopg_get_indexdef              
  ----------------------+----------------------------------------------
   <18284>btree_hypo_id | CREATE INDEX ON public.hypo USING btree (id)
  (1 row)

- **hypopg_relation_size(oid)**: estimate how big a hypothetical index would
  be:

.. code-block:: psql

  SELECT indexname, pg_size_pretty(hypopg_relation_size(indexrelid))
    FROM hypopg_list_indexes() ;
        indexname       | pg_size_pretty
  ----------------------+----------------
   <18284>btree_hypo_id | 2544 kB
  (1 row)

- **hypopg_drop_index(oid)**: remove the given hypothetical index
- **hypopg_reset()**: remove all hypothetical indexes

Hypothetical partitioning
-------------------------

.. note::

   This is only possible for PostgreSQL 10 an above.  The partitioning
   possibilites depend on the PostgreSQL version.  For instance, you can't
   create a hypothetical hash partition on using PostgreSQL 10.


For clarity, let's see how it works with a very simple test case:

.. code-block:: psql

  CREATE TABLE hypo_part_range (id integer, val text);
  INSERT INTO hypo_part_range SELECT i, 'line ' || i FROM generate_series(1, 29999) i;

This is a simple table, containing some rows and without indexes.  Trying to
retrieve a row will do as expected:

.. code-block:: psql

  EXPLAIN SELECT * FROM hypo_part_range WHERE id = 2;
                              QUERY PLAN                            
  ------------------------------------------------------------------
   Seq Scan on hypo_part_range  (cost=0.00..537.99 rows=1 width=14)
     Filter: (id = 2)
  (2 rows)

Now, let's try to hypothetically partition this table with a range partitioning
scheme.  For that, we have two functions:

- **hypopg_partition_table**: it has two mandatory arguments.  The first
  argument is the table to be hypothetically partitioned, and the second is the
  `PARTITION BY` clause, as you would use for declarative partitioning
- **hypopg_add_partition**: it has two mandatory arguments, and one optional.
  The first mandatory argument is the partitiong name, the second is the
  `PARTITION OF` clause, and the optional argument is a `PARTITION BY` clause,
  if you want to declare multiple level of partitioning.

For instance:

.. code-block:: psql

  SELECT hypopg_partition_table('hypo_part_range', 'PARTITION BY RANGE(id)');
  SELECT tablename FROM hypopg_add_partition('hypo_part_range_1_10000', 'PARTITION OF hypo_part_range FOR VALUES FROM (1) TO (10000)');
  SELECT tablename FROM hypopg_add_partition('hypo_part_range_10000_20000', 'PARTITION OF hypo_part_range FOR VALUES FROM (10000) TO (20000)');
  SELECT tablename FROM hypopg_add_partition('hypo_part_range_20000_30000', 'PARTITION OF hypo_part_range FOR VALUES FROM (20000) TO (30000)');

.. note::

  If you need to declare bounds on a textual column, the dollar-quoting
  notation will be helpful.  For instance:

  .. code-block:: psql

    SELECT hypopg_add_partition('p_name', $$PARTITION OF tbl FOR VALUES FROM 'aaa' TO 'aab'$$);

Now, let's see what happens if we try to retrieve a row of the hypothetically
partitioned table:

.. code-block:: psql

  EXPLAIN SELECT * FROM hypo_part_range WHERE id = 2;
                                             QUERY PLAN                                           
  ------------------------------------------------------------------------------------------------
   Append  (cost=0.00..179.95 rows=1 width=14)
     ->  Seq Scan on hypo_part_range hypo_part_range_1_10000  (cost=0.00..179.95 rows=1 width=14)

We can see that since there's an Append node, PostgreSQL acted as if the table
was partitioned, and that all but one partition was pruned.

It's also possible to create a hypothetical index on the hypothetical
partitions:


.. code-block:: psql

  SELECT hypopg_create_index('CREATE INDEX on hypo_part_range_1_10000 (id)');
                                                                    QUERY PLAN                                                                   
  -----------------------------------------------------------------------------------------------------------------------------------------------
   Append  (cost=0.04..8.06 rows=1 width=14)
     ->  Index Scan using <258199>btree_hypo_part_range_1_10000_id on hypo_part_range hypo_part_range_1_10000  (cost=0.04..8.05 rows=1 width=14)
           Index Cond: (id = 2)
  (3 rows)

Manipulate hypothetical partitions
----------------------------------

Some other convenience functions are available:

- **hypopg_table()**: list all hypothetical partitions that have been created
- **hypopg_analyze(regclass, fraction)**: perform an operation similar to
  ANALYZE on a hypothetically partitioned table, to get better estimates
- **hypopg_statistic()**: returns the list of statistics gathered by
  previous runs of **hypopg_analyze**, in the same format as `pg_statistic`.
  For an easier reading, the view **hypopg_stats** exists, which returns the
  data in the same format as `pg_stats`
- **hypopg_drop_table(oid)**: delete a previously created partition, or unpartition
  a hypothetically partitioned table (including the stored statistics if any)
- **hypopg_reset_table()**: remove all previously created hypothetical partition
  (inclufing the stored statistics if any)
