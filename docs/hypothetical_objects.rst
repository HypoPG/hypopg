.. _hypothetical_objects:

Hypothetical objects
====================

HypoPG support two kinds of hypothetical objects: hypothetical indexes and
hypothetical partitioning.

.. _hypothetical_indexes:

Hypothetical indexes
--------------------
A hypothetical, or virtual, index is an index that doesn't really exists, and
thus doesn't cost CPU, disk or any resource to create.  They're useful to know
if specific indexes can increase performance for problematic queries, since
you can know if PostgreSQL will use these indexes or not without having to
spend resources to create them.

.. _hypothetical_partitioning:

Hypothetical partitioning
-------------------------
Hypothetical partitioning, available for PostgreSQL servers version 10 and
above, is a real table on which you can hypothetically apply partitining
scheme as you would do for declarative partitioning.  PostgreSQL will act as if
this table was really partitioned, so you can know quickly test multiple
partitioning scheme, and you can see how each of them will change your queries
behavior and check which one is the best for your specific appliation.
