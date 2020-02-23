.. _hypothetical_objects:

Hypothetical objects
====================

HypoPG support two kinds of hypothetical objects: *hypothetical indexes* and
*hypothetical partitioning*.

.. _hypothetical_indexes:

Hypothetical Indexes
--------------------
A hypothetical, or virtual, index is an index that does not really exist, and
therefore does not cost CPU, disk or any resource to create. They are useful to find out
whether specific indexes can increase the performance for problematic queries, since
you can discover if PostgreSQL will use these indexes or not without having to
spend resources to create them.

.. _hypothetical_partitioning:

Hypothetical Partitioning
-------------------------
Hypothetical partitioning, available for PostgreSQL servers version 10 and
later, is a real table to which you can hypothetically apply partitining
schemes as you would do for declarative partitioning. PostgreSQL will act as if
this table was really partitioned, so you can quickly test multiple
partitioning schemes, and you can see how each will change your query's
behavior and check which one is the best for your specific application.
