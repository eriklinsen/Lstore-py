# Lstore-py
_PLEASE BE ADVISED: While Lstore-py is certainly functional, the current codebase is in an undesirable state. Presently,
the codebase does not meet my standards for conciseness or readability. I'm currently in the process of updating and
refactoring the code you find here (as of 1/30/2020)._
## Milestone 1
Implemented support update, select, delete, and aggregate queries.

Implemented a hash-table based index over the key column specified when creating the table. There is currently no
support for creating multiple indices over multiple columns. For this milestone, only one index is maintained at a
time. The index can, however, be indexed over a column with duplicate values (i.e. not all key values have to be
unique).

The test script should be located in the same directory as the source code. db.py should be
imported using 'from db import Database', and query.py should be imported using 'from query import Query'.

## Milestone 2
The system has been made durable. All committed transactions will be written to disk. Merging has also been
incorporated along with a central buffer pool that is used by all tables/threads.

## Milestone 3
Implmenented support for transactions that are both atomic and isolated. This allows for the execution of
multi-statment transactions, such that all operations/statements in a transaction will be committed or none will (which
will result in an abort).
Additionally, Concurrency Control has been incorporated, which enables the concurrent execution of multiple
transactions.
