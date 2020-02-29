# MEDB
## Milestone 1
Supports update, select, delete, and sum.

Rather unclean. Too much repetitive code. Documentation style is also fairly inconsistent. Readability suffers at certain points. This can be fixed by wrapping functions around frequently repeated operations.

Maintains a hash-table based index over the key column specified when creating the table. There is currently no support for creating multiple indexes over multiple columns. For this milestone, only one index is maintained at a time. The index can, however, be indexed over a column with duplicate values (i.e. not all key values have to be unique).

test script should be located in the same directory as the source code (just like tester.py). db.py should be imported using 'from db import Database', and query.py should be imported using 'from query import Query'.

## Milestone 2
Provides support for memory persistence. Also incorporates merging and central
buffer pool used by all tables/threads.

Insert and select performance scales with the size of the buffer pool. In other
words, the larger the buffer pool, the better the performance.

Performance for updates are, however, abysmal. This seems to be largely attributable to
the locking mechanisms used to regulate buffer pool access.

Some extraneous code has been removed and replced by function calls.
