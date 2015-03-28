# cassandra_util

Python utilities for cassandra

## cassandra_util.concurrent

At this point in time the only module provided. It provides a near drop in replacement for cassandra.execute_concurrent which is able to execute statements concurrently with a memory footprint in O(concurrency) instead of O(statements). This yields orders of magnitude less memory usage and throughput with large numbers of statements.

```python
insert = session.prepare('insert into tbl (key, value) values (?, ?)')

execute_concurrent(
    session,
    ((insert, ('key', i)) for i in xrange(1000 * 1000))
)
```
