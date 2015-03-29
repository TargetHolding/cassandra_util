# cassandra_util

Python utilities for cassandra

## cassandra_util.concurrent

At this point in time the only module provided. It provides a near drop in replacement for cassandra.execute_concurrent which is able to execute statements concurrently with a memory footprint in O(concurrency) instead of O(statements). This yields orders of magnitude less memory usage and throughput with large numbers of statements.

This implementation has been offered for integration in the python driver for Cassandra from Datastax: [https://datastax-oss.atlassian.net/browse/PYTHON-274].

### Examples
The examples below execute with a very limited and constant memory footprint, while the original implementation consumes > 200 megabytes to start with and which quickly grows as statements are being executed.

#### Bulk insertion
Insert values from a generator:
```python
insert = session.prepare('insert into tbl (key, value) values (?, ?)')
execute_concurrent(session, ((insert, ('key', i)) for i in xrange(1000 * 1000)))
```

#### Bulk selection
Calculate the sum where the select statements are yielded from a generator:
```python
select = session.prepare('select value from tbl where key = ?')
print sum(r[0].value for _, r in execute_concurrent(session, ((select, (str(i),)) for i in xrange(1000 *1000))))
```
