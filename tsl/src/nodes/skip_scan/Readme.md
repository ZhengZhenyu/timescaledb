# SkipScan #

This module implements a skip-scan; an optimization for `SELECT DISTINCT ON`.
Usually for `SELECT DISTINCT ON` Postgres will plan either a `UNIQUE` over a
sorted path, or some form of aggregate. In either case, it needs to scan the
entire table, even in cases where there are only a few unique values.

A skip-scan optimizes this case when we have an ordered index. Instead of
scanning the entire table and deduplicating after, the scan remembers the last
value returned, and searches the index for the next value after that one. This
means that for a table with `k` keys, with `u` distinct values, a skip-scan runs
in time `u * log(k)` as opposed to scanning then deduplicating, which takes time
`k`. We can write the number of unique values `u` as of function of `k` by
dividing by the number of repeats `r` i.e. `u = k/r` this means that a skip-scan
will be faster if each key is repeated more than a logarithmic number of times,
i.e. if `r > log(k)` then `u * log(k) < k/log(k) * log(k) < k`.


## Implementation ##

We plan our skip-scan with a tree something like

```SQL
Custom Scan (SkipScan) on table
   ->  Index Scan using table_key_idx on table
       Index Cond: (key > NULL)
```

After each iteration through the `SkipScan` we replace the `key > NULL` with
a `key > [next value we are returning]` and restart the underlying `IndexScan`.
There are some subtleties around `NULL` handling, see the source file for more
detail.


## Planning Heuristics ##

To plan our SkipScan we look for a compatible plan, for instance

```SQL
Unique
   ->  Index Scan
```

or

```SQL
Unique
   ->  Merge Append
         ->  Index Scan
         ...
```

given such a plan, we know the index is sorted in an order with the distinct
key(s) first, so we can add quals to the `IndexScan` representing the previous
key returned, and thus skip over the repeated values. The `Unique` node tells us
which columns are relevant.

We use this to create plans that look like

```SQL
Custom Scan (SkipScan)
   ->  Index Scan
       Index Cond: (column > [prev column]) ...
```

or

```SQL
Unique
   ->  Merge Append
         ->  Custom Scan (SkipScan)
                ->  Index Scan
                    Index Cond: (column > [prev column]) ...
         ...
```

respectively.


## Postgres-Native Skip Scan ##

Upstream postgres is also working on a skip-scan implementation, see e.g.
https://commitfest.postgresql.org/19/1741/
As when this document was first written, it is not yet merged. Their strategy
involves deep changes to the btree searching code, and thus is much more
invasive than ours, and will be available in PG13 at earliest. The two
implementations should not interfere with eachother.
