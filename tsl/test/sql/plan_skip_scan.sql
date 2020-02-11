-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE test_table(time INT, dev INT, val INT);

INSERT INTO test_table SELECT t, d, random() FROM generate_series(1, 1000) t, generate_series(1, 10) d;
INSERT INTO test_table VALUES (NULL, 0, -1), (0, NULL, -1);

CREATE INDEX ON test_table(dev);
CREATE INDEX ON test_table(dev NULLS FIRST);
CREATE INDEX ON test_table(dev, time);


CREATE TABLE test_ht(time INT, dev INT, val INT);
SELECT create_hypertable('test_ht', 'time', chunk_time_interval => 250);

INSERT INTO test_ht SELECT t, d, random() FROM generate_series(1, 1000) t, generate_series(1, 10) d;
INSERT INTO test_ht VALUES (0, NULL, -1);

CREATE INDEX ON test_ht(dev);
CREATE INDEX ON test_ht(dev NULLS FIRST);
CREATE INDEX ON test_ht(dev, time);


\set PREFIX 'EXPLAIN (COSTS OFF)'
\ir include/skip_scan_test_query.sql

-- don't SkipSkan with an invalid ORDER BY
:PREFIX SELECT time, dev, val, 'd' FROM (SELECT DISTINCT ON (dev) * FROM test_ht ORDER BY dev DESC, time) a;
:PREFIX SELECT time, dev, val, 'd' FROM (SELECT DISTINCT ON (dev) * FROM test_ht ORDER BY dev, time DESC) a;
