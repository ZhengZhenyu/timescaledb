-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- SkipSkan over IndexScan
:PREFIX SELECT time, dev, val, 'a' FROM (SELECT DISTINCT ON (dev) * FROM test_table) a;

-- SkipSkan over IndexOnlyScan
:PREFIX SELECT NULL, dev, NULL, 'b' FROM (SELECT DISTINCT ON (dev) dev FROM test_table) a;
:PREFIX SELECT time, dev, NULL, 'c' FROM (SELECT DISTINCT ON (dev) dev, time FROM test_table) a;


-- SkipSkan with NULLS FIRST
:PREFIX SELECT time, dev, val, 'd' FROM (SELECT DISTINCT ON (dev) * FROM test_table ORDER BY dev NULLS FIRST) a;
:PREFIX SELECT NULL, dev, NULL, 'e' FROM (SELECT DISTINCT ON (dev) dev FROM test_table ORDER BY dev NULLS FIRST) a;
:PREFIX SELECT time, dev, NULL, 'f' FROM (SELECT DISTINCT ON (dev) dev, time FROM test_table ORDER BY dev NULLS FIRST) a;

-- ORDER BY
:PREFIX SELECT time, dev, val, 'oba' FROM (SELECT DISTINCT ON (dev) * FROM test_table ORDER BY dev, time) a;
:PREFIX SELECT time, dev, val, 'obd' FROM (SELECT DISTINCT ON (dev) * FROM test_table ORDER BY dev DESC, time DESC) a;

-- WHERE CLAUSES
:PREFIX SELECT time, dev, val, 'g' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE dev > 5) a;
:PREFIX SELECT time, dev, val, 'h' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE time > 5) a;
:PREFIX SELECT time, dev, val, 'i' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE val > 5) a;

:PREFIX SELECT NULL, dev, NULL, 'j' FROM (SELECT DISTINCT ON (dev) dev FROM test_table WHERE dev > 5) a;
:PREFIX SELECT NULL, dev, NULL, 'k' FROM (SELECT DISTINCT ON (dev) dev FROM test_table WHERE time > 5) a;
:PREFIX SELECT NULL, dev, NULL, 'l' FROM (SELECT DISTINCT ON (dev) dev FROM test_table WHERE val > 5) a;

:PREFIX SELECT time, dev, val, 'm' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE dev != 5) a;
:PREFIX SELECT time, dev, val, 'n' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE time != 5) a;
:PREFIX SELECT time, dev, val, 'o' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE val != 5) a;
:PREFIX SELECT time, dev, val, 'p' FROM (SELECT DISTINCT ON (dev) * FROM test_table WHERE dev >= 1 + 1) a;

-- ReScan tests
:PREFIX SELECT time, dev, val, 'q' FROM (SELECT DISTINCT ON (dev) * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT * FROM test_table WHERE time != a.v) b) a;

:PREFIX SELECT time, dev, val, 'r' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM test_table WHERE dev != a.v) b) a;

-- RuntimeKeys
:PREFIX SELECT time, dev, val, 's' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM test_table WHERE dev >= a.v) b) c;


-- SkipSkan on later keys of an index
:PREFIX SELECT time, NULL, NULL, 't' FROM (SELECT DISTINCT ON (time) time FROM test_table WHERE dev = 2) a;
:PREFIX SELECT time, dev, val, 'u' FROM (SELECT DISTINCT ON (time) * FROM test_table WHERE dev = 2) a;

-- Emulate multi-column DISTINCT using multiple SkipSkans
:PREFIX SELECT time, dev, val, 'v' FROM (SELECT b.* FROM
    (SELECT DISTINCT ON (dev) dev FROM test_table) a,
    LATERAL (SELECT DISTINCT ON (time) * FROM test_table WHERE dev = a.dev) b) c;

:PREFIX SELECT time, dev, NULL, 'w' FROM (SELECT b.* FROM
    (SELECT DISTINCT ON (dev) dev FROM test_table) a,
    LATERAL (SELECT DISTINCT ON (time) dev, time FROM test_table WHERE dev = a.dev) b) c;


--------------------------
--------------------------
---- hypertable tests ----
--------------------------
--------------------------

-- SkipSkan over IndexScan
:PREFIX SELECT time, dev, val, 'a' FROM (SELECT DISTINCT ON (dev) * FROM test_ht) a;

-- SkipSkan over IndexOnlyScan
:PREFIX SELECT NULL, dev, NULL, 'b' FROM (SELECT DISTINCT ON (dev) dev FROM test_ht) a;
:PREFIX SELECT time, dev, NULL, 'c' FROM (SELECT DISTINCT ON (dev) dev, time FROM test_ht) a;

-- SkipSkan with NULLS FIRST
:PREFIX SELECT time, dev, val, 'd' FROM (SELECT DISTINCT ON (dev) * FROM test_ht ORDER BY dev NULLS FIRST) a;
:PREFIX SELECT NULL, dev, NULL, 'e' FROM (SELECT DISTINCT ON (dev) dev FROM test_ht ORDER BY dev NULLS FIRST) a;
:PREFIX SELECT time, dev, NULL, 'f' FROM (SELECT DISTINCT ON (dev) dev, time FROM test_ht ORDER BY dev NULLS FIRST) a;

-- ORDER BY
:PREFIX SELECT time, dev, val, 'oba' FROM (SELECT DISTINCT ON (dev) * FROM test_ht ORDER BY dev, time) a;
:PREFIX SELECT time, dev, val, 'obd' FROM (SELECT DISTINCT ON (dev) * FROM test_ht ORDER BY dev DESC, time DESC) a;

-- WHERE CLAUSES
:PREFIX SELECT time, dev, val, 'g' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE dev > 5) a;
:PREFIX SELECT time, dev, val, 'h' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE time > 5) a;
:PREFIX SELECT time, dev, val, 'i' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE val > 5) a;

:PREFIX SELECT NULL, dev, NULL, 'j' FROM (SELECT DISTINCT ON (dev) dev FROM test_ht WHERE dev > 5) a;
:PREFIX SELECT NULL, dev, NULL, 'k' FROM (SELECT DISTINCT ON (dev) dev FROM test_ht WHERE time > 5) a;
:PREFIX SELECT NULL, dev, NULL, 'l' FROM (SELECT DISTINCT ON (dev) dev FROM test_ht WHERE val > 5) a;

:PREFIX SELECT time, dev, val, 'm' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE dev != 5) a;
:PREFIX SELECT time, dev, val, 'n' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE time != 5) a;
:PREFIX SELECT time, dev, val, 'o' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE val != 5) a;
:PREFIX SELECT time, dev, val, 'p' FROM (SELECT DISTINCT ON (dev) * FROM test_ht WHERE dev >= 1 + 1) a;

-- ReScan tests
:PREFIX SELECT time, dev, val, 'q' FROM (SELECT DISTINCT ON (dev) * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT * FROM test_ht WHERE time != a.v) b) a;

:PREFIX SELECT time, dev, val, 'r' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM test_ht WHERE dev != a.v) b) a;

-- RuntimeKeys
:PREFIX SELECT time, dev, val, 's' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM test_ht WHERE dev >= a.v) b) c;

-- SkipSkan on later keys of an index
:PREFIX SELECT time, NULL, NULL, 't' FROM (SELECT DISTINCT ON (time) time FROM test_ht WHERE dev = 2) a;
:PREFIX SELECT time, dev, val, 'u' FROM (SELECT DISTINCT ON (time) * FROM test_ht WHERE dev = 2) a;

-- Emulate multi-column DISTINCT using multiple SkipSkans
:PREFIX SELECT time, dev, val, 'v' FROM (SELECT b.* FROM
    (SELECT DISTINCT ON (dev) dev FROM test_ht) a,
    LATERAL (SELECT DISTINCT ON (time) * FROM test_ht WHERE dev = a.dev) b) c;

:PREFIX SELECT time, dev, NULL, 'w' FROM (SELECT b.* FROM
    (SELECT DISTINCT ON (dev) dev FROM test_ht) a,
    LATERAL (SELECT DISTINCT ON (time) dev, time FROM test_ht WHERE dev = a.dev) b) c;
