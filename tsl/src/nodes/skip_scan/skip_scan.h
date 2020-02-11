/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H
#define TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H

#include <postgres.h>
#include <access/relscan.h>
#include <access/skey.h>
#include <nodes/execnodes.h>
#include <optimizer/planner.h>

typedef enum SkipScanStage
{
	SkipScanSearchingForFirst = 0x0,
	SkipScanFoundNull = 0x1,
	SkipScanFoundVal = 0x2,
	SkipScanSearchingForAdditional = 0x4,

	SkipScanSearchingForNull = SkipScanSearchingForAdditional | SkipScanFoundVal,
	SkipScanSearchingForVal = SkipScanSearchingForAdditional | SkipScanFoundNull,

	SkipScanFoundNullAndVal = SkipScanFoundVal | SkipScanFoundNull,
} SkipScanStage;

typedef struct SkipScanState
{
	CustomScanState cscan_state;
	IndexScanDesc *scan_desc;
	MemoryContext ctx;

	/* Interior Index(Only)Scan the SkipScan runs over */
	ScanState *idx;

	/* Pointers into the Index(Only)Scan */
	int *num_scan_keys;
	ScanKey *scan_keys;
	Buffer *index_only_buffer;
	bool *reached_end;

	Datum prev_distinct_val;
	bool prev_is_null;

	/* Info about the type we are performing DISTINCT on */
	bool distinct_by_val;
	int distinct_col_attnum;
	int distinct_typ_len;

	SkipScanStage stage;

	ScanKeyData skip_qual;
	int skip_qual_offset;
	bool skip_qual_removed;

	bool index_only_scan;

	Relation index_rel;
	void *idx_scan;
} SkipScanState;

typedef struct SkipScanPath
{
	CustomPath cpath;
	IndexPath *index_path;

	/* Index clause which we'll use to skip past elements we've already seen */
	RestrictInfo *skip_clause;
	/* The column offset, on the index, of the column we are calling DISTINCT on */
	int distinct_column;
	int distinct_typ_len;
	bool distinct_by_val;
} SkipScanPath;

extern void ts_add_skip_scan_paths(PlannerInfo *root, RelOptInfo *output_rel);
extern Node *ts_skip_scan_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_TSL_NODES_SKIP_SKAN_PLANNER_H */
