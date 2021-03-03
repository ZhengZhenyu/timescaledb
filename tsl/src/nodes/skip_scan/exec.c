/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * SkipScan is an optimized form of SELECT DISTINCT ON (column)
 * Conceptually, a SkipScan is a regular IndexScan with an additional skip-qual like
 *     WHERE column > [previous value of column]
 *
 * Implementing this qual is complicated by two factors:
 *   1. The first time through the SkipScan there is no previous value for the
 *      DISTINCT column.
 *   2. NULL values don't behave nicely with ordering operators.
 *
 * To get around these issues, we have to special case those two cases. All in
 * all, the SkipScan's state machine evolves according to the following flowchart
 *
 *                  start
 *                    |
 *        +========================+
 *        | search for first tuple |
 *        +========================+
 *           /               \
 *     found NULL         found value
 *        /                     \
 * +============+          +============+
 * | search for |--found-->| find value |
 * |  non-NULL  |  value   | after prev |
 * +============+          +============+
 *       |                        |
 *   found nothing           out of tuples
 *       |                        |
 *       |                  +=============+
 *  /===========\           | search for  |
 *  |   DONE    |<----------| NULL if one |
 *  \===========/           | hasn't been |
 *                          | found yet   |
 *                          +=============+
 *
 * We start by calling the underlying IndexScan once, without the skip qual, to
 * get the first tuple. If this tuple contains a NULL for our DISTINCT column, we
 * assume we might be using a NULLs FIRST index, and search again with a
 * `column IS NOT NULL` to see if we can find a real first value. If we find a
 * first non-NULL value, we keep fetching nodes, updating our
 * `column > [previous value of column]` all the while, until we run out of
 * tuples. Once we run out of tuples, if we have not yet seen a NULL value, we
 * search once more using `column IS NULL`, in case we are using a NULLs LAST
 * index, after which we are done.
 */

#include <postgres.h>
#include <executor/nodeIndexscan.h>
#include <executor/nodeIndexonlyscan.h>
#include <nodes/extensible.h>
#include <nodes/pg_list.h>
#include <storage/bufmgr.h>
#include <utils/datum.h>

#include "guc.h"
#include "nodes/skip_scan/skip_scan.h"

static void skip_scan_state_beginscan(SkipScanState *state);
static void skip_scan_state_fixup_qual_order(SkipScanState *state,
											 IndexRuntimeKeyInfo *runtime_keys,
											 int num_runtime_keys);
static void update_skip_key(SkipScanState *state, TupleTableSlot *slot);
static TupleTableSlot *skip_scan_search_for_null(SkipScanState *state);
static TupleTableSlot *skip_scan_search_for_nonnull(SkipScanState *state);

static void skip_scan_state_remove_skip_qual(SkipScanState *state);
static inline bool skip_scan_state_readd_skip_qual_if_needed(SkipScanState *state);
static inline void skip_scan_state_populate_skip_qual(SkipScanState *state);

/*********
 * Begin *
 *********/

static void
skip_scan_begin(CustomScanState *node, EState *estate, int eflags)
{
	SkipScanState *state = (SkipScanState *) node;
	IndexRuntimeKeyInfo *runtime_keys;
	int num_runtime_keys;
	if (IsA(state->idx_scan, IndexScan))
	{
		IndexScanState *idx =
			castNode(IndexScanState, ExecInitNode(state->idx_scan, estate, eflags));
		state->index_only_scan = false;

		node->custom_ps = list_make1(&idx->ss.ps);

		state->idx = &idx->ss;

		state->scan_keys = &idx->iss_ScanKeys;
		state->num_scan_keys = &idx->iss_NumScanKeys;
		state->index_rel = idx->iss_RelationDesc;
		state->scan_desc = &idx->iss_ScanDesc;
		state->index_only_buffer = NULL;
		state->reached_end = &idx->iss_ReachedEnd;

		runtime_keys = idx->iss_RuntimeKeys;
		num_runtime_keys = idx->iss_NumRuntimeKeys;

		/* we do not support orderByKeys out of conservatism; we do not know what,
		 * if any, work would be required to support them. The planner should
		 * never plan a SkipScan which would cause this ERROR.
		 */
		if (idx->iss_NumOrderByKeys > 0)
			elog(ERROR, "cannot SkipScan with OrderByKeys");
	}
	else if (IsA(state->idx_scan, IndexOnlyScan))
	{
		IndexOnlyScanState *idx =
			castNode(IndexOnlyScanState, ExecInitNode(state->idx_scan, estate, eflags));
		state->index_only_scan = true;

		node->custom_ps = list_make1(&idx->ss.ps);

		state->idx = &idx->ss;
		state->scan_keys = &idx->ioss_ScanKeys;
		state->num_scan_keys = &idx->ioss_NumScanKeys;
		state->index_rel = idx->ioss_RelationDesc;
		state->scan_desc = &idx->ioss_ScanDesc;
		state->index_only_buffer = &idx->ioss_VMBuffer;

		/* IndexOnlyScan does not have a reached_end field */
		state->reached_end = NULL;

		runtime_keys = idx->ioss_RuntimeKeys;
		num_runtime_keys = idx->ioss_NumRuntimeKeys;

		/* we do not support orderByKeys out of conservatism; we do not know what,
		 * if any, work would be required to support them.  The planner should
		 * never plan a SkipScan which would cause this ERROR.
		 */
		if (idx->ioss_NumOrderByKeys > 0)
			elog(ERROR, "cannot SkipScan with OrderByKeys");
	}
	else
		elog(ERROR, "unknown subscan type in SkipScan");

	state->prev_distinct_val = 0;
	state->prev_is_null = true;
	state->stage = SkipScanSearchingForFirst;
	state->skip_qual_removed = false;

	/* in an EXPLAIN the scan_keys are never populated,
	 * so we do not reorder them
	 */
	if (*state->num_scan_keys <= 0)
		return;

	state->skip_qual = (*state->scan_keys)[0];
	state->skip_qual_offset = 0;

	skip_scan_state_fixup_qual_order(state, runtime_keys, num_runtime_keys);
}

/* ScanKeys must be ordered by index attribute, while we pu the skip qual at the
 * front so it's easy to find. Now that it's in an easy-to-work-with form, move
 * the skip key if the distinct-column is not the first one in the index
 */
static void
skip_scan_state_fixup_qual_order(SkipScanState *state, IndexRuntimeKeyInfo *runtime_keys,
								 int num_runtime_keys)
{
	/* find the correct location for the skip qual, it should be the first qual
	 * on its column
	 */
	while (true)
	{
		int i = state->skip_qual_offset + 1;
		if (i >= *state->num_scan_keys)
			break;

		ScanKey sk = &(*state->scan_keys)[i];
		if (sk->sk_attno >= state->skip_qual.sk_attno)
			break;

		state->skip_qual_offset += 1;
	}

	Assert(state->skip_qual_offset < *state->num_scan_keys);

	/* move the ScanKays if the skip key was in the wrong place */
	if (state->skip_qual_offset > 0)
	{
		memmove((*state->scan_keys),
				(*state->scan_keys) + 1,
				sizeof(**state->scan_keys) * state->skip_qual_offset);
		(*state->scan_keys)[state->skip_qual_offset] = state->skip_qual;
		ScanKey skip_key = &(*state->scan_keys)[state->skip_qual_offset];

		/* fix up any runtime keys whose location may have changed */
		for (int i = 0; i < num_runtime_keys; i++)
		{
			if (runtime_keys[i].scan_key <= skip_key)
				runtime_keys[i].scan_key -= 1;
		}
	}
}

/****************************
 * small accessor functions *
 ****************************/

static inline bool
skip_scan_is_searching_for_first_val(SkipScanState *state)
{
	return state->stage == SkipScanSearchingForFirst;
}

static inline bool
skip_scan_state_found_null(SkipScanState *state)
{
	return (state->stage & SkipScanFoundNull) != 0;
}

static inline bool
skip_scan_state_found_val(SkipScanState *state)
{
	return (state->stage & SkipScanFoundVal) != 0;
}

static inline bool
skip_scan_state_found_everything(SkipScanState *state)
{
	return (state->stage & SkipScanFoundNullAndVal) == SkipScanFoundNullAndVal;
}

static inline bool
skip_scan_state_is_searching_for_null(SkipScanState *state)
{
	return (state->stage & SkipScanSearchingForNull) == SkipScanSearchingForNull;
}

static inline bool
skip_scan_state_is_searching_for_val(SkipScanState *state)
{
	return (state->stage & SkipScanSearchingForVal) == SkipScanSearchingForVal;
}

static inline bool
skip_scan_is_finished(SkipScanState *state)
{
	/*
	 * Once the underlying Index(Only)Scan runs out of tuples,
	 * we're not going to find anything more if
	 *  1. We're searching for first, and found nothing:
	 *     the regular qual must exclude everything
	 *  2. We're searching for a NULL, but have not found one:
	 *     we must have already found a non-NULL val, and be searching for a final NULL
	 *  3. We're searching for a non-NULL, but have not found one:
	 *     we must have already found a NULL val, and be searching for non-NULL ones
	 */
	return (state->stage & SkipScanFoundNullAndVal) == 0 ||
		   (skip_scan_state_is_searching_for_val(state) && !skip_scan_state_found_val(state)) ||
		   (skip_scan_state_is_searching_for_null(state) && !skip_scan_state_found_null(state));
}

static inline IndexScanDesc
skip_scan_state_get_scandesc(SkipScanState *state)
{
	return *state->scan_desc;
}

static inline ScanKey
skip_scan_state_get_scankeys(SkipScanState *state)
{
	return *state->scan_keys;
}

static inline ScanKey
skip_scan_state_get_skipkey(SkipScanState *state)
{
	Assert(!state->skip_qual_removed);
	Assert(*state->num_scan_keys > 0);
	return &skip_scan_state_get_scankeys(state)[state->skip_qual_offset];
}

/*******************************
 * Primary Execution Functions *
 *******************************/

static TupleTableSlot *
skip_scan_exec(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;
	TupleTableSlot *result;

	if (skip_scan_is_searching_for_first_val(state))
	{
		Assert(skip_scan_state_get_scandesc(state) == NULL);
		/* first time through we ignore the inital scan keys, which are used to
		 * skip previously seen values, we'll change back the number of scan keys
		 * the first time through update_skip_key
		 */
		skip_scan_state_remove_skip_qual(state);
		skip_scan_state_beginscan(state);

		/* ignore the first scan key, which is the qual we add to skip repeat
		 * values, the other quals still need to be applied. We'll set this back
		 * once we have the inital values, and our qual can be applied.
		 */
		index_rescan(skip_scan_state_get_scandesc(state),
					 skip_scan_state_get_scankeys(state),
					 *state->num_scan_keys,
					 NULL /*orderbys*/,
					 0 /*norderbys*/);
	}
	else
	{
		/* in subsequent calls we rescan based on the previously found element
		 * which will have been set below in update_skip_key
		 */
		bool skip_qual_was_removed = skip_scan_state_readd_skip_qual_if_needed(state);
		/* if the skip qual was just readded, we need to restart the indexscan to
		 * to tell it about the new qual
		 */
		if (skip_qual_was_removed)
			skip_scan_state_beginscan(state);

		skip_scan_state_populate_skip_qual(state);
		/* rescan the index based on the new distinct value */
		index_rescan(skip_scan_state_get_scandesc(state),
					 skip_scan_state_get_scankeys(state),
					 *state->num_scan_keys,
					 NULL /*orderbys*/,
					 0 /*norderbys*/);
	}

	/* get the next node from the underlying Index(Only)Scan */
	result = state->idx->ps.ExecProcNode(&state->idx->ps);
	bool index_scan_finished = TupIsNull(result);
	if (!index_scan_finished)
	{
		/* rescan can invalidate tuples, so if we're below a MergeAppend, we need
		 * to materialize the slot to ensure it won't be freed. (Technically, we
		 * do not need to do this if we're directly below the Unique node)
		 */
		ExecMaterializeSlot(result);
		update_skip_key(state, result);
	}
	else if (skip_scan_state_found_everything(state))
		return result;
	else if (skip_scan_is_finished(state))
		return result; /* the non-skip-quals exclude everything remaining */
	else
	{
		/* We've run out of tuples from the underlying scan, but we may not be done.
		 * NULL values don't participate in the normal ordering of values
		 * (eg. in SQL column < NULL will never be true, and column < value
		 * implies column IS NOT NULL), so they have to be handled specially.
		 * Further, NULL values can be returned either before or after the other
		 * values in the column depending on whether the index was declaed
		 * NULLS FIRST or NULLS LAST. Therefore just because we've reached the
		 * end of the IndexScan doesn't mean we're done; if we've only seen NULL
		 * values that means we may be in a NULLS FIRST index, and we need to
		 * check if a non-null value exists. Alternatively, if we haven't seen a
		 * NULL, we may be in a NULLS LAST column, so we need to check if a NULL
		 * value exists.
		 */
		if (!skip_scan_state_found_null(state))
			return skip_scan_search_for_null(state);
		else if (!skip_scan_state_found_val(state))
			return skip_scan_search_for_nonnull(state);
	}

	return result;
}

/* end the previous ScanDesc, if it exists, and start a new one. We call this
 * when we change the number of scan keys: on the first run, to set up the scan
 * and on the first one after that to set up our skip qual.
 */
static void
skip_scan_state_beginscan(SkipScanState *state)
{
	IndexScanDesc new_scan_desc;
	CustomScanState *node = &state->cscan_state;
	EState *estate = node->ss.ps.state;
	IndexScanDesc old_scan_desc = skip_scan_state_get_scandesc(state);
	if (old_scan_desc != NULL)
		index_endscan(old_scan_desc);

	new_scan_desc = index_beginscan(node->ss.ss_currentRelation,
									state->index_rel,
									estate->es_snapshot,
									*state->num_scan_keys,
									0 /*norderbys*/);

	if (state->index_only_scan)
	{
		new_scan_desc->xs_want_itup = true;
		*state->index_only_buffer = InvalidBuffer;
	}

	*state->scan_desc = new_scan_desc;
}

static TupleTableSlot *
skip_scan_search_for_null(SkipScanState *state)
{
	Assert(skip_scan_state_found_val(state));
	/* We haven't seen a NULL, redo the scan with the skip-qual set to
	 * only allow NULL values, to see if there is a valid NULL to return.
	 */
	state->stage |= SkipScanSearchingForNull;
	if (state->reached_end != NULL)
		*state->reached_end = false;
	return skip_scan_exec(&state->cscan_state);
}

static TupleTableSlot *
skip_scan_search_for_nonnull(SkipScanState *state)
{
	Assert(skip_scan_state_found_null(state));
	/* We only seen NULL values, redo the scan with the skip-qual set to
	 * exclude NULL values, to see if there is are valid non-NULL values
	 * to return.
	 */
	state->stage |= SkipScanSearchingForVal;
	if (state->reached_end != NULL)
		*state->reached_end = false;
	return skip_scan_exec(&state->cscan_state);
}

static void
update_skip_key(SkipScanState *state, TupleTableSlot *slot)
{
	int col = state->distinct_col_attnum;

	if (!state->prev_is_null && !state->distinct_by_val)
		pfree(DatumGetPointer(state->prev_distinct_val));

	MemoryContext old_ctx = MemoryContextSwitchTo(state->ctx);
	state->prev_distinct_val = slot_getattr(slot, col, &state->prev_is_null);
	if (state->prev_is_null)
	{
		state->stage |= SkipScanFoundNull;
	}
	else
	{
		state->stage |= SkipScanFoundVal;
		state->prev_distinct_val =
			datumCopy(state->prev_distinct_val, state->distinct_by_val, state->distinct_typ_len);
	}

	MemoryContextSwitchTo(old_ctx);

	/* if we were searching for an additional value after exhausting the
	 * underlying Index(Only)Scan the first time, we just found it.
	 */
	state->stage &= ~SkipScanSearchingForAdditional;
}

static void
skip_scan_state_remove_skip_qual(SkipScanState *state)
{
	Assert(*state->num_scan_keys >= 1);
	Assert(!state->skip_qual_removed);
	ScanKey start = skip_scan_state_get_skipkey(state);
	state->skip_qual = *start;
	int keys_to_move = *state->num_scan_keys - state->skip_qual_offset - 1;
	if (keys_to_move > 0)
		memmove(start, start + 1, sizeof(*start) * keys_to_move);
	*state->num_scan_keys -= 1;
	state->skip_qual_removed = true;
}

static inline bool
skip_scan_state_readd_skip_qual_if_needed(SkipScanState *state)
{
	if (state->skip_qual_removed)
	{
		state->skip_qual_removed = false;

		int keys_to_move = *state->num_scan_keys - state->skip_qual_offset;
		*state->num_scan_keys += 1;

		ScanKey start = skip_scan_state_get_skipkey(state);
		if (keys_to_move > 0)
			memmove(start + 1, start, sizeof(*start) * keys_to_move);
		*start = state->skip_qual;
		return true;
	}
	return false;
}

static inline void
skip_scan_state_populate_skip_qual(SkipScanState *state)
{
	ScanKey key = skip_scan_state_get_skipkey(state);
	key->sk_argument = state->prev_distinct_val;
	if (skip_scan_state_is_searching_for_null(state))
	{
		key->sk_flags = SK_SEARCHNULL | SK_ISNULL;
	}
	else if (skip_scan_state_is_searching_for_val(state))
	{
		key->sk_flags = SK_SEARCHNOTNULL | SK_ISNULL;
	}
	else if (state->prev_is_null)
	{
		/* Once we've seen a NULL we don't need another, so we remove the
		 * SEARCHNULL to enable us to finish early, if that's what's driving
		 * us.
		 */
		if (skip_scan_state_found_null(state))
			key->sk_flags &= ~SK_SEARCHNULL;

		key->sk_flags |= SK_ISNULL;
	}
	else
	{
		/* Once we've found a value, we only want to find values after that
		 * one, so remove SEARCHNOTNULL in case we were using that to find
		 * the first non-NULL value.
		 */
		if (skip_scan_state_found_val(state))
			key->sk_flags &= ~SK_SEARCHNOTNULL;

		key->sk_flags &= ~SK_ISNULL;
	}
}

static void
skip_scan_end(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;

	if (state->index_only_scan && BufferIsValid(*state->index_only_buffer))
		ReleaseBuffer(*state->index_only_buffer);

	ExecEndNode(&state->idx->ps);
}

static void
skip_scan_rescan(CustomScanState *node)
{
	SkipScanState *state = (SkipScanState *) node;
	IndexScanDesc old_scan_desc = skip_scan_state_get_scandesc(state);
	if (old_scan_desc != NULL)
		index_endscan(old_scan_desc);

	*state->scan_desc = NULL;

	/* If we never found any values (which can happen if we have a qual on a
	 * param that excludes all of the rows), we'll never have
	 * called update_skip_key so the scan keys will still be setup to skip
	 * the skip qual. Fix that here.
	 */
	skip_scan_state_readd_skip_qual_if_needed(state);

	ExecReScan(&state->idx->ps);

	state->prev_distinct_val = 0;
	state->prev_is_null = true;
	state->stage = SkipScanSearchingForFirst;
}

static CustomExecMethods skip_scan_state_methods = {
	.CustomName = "SkipScanState",
	.BeginCustomScan = skip_scan_begin,
	.EndCustomScan = skip_scan_end,
	.ExecCustomScan = skip_scan_exec,
	.ReScanCustomScan = skip_scan_rescan,
};

Node *
ts_skip_scan_state_create(CustomScan *cscan)
{
	SkipScanState *state = (SkipScanState *) newNode(sizeof(SkipScanState), T_CustomScanState);

	state->idx_scan = linitial(cscan->custom_plans);

	state->distinct_col_attnum = linitial_int(cscan->custom_private);

	state->distinct_by_val = lsecond_int(cscan->custom_private);

	state->distinct_typ_len = lthird_int(cscan->custom_private);

	state->cscan_state.methods = &skip_scan_state_methods;
	return (Node *) state;
}
