/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <access/visibilitymap.h>
#include <catalog/pg_type.h>
#include <executor/nodeIndexscan.h>
#include <executor/nodeIndexonlyscan.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/paramassign.h>
#include <optimizer/planmain.h>
#include <optimizer/planner.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <storage/predicate.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <parser/parse_func.h>

#include <compat.h>
#if PG12_GE
#include <optimizer/optimizer.h>
#endif
#include "guc.h"
#include "nodes/skip_scan/skip_scan.h"

#include <math.h>

static EquivalenceMember *find_ec_member_for_tle(EquivalenceClass *ec, TargetEntry *tle,
												 Relids relids);
static AttrNumber *find_column_from_tlist(List *target_list, PathKey *pathkey);

/**************************
 * SkipScan Plan Creation *
 **************************/

static CustomScanMethods skip_scan_plan_methods = {
	.CustomName = "SkipScan",
	.CreateCustomScanState = ts_skip_scan_state_create,
};

static Plan *
skip_scan_plan_create(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path, List *tlist,
					  List *clauses, List *custom_plans)
{
	SkipScanPath *path = (SkipScanPath *) best_path;
	CustomScan *skip_plan = makeNode(CustomScan);
	IndexPath *index_path = path->index_path;

	/* technically our placeholder col > NULL is unsatisfiable, and in some instances
	 * the planner will realize this and use is as an excuse to remove other quals.
	 * in order to prevent this, we prepare this qual ourselves.
	 */
	List *stripped_skip_clauses = get_actual_clauses(list_make1(path->skip_clause));

	List *fixed_skip_clauses = NIL;
	/* fix_indexqual_references */
	{
		RestrictInfo *rinfo = path->skip_clause;
		int indexcol = path->distinct_column;
		IndexOptInfo PG_USED_FOR_ASSERTS_ONLY *index = index_path->indexinfo;
		OpExpr *op = copyObject(castNode(OpExpr, rinfo->clause));
		castNode(OpExpr, copyObject(rinfo->clause));
		Assert(list_length(op->args) == 2);
		Assert(bms_equal(rinfo->left_relids, index->rel->relids));

		/* fix_indexqual_operand */
		Assert(index->indexkeys[indexcol] != 0);
		Var *node = castNode(Var, linitial(op->args));
		Assert(((Var *) node)->varno == index->rel->relid &&
			   ((Var *) node)->varattno == index->indexkeys[indexcol]);

		Var *result = (Var *) copyObject(node);
		result->varno = INDEX_VAR;
		result->varattno = indexcol + 1;

		linitial(op->args) = result;
		fixed_skip_clauses = lappend(fixed_skip_clauses, op);
	}

	Plan *plan = create_plan(root, &index_path->path);

	if (IsA(plan, IndexScan))
	{
		IndexScan *idx_plan = castNode(IndexScan, plan);
		skip_plan->scan = idx_plan->scan;
		idx_plan->indexqual = list_concat(fixed_skip_clauses, idx_plan->indexqual);
		idx_plan->indexqualorig = list_concat(stripped_skip_clauses, idx_plan->indexqualorig);
	}
	else if (IsA(plan, IndexOnlyScan))
	{
		IndexOnlyScan *idx_plan = castNode(IndexOnlyScan, plan);
		skip_plan->scan = idx_plan->scan;
		idx_plan->indexqual = list_concat(fixed_skip_clauses, idx_plan->indexqual);
	}
	else
		elog(ERROR, "bad subplan type for SkipScan: %d", plan->type);

	/* based on make_unique_from_pathkeys */
	AttrNumber *distinct_columns =
		find_column_from_tlist(plan->targetlist, linitial(best_path->path.pathkeys));
	if (distinct_columns == NULL)
		elog(ERROR, "Invalid skip column in SkipScanPath; could not find in tlist");

	skip_plan->custom_scan_tlist = list_copy(plan->targetlist);
	skip_plan->scan.plan.qual = NIL;
	skip_plan->scan.plan.type = T_CustomScan;
	skip_plan->scan.plan.parallel_safe = false;
	skip_plan->scan.plan.parallel_aware = false;
	skip_plan->methods = &skip_scan_plan_methods;
	skip_plan->custom_plans = list_make1(plan);
	skip_plan->custom_private =
		list_make3_int(*distinct_columns, path->distinct_by_val, path->distinct_typ_len);
	return &skip_plan->scan.plan;
}

/*************************
 * SkipScanPath Creation *
 *************************/
static CustomPathMethods skip_scan_path_methods = {
	.CustomName = "SkipScanPath",
	.PlanCustomPath = skip_scan_plan_create,
};

static SkipScanPath *create_index_skip_scan_path(PlannerInfo *root, UpperUniquePath *unique_path,
												 IndexPath *index_path, bool for_append);

#define SKIP_SKAN_REPLACE_UNIQUE false
#define SKIP_SKAN_UNDER_APPEND true

void
ts_add_skip_scan_paths(PlannerInfo *root, RelOptInfo *output_rel)
{
	if (!ts_guc_enable_skip_scan)
		return;
	ListCell *lc;
	List *pathlist = output_rel->pathlist;
	foreach (lc, pathlist)
	{
		UpperUniquePath *unique_path;
		Path *path = lfirst(lc);

		if (!IsA(path, UpperUniquePath))
			continue;

		unique_path = castNode(UpperUniquePath, path);

		/* currently we do not handle DISTINCT on more than one key. To do so,
		 * we would need to break down the SkipScan into subproblems: first
		 * find the minimal tuple then for each prefix find all unique suffix
		 * tuples. For instance, if we are searching over (int, int), we would
		 * first find (0, 0) then find (0, N) for all N in the domain, then
		 * find (1, N), then (2, N), etc
		 */
		if (unique_path->numkeys > 1)
			continue;

		if (IsA(unique_path->subpath, IndexPath))
		{
			IndexPath *index_path = castNode(IndexPath, unique_path->subpath);

			SkipScanPath *skip_scan_path = create_index_skip_scan_path(root,
																	   unique_path,
																	   index_path,
																	   SKIP_SKAN_REPLACE_UNIQUE);
			if (skip_scan_path == NULL)
				continue;

			// FIXME figure out costing Selectivity should be approximately n_distinct/total_tuples
			// total_cost = (index_cpu_cost + table_cpu_cost) + (index_IO_cost + table_IO_cost)
			skip_scan_path->cpath.path.total_cost = log2(unique_path->path.total_cost);
			// noop_unique_path->path.total_cost /= index_path->indexselectivity;
			// elog(WARNING, "cost %f", noop_unique_path->path.total_cost);
			// noop_unique_path->path.total_cost *= n_distinct;
			add_path(output_rel, &skip_scan_path->cpath.path);
		}
		else if (IsA(unique_path->subpath, MergeAppendPath))
		{
			MergeAppendPath *merge_path = castNode(MergeAppendPath, unique_path->subpath);
			bool can_skip_scan = false;
			List *new_paths = NIL;
			ListCell *lc;

			foreach (lc, merge_path->subpaths)
			{
				Path *sub_path = lfirst(lc);
				if (IsA(sub_path, IndexPath))
				{
					IndexPath *index_path = castNode(IndexPath, sub_path);
					SkipScanPath *skip_scan_path =
						create_index_skip_scan_path(root,
													unique_path,
													index_path,
													SKIP_SKAN_UNDER_APPEND);
					if (skip_scan_path != NULL)
					{
						sub_path = &skip_scan_path->cpath.path;
						can_skip_scan = true;
					}
				}

				new_paths = lappend(new_paths, sub_path);
			}

			if (!can_skip_scan)
				return;

			MergeAppendPath *new_merge_path = makeNode(MergeAppendPath);
			*new_merge_path = *merge_path;
			new_merge_path->subpaths = new_paths;
			new_merge_path->path.parallel_aware = false;
			new_merge_path->path.parallel_safe = false;
			// FIXME
			new_merge_path->path.total_cost = log2(merge_path->path.total_cost);

			UpperUniquePath *new_unique_path = makeNode(UpperUniquePath);
			*new_unique_path = *unique_path;
			new_unique_path->subpath = &new_merge_path->path;
			new_unique_path->path.parallel_aware = false;
			new_unique_path->path.parallel_safe = false;
			// FIXME
			new_unique_path->path.total_cost = log2(new_unique_path->path.total_cost);
			add_path(output_rel, &new_unique_path->path);
		}
	}
}

static SkipScanPath *
create_index_skip_scan_path(PlannerInfo *root, UpperUniquePath *unique_path, IndexPath *index_path,
							bool for_append)
{
	SkipScanPath *skip_scan_path = NULL;
	if (index_path->indexinfo->sortopfamily == NULL)
		return NULL; /* non-orderable index, skip these for now */

	/* we do not support orderByKeys out of conservatism; we do not know what,
	 * if any, work would be required to support them.
	 */
	if (index_path->indexorderbys != NIL)
		return NULL;

	skip_scan_path = palloc0(sizeof(*skip_scan_path));
	if (for_append)
		skip_scan_path->cpath.path = index_path->path;
	else
		skip_scan_path->cpath.path = unique_path->path;
	skip_scan_path->cpath.path.type = T_CustomPath;
	skip_scan_path->cpath.path.pathtype = T_CustomScan;
	skip_scan_path->cpath.custom_paths = list_make1(index_path);
	skip_scan_path->cpath.methods = &skip_scan_path_methods;
	skip_scan_path->index_path = index_path;
	Assert(unique_path->numkeys <= index_path->indexinfo->nkeycolumns);

	IndexOptInfo *idx_info = index_path->indexinfo;
	Index rel_index = idx_info->rel->relid;
	Oid rel_oid = root->simple_rte_array[rel_index]->relid;

	/* find the ordering operator we'll use to skip around each key column */
	PathKey *first_pathkey = linitial(index_path->path.pathkeys);

	AttrNumber *col_num = find_column_from_tlist(idx_info->indextlist, first_pathkey);
	if (col_num == NULL)
		elog(ERROR, "could not find col for SkipScan");

	int col = AttrNumberGetAttrOffset(*col_num);

	int table_col = idx_info->indexkeys[col];
	if (table_col == 0)
		return NULL; /* cannot use this index */

	HeapTuple column_tuple =
		SearchSysCache2(ATTNUM, ObjectIdGetDatum(rel_oid), Int16GetDatum(table_col));
	if (!HeapTupleIsValid(column_tuple))
		return NULL; /* cannot use this index */

	Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(column_tuple);

	Oid column_type = att_tup->atttypid;
	int32 column_typmod = att_tup->atttypmod;
	Oid column_collation = att_tup->attcollation;

	skip_scan_path->distinct_by_val = att_tup->attbyval;
	skip_scan_path->distinct_typ_len = att_tup->attlen;
	ReleaseSysCache(column_tuple);
	if (!OidIsValid(column_type))
		return NULL; /* cannot use this index */

	Oid btree_opfamily = idx_info->sortopfamily[col];

	int16 strategy = idx_info->reverse_sort[col] ? BTLessStrategyNumber : BTGreaterStrategyNumber;
	if (index_path->indexscandir == BackwardScanDirection)
	{
		strategy =
			(strategy == BTLessStrategyNumber) ? BTGreaterStrategyNumber : BTLessStrategyNumber;
	}
	Oid comparator = get_opfamily_member(btree_opfamily, column_type, column_type, strategy);
	if (!OidIsValid(comparator))
		return NULL; /* cannot use this index */

	Const *prev_val = makeNullConst(column_type, column_typmod, column_collation);
	Var *current_val = makeVar(rel_index /*varno*/,
							   table_col /*varattno*/,
							   column_type /*vartype*/,
							   column_typmod /*vartypmod*/,
							   column_collation /*varcollid*/,
							   0 /*varlevelsup*/);

	Expr *comparsion_expr = make_opclause(comparator,
										  BOOLOID /*opresulttype*/,
										  false /*opretset*/,
										  &current_val->xpr /*leftop*/,
										  &prev_val->xpr /*rightop*/,
										  InvalidOid /*opcollid*/,
										  idx_info->indexcollations[col] /*inputcollid*/);
	set_opfuncid(castNode(OpExpr, comparsion_expr));
	RestrictInfo *clause = make_simple_restrictinfo(comparsion_expr);
	skip_scan_path->skip_clause = clause;
	skip_scan_path->distinct_column = col;

	return skip_scan_path;
}

/************
 * Utilites *
 ************/

static AttrNumber *
find_column_from_tlist(List *target_list, PathKey *pathkey)
{
	EquivalenceClass *ec = pathkey->pk_eclass;
	TargetEntry *tle = NULL;
	if (ec->ec_has_volatile)
	{
		/*
		 * If the pathkey's EquivalenceClass is volatile, then it must
		 * have come from an ORDER BY clause, and we have to match it to
		 * that same targetlist entry.
		 */
		if (ec->ec_sortref == 0) /* can't happen */
			elog(ERROR, "volatile EquivalenceClass has no sortref");
		tle = get_sortgroupref_tle(ec->ec_sortref, target_list);
		Assert(tle);
		Assert(list_length(ec->ec_members) == 1);
	}
	else
	{
		/*
		 * Otherwise, we can use any non-constant expression listed in the
		 * pathkey's EquivalenceClass.  For now, we take the first tlist
		 * item found in the EC.
		 */
		ListCell *j;
		foreach (j, target_list)
		{
			tle = (TargetEntry *) lfirst(j);
			if (find_ec_member_for_tle(ec, tle, NULL))
				break;
			tle = NULL;
		}
	}

	if (!tle)
		return NULL;

	return &tle->resno;
}

static EquivalenceMember *
find_ec_member_for_tle(EquivalenceClass *ec, TargetEntry *tle, Relids relids)
{
	Expr *tlexpr;
	ListCell *lc;

	/* We ignore binary-compatible relabeling on both ends */
	tlexpr = tle->expr;
	while (tlexpr && IsA(tlexpr, RelabelType))
		tlexpr = ((RelabelType *) tlexpr)->arg;

	foreach (lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		Expr *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they belong to the rel being sorted.
		 */
		// TODO check with HT
		// if (em->em_is_child &&
		// 	!bms_is_subset(em->em_relids, relids))
		// 	continue;

		/* Match if same expression (after stripping relabel) */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		if (equal(emexpr, tlexpr))
			return em;
	}

	return NULL;
}
