/*
 * fb_custom_scan.c
 *    Replace FunctionScan with a streaming CustomScan for pg_flashback().
 */

#include "postgres.h"

#include "access/relation.h"
#include "executor/executor.h"
#include "executor/execScan.h"
#include "executor/nodeCustom.h"
#include "executor/tuptable.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "commands/explain_format.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "fb_custom_scan.h"
#include "fb_entry.h"

typedef struct FbFlashbackCustomScanState
{
	CustomScanState css;
	ExprState  *target_ts_expr;
	FbFlashbackQueryState *query;
	Oid			source_relid;
	bool		query_done;
} FbFlashbackCustomScanState;

static set_rel_pathlist_hook_type fb_prev_set_rel_pathlist_hook = NULL;

static Node *fb_flashback_create_custom_scan_state(CustomScan *cscan);
static void fb_flashback_begin_custom_scan(CustomScanState *node,
										   EState *estate,
										   int eflags);
static TupleTableSlot *fb_flashback_exec_custom_scan(CustomScanState *node);
static void fb_flashback_end_custom_scan(CustomScanState *node);
static void fb_flashback_rescan_custom_scan(CustomScanState *node);
static void fb_flashback_explain_custom_scan(CustomScanState *node,
											 List *ancestors,
											 ExplainState *es);
static Plan *fb_flashback_plan_custom_path(PlannerInfo *root,
										   RelOptInfo *rel,
										   CustomPath *best_path,
										   List *tlist,
										   List *clauses,
										   List *custom_plans);
static void fb_flashback_set_rel_pathlist(PlannerInfo *root,
										  RelOptInfo *rel,
										  Index rti,
										  RangeTblEntry *rte);
static List *fb_flashback_build_scan_tlist(Index relid, Oid source_relid);

static const CustomExecMethods fb_flashback_exec_methods = {
	.CustomName = "FbFlashbackScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomScanMethods fb_flashback_scan_methods = {
	.CustomName = "FbFlashbackScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomPathMethods fb_flashback_path_methods = {
	.CustomName = "FbFlashbackScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static char *
fb_flashback_relation_label(Oid relid)
{
	char	   *relname;
	char	   *nspname;

	relname = get_rel_name(relid);
	nspname = get_namespace_name(get_rel_namespace(relid));
	if (relname == NULL || nspname == NULL)
		return psprintf("%u", relid);

	return psprintf("%s.%s",
					quote_identifier(nspname),
					quote_identifier(relname));
}

static List *
fb_flashback_build_scan_tlist(Index relid, Oid source_relid)
{
	List	   *tlist = NIL;
	Relation	rel;
	TupleDesc	tupdesc;
	int			attno;

	rel = relation_open(source_relid, AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);
		Var		   *var;
		TargetEntry *tle;

		if (attr->attisdropped)
			continue;

		var = makeVar(relid,
					  attno,
					  attr->atttypid,
					  attr->atttypmod,
					  attr->attcollation,
					  0);
		tle = makeTargetEntry((Expr *) var,
							  attno,
							  pstrdup(NameStr(attr->attname)),
							  false);
		tlist = lappend(tlist, tle);
	}

	relation_close(rel, AccessShareLock);
	return tlist;
}

static FuncExpr *
fb_flashback_match_rte_function(RangeTblEntry *rte, Oid *source_relid_out)
{
	RangeTblFunction *rtfunc;
	Node	   *funcexpr;
	FuncExpr   *func;
	Node	   *source_arg;
	Node	   *target_ts_arg;
	Oid			source_type;
	Oid			source_relid;
	char	   *func_name;

	if (source_relid_out != NULL)
		*source_relid_out = InvalidOid;

	if (rte == NULL || rte->rtekind != RTE_FUNCTION)
		return NULL;
	if (rte->funcordinality || list_length(rte->functions) != 1)
		return NULL;

	rtfunc = linitial_node(RangeTblFunction, rte->functions);
	funcexpr = strip_implicit_coercions(rtfunc->funcexpr);
	if (!IsA(funcexpr, FuncExpr))
		return NULL;

	func = castNode(FuncExpr, funcexpr);
	if (list_length(func->args) != 2)
		return NULL;

	func_name = get_func_name(func->funcid);
	if (func_name == NULL || strcmp(func_name, "pg_flashback") != 0)
		return NULL;

	source_arg = (Node *) linitial(func->args);
	target_ts_arg = (Node *) lsecond(func->args);
	source_type = exprType(source_arg);
	source_relid = typeidTypeRelid(source_type);
	if (!OidIsValid(source_relid) || exprType(target_ts_arg) != TEXTOID)
		return NULL;

	if (source_relid_out != NULL)
		*source_relid_out = source_relid;

	return func;
}

static void
fb_flashback_add_custom_path(PlannerInfo *root,
							 RelOptInfo *rel,
							 RangeTblEntry *rte)
{
	CustomPath *cpath;
	Oid			source_relid = InvalidOid;

	if (fb_flashback_match_rte_function(rte, &source_relid) == NULL)
		return;
	if (!bms_is_empty(rel->lateral_relids))
		return;

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = 0;
	cpath->path.pathkeys = NIL;
	cost_functionscan(&cpath->path, root, rel, NULL);
	if (cpath->path.startup_cost >= 1.0)
		cpath->path.startup_cost -= 1.0;
	if (cpath->path.total_cost >= cpath->path.startup_cost + 1.0)
		cpath->path.total_cost -= 1.0;
	cpath->flags = 0;
	cpath->custom_paths = NIL;
	cpath->custom_restrictinfo = NIL;
	cpath->custom_private = list_make1(makeConst(OIDOID,
												 -1,
												 InvalidOid,
												 sizeof(Oid),
												 ObjectIdGetDatum(source_relid),
												 false,
												 true));
	cpath->methods = &fb_flashback_path_methods;

	add_path(rel, &cpath->path);
}

static void
fb_flashback_set_rel_pathlist(PlannerInfo *root,
							  RelOptInfo *rel,
							  Index rti,
							  RangeTblEntry *rte)
{
	if (fb_prev_set_rel_pathlist_hook != NULL)
		fb_prev_set_rel_pathlist_hook(root, rel, rti, rte);

	fb_flashback_add_custom_path(root, rel, rte);
}

static Plan *
fb_flashback_plan_custom_path(PlannerInfo *root,
							  RelOptInfo *rel,
							  CustomPath *best_path,
							  List *tlist,
							  List *clauses,
							  List *custom_plans)
{
	CustomScan *cscan;
	RangeTblEntry *rte;
	FuncExpr   *func;
	Expr	   *target_ts_expr;
	Const	   *source_relid_const;
	List	   *scan_tlist;

	rte = planner_rt_fetch(rel->relid, root);
	func = fb_flashback_match_rte_function(rte, NULL);
	if (func == NULL)
		elog(ERROR, "fb flashback custom path lost pg_flashback function expression");

	source_relid_const = linitial_node(Const, best_path->custom_private);
	target_ts_expr = copyObject(lsecond(func->args));
	scan_tlist = fb_flashback_build_scan_tlist(rel->relid,
											   DatumGetObjectId(source_relid_const->constvalue));

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->scan.scanrelid = 0;
	cscan->flags = 0;
	cscan->custom_plans = custom_plans;
	cscan->custom_exprs = list_make1(target_ts_expr);
	cscan->custom_private = list_make1(copyObject(source_relid_const));
	cscan->custom_scan_tlist = scan_tlist;
	cscan->methods = &fb_flashback_scan_methods;

	return &cscan->scan.plan;
}

static FbFlashbackCustomScanState *
fb_flashback_custom_state(CustomScanState *node)
{
	return (FbFlashbackCustomScanState *) node;
}

static void
fb_flashback_custom_start_query(FbFlashbackCustomScanState *state)
{
	CustomScan *cscan = castNode(CustomScan, state->css.ss.ps.plan);
	Expr	   *target_ts_expr;
	Datum		target_ts_datum;
	bool		isnull = false;
	text	   *target_ts_text;

	target_ts_expr = linitial_node(Expr, cscan->custom_exprs);
	if (state->target_ts_expr == NULL)
		state->target_ts_expr = ExecInitExpr(target_ts_expr, &state->css.ss.ps);

	target_ts_datum = ExecEvalExprSwitchContext(state->target_ts_expr,
												 state->css.ss.ps.ps_ExprContext,
												 &isnull);
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("pg_flashback target timestamp must not be NULL")));

	target_ts_text = DatumGetTextPP(target_ts_datum);
	state->query = fb_flashback_query_begin(state->source_relid, target_ts_text);
	state->query_done = false;
}

static TupleTableSlot *
fb_flashback_custom_next(ScanState *scanstate)
{
	FbFlashbackCustomScanState *state = (FbFlashbackCustomScanState *) scanstate;
	TupleTableSlot *slot = scanstate->ss_ScanTupleSlot;
	Datum		result;

	ExecClearTuple(slot);
	if (state->query == NULL && !state->query_done)
		fb_flashback_custom_start_query(state);
	if (state->query == NULL)
		return slot;

	if (!fb_flashback_query_next_datum(state->query, &result))
	{
		if (!state->query_done)
		{
			fb_flashback_query_finish(state->query);
			state->query_done = true;
		}
		return slot;
	}

	ExecStoreHeapTupleDatum(result, slot);
	return slot;
}

static bool
fb_flashback_custom_recheck(ScanState *scanstate, TupleTableSlot *slot)
{
	(void) scanstate;
	(void) slot;
	return true;
}

static Node *
fb_flashback_create_custom_scan_state(CustomScan *cscan)
{
	FbFlashbackCustomScanState *state;

	(void) cscan;
	state = palloc0(sizeof(*state));
	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &fb_flashback_exec_methods;
	return (Node *) state;
}

static void
fb_flashback_begin_custom_scan(CustomScanState *node,
							   EState *estate,
							   int eflags)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	Const	   *source_relid_const;

	(void) estate;
	(void) eflags;

	source_relid_const = linitial_node(Const, cscan->custom_private);
	state->source_relid = DatumGetObjectId(source_relid_const->constvalue);
}

static TupleTableSlot *
fb_flashback_exec_custom_scan(CustomScanState *node)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);

	PG_TRY();
	{
		return ExecScan(&state->css.ss,
						fb_flashback_custom_next,
						fb_flashback_custom_recheck);
	}
	PG_CATCH();
	{
		if (state->query != NULL && !state->query_done)
			fb_flashback_query_abort(state->query);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static void
fb_flashback_end_custom_scan(CustomScanState *node)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);

	if (state->query == NULL)
		return;

	if (state->query_done)
		fb_flashback_query_finish(state->query);
	else
		fb_flashback_query_abort(state->query);

	state->query = NULL;
}

static void
fb_flashback_rescan_custom_scan(CustomScanState *node)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);

	if (state->query != NULL)
	{
		if (state->query_done)
			fb_flashback_query_finish(state->query);
		else
			fb_flashback_query_abort(state->query);
		state->query = NULL;
	}

	state->query_done = false;
	ExecScanReScan(&state->css.ss);
	fb_flashback_custom_start_query(state);
}

static void
fb_flashback_explain_custom_scan(CustomScanState *node,
								 List *ancestors,
								 ExplainState *es)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);
	char	   *rel_label;

	(void) ancestors;
	rel_label = fb_flashback_relation_label(state->source_relid);
	ExplainPropertyText("Flashback Relation", rel_label, es);
	pfree(rel_label);
}

void
fb_custom_scan_init(void)
{
	static bool initialized = false;

	if (initialized)
		return;

	RegisterCustomScanMethods(&fb_flashback_scan_methods);
	fb_prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = fb_flashback_set_rel_pathlist;
	initialized = true;
}
