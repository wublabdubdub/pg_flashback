/*
 * fb_custom_scan.c
 *    Replace FunctionScan with a streaming CustomScan for pg_flashback().
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/relation.h"
#include "access/stratnum.h"
#include "access/tableam.h"
#include "access/tupdesc.h"
#include "catalog/pg_am_d.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "executor/execScan.h"
#include "executor/nodeCustom.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_custom_scan.h"
#include "fb_guc.h"
#include "fb_memory.h"
#include "fb_progress.h"
#include "fb_replay.h"
#include "fb_reverse_ops.h"
#include "fb_spool.h"

typedef enum FbCustomNodeKind
{
	FB_CUSTOM_NODE_WAL_INDEX = 1,
	FB_CUSTOM_NODE_REPLAY_DISCOVER,
	FB_CUSTOM_NODE_REPLAY_WARM,
	FB_CUSTOM_NODE_REPLAY_FINAL,
	FB_CUSTOM_NODE_REVERSE_SOURCE,
	FB_CUSTOM_NODE_APPLY
} FbCustomNodeKind;

typedef struct FbFlashbackCustomScanState
{
	CustomScanState css;
	FbCustomNodeKind kind;
	ExprState  *target_ts_expr;
	Oid			source_relid;
	FbFastPathSpec fast_path;
	bool		stage_ready;
	bool		query_done;
	bool		progress_started;
	bool		progress_finished;
	bool		progress_aborted;
	TimestampTz target_ts;
	FbRelationInfo info;
	TupleDesc	tupdesc;
	FbSpoolSession *spool;
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbReplayDiscoverState *discover;
	FbReplayWarmState *warm;
	FbReplayResult replay_result;
	FbReverseOpSource *reverse;
	FbApplyContext *apply;
} FbFlashbackCustomScanState;

typedef struct FbPlannerFastPathSpec
{
	FbFastPathMode mode;
	AttrNumber key_attnum;
	Oid key_type_oid;
	Oid key_collation;
	bool ordered_output;
	bool order_asc;
	uint64 limit_count;
	bool has_lower_bound;
	bool lower_inclusive;
	Const *lower_const;
	bool has_upper_bound;
	bool upper_inclusive;
	Const *upper_const;
	List *key_consts;
} FbPlannerFastPathSpec;

static set_rel_pathlist_hook_type fb_prev_set_rel_pathlist_hook = NULL;

static const CustomExecMethods *fb_custom_exec_methods(FbCustomNodeKind kind);
static const CustomScanMethods *fb_custom_scan_methods(FbCustomNodeKind kind);
static const CustomPathMethods *fb_custom_path_methods(FbCustomNodeKind kind);
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
static CustomPath *fb_flashback_make_custom_path(PlannerInfo *root,
												 RelOptInfo *rel,
												 Oid source_relid,
												 FbCustomNodeKind kind,
												 Path *child_path,
												 bool has_fast_path,
												 const FbPlannerFastPathSpec *fast_path);
static List *fb_flashback_build_scan_tlist(Index relid, Oid source_relid);
static bool fb_flashback_build_planner_fast_path(PlannerInfo *root,
												 RelOptInfo *rel,
												 Oid source_relid,
												 List *clauses,
												 FbPlannerFastPathSpec *spec);
static List *fb_flashback_serialize_fast_path(const FbPlannerFastPathSpec *spec);
static void fb_flashback_deserialize_fast_path(List *custom_private,
											   FbFlashbackCustomScanState *state);

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

static const char *
fb_fast_path_mode_name(FbFastPathMode mode)
{
	switch (mode)
	{
		case FB_FAST_PATH_KEY_EQ:
			return "key_eq";
		case FB_FAST_PATH_KEY_IN:
			return "key_in";
		case FB_FAST_PATH_KEY_RANGE:
			return "key_range";
		case FB_FAST_PATH_KEY_TOPN:
			return "key_topn";
		case FB_FAST_PATH_NONE:
			break;
	}

	return "none";
}

static const CustomExecMethods fb_wal_index_exec_methods = {
	.CustomName = "FbWalIndexScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomExecMethods fb_replay_discover_exec_methods = {
	.CustomName = "FbReplayDiscoverScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomExecMethods fb_replay_warm_exec_methods = {
	.CustomName = "FbReplayWarmScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomExecMethods fb_replay_final_exec_methods = {
	.CustomName = "FbReplayFinalScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomExecMethods fb_reverse_source_exec_methods = {
	.CustomName = "FbReverseSourceScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomExecMethods fb_apply_exec_methods = {
	.CustomName = "FbApplyScan",
	.BeginCustomScan = fb_flashback_begin_custom_scan,
	.ExecCustomScan = fb_flashback_exec_custom_scan,
	.EndCustomScan = fb_flashback_end_custom_scan,
	.ReScanCustomScan = fb_flashback_rescan_custom_scan,
	.ExplainCustomScan = fb_flashback_explain_custom_scan,
};

static const CustomScanMethods fb_wal_index_scan_methods = {
	.CustomName = "FbWalIndexScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomScanMethods fb_replay_discover_scan_methods = {
	.CustomName = "FbReplayDiscoverScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomScanMethods fb_replay_warm_scan_methods = {
	.CustomName = "FbReplayWarmScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomScanMethods fb_replay_final_scan_methods = {
	.CustomName = "FbReplayFinalScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomScanMethods fb_reverse_source_scan_methods = {
	.CustomName = "FbReverseSourceScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomScanMethods fb_apply_scan_methods = {
	.CustomName = "FbApplyScan",
	.CreateCustomScanState = fb_flashback_create_custom_scan_state,
};

static const CustomPathMethods fb_wal_index_path_methods = {
	.CustomName = "FbWalIndexScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static const CustomPathMethods fb_replay_discover_path_methods = {
	.CustomName = "FbReplayDiscoverScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static const CustomPathMethods fb_replay_warm_path_methods = {
	.CustomName = "FbReplayWarmScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static const CustomPathMethods fb_replay_final_path_methods = {
	.CustomName = "FbReplayFinalScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static const CustomPathMethods fb_reverse_source_path_methods = {
	.CustomName = "FbReverseSourceScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static const CustomPathMethods fb_apply_path_methods = {
	.CustomName = "FbApplyScan",
	.PlanCustomPath = fb_flashback_plan_custom_path,
};

static const CustomExecMethods *
fb_custom_exec_methods(FbCustomNodeKind kind)
{
	switch (kind)
	{
		case FB_CUSTOM_NODE_WAL_INDEX:
			return &fb_wal_index_exec_methods;
		case FB_CUSTOM_NODE_REPLAY_DISCOVER:
			return &fb_replay_discover_exec_methods;
		case FB_CUSTOM_NODE_REPLAY_WARM:
			return &fb_replay_warm_exec_methods;
		case FB_CUSTOM_NODE_REPLAY_FINAL:
			return &fb_replay_final_exec_methods;
		case FB_CUSTOM_NODE_REVERSE_SOURCE:
			return &fb_reverse_source_exec_methods;
		case FB_CUSTOM_NODE_APPLY:
			return &fb_apply_exec_methods;
	}

	elog(ERROR, "invalid fb custom node kind: %d", (int) kind);
	return &fb_apply_exec_methods;
}

static const CustomScanMethods *
fb_custom_scan_methods(FbCustomNodeKind kind)
{
	switch (kind)
	{
		case FB_CUSTOM_NODE_WAL_INDEX:
			return &fb_wal_index_scan_methods;
		case FB_CUSTOM_NODE_REPLAY_DISCOVER:
			return &fb_replay_discover_scan_methods;
		case FB_CUSTOM_NODE_REPLAY_WARM:
			return &fb_replay_warm_scan_methods;
		case FB_CUSTOM_NODE_REPLAY_FINAL:
			return &fb_replay_final_scan_methods;
		case FB_CUSTOM_NODE_REVERSE_SOURCE:
			return &fb_reverse_source_scan_methods;
		case FB_CUSTOM_NODE_APPLY:
			return &fb_apply_scan_methods;
	}

	elog(ERROR, "invalid fb custom node kind: %d", (int) kind);
	return &fb_apply_scan_methods;
}

static const CustomPathMethods *
fb_custom_path_methods(FbCustomNodeKind kind)
{
	switch (kind)
	{
		case FB_CUSTOM_NODE_WAL_INDEX:
			return &fb_wal_index_path_methods;
		case FB_CUSTOM_NODE_REPLAY_DISCOVER:
			return &fb_replay_discover_path_methods;
		case FB_CUSTOM_NODE_REPLAY_WARM:
			return &fb_replay_warm_path_methods;
		case FB_CUSTOM_NODE_REPLAY_FINAL:
			return &fb_replay_final_path_methods;
		case FB_CUSTOM_NODE_REVERSE_SOURCE:
			return &fb_reverse_source_path_methods;
		case FB_CUSTOM_NODE_APPLY:
			return &fb_apply_path_methods;
	}

	elog(ERROR, "invalid fb custom node kind: %d", (int) kind);
	return &fb_apply_path_methods;
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

static bool
fb_flashback_load_fast_key_meta(Oid source_relid,
								  FbRelationInfo *info,
								  Oid *key_type_oid,
								  Oid *key_collation,
								  Oid *key_opfamily)
{
	Relation rel;
	Relation index_rel;
	Form_pg_attribute attr;
	bool supported = false;

	fb_catalog_load_relation_info(source_relid, info);
	if (info->mode != FB_APPLY_KEYED ||
		info->key_natts != 1 ||
		!OidIsValid(info->key_index_oid))
		return false;

	rel = relation_open(source_relid, AccessShareLock);
	index_rel = index_open(info->key_index_oid, AccessShareLock);

	if (index_rel->rd_rel->relam == BTREE_AM_OID)
	{
		attr = TupleDescAttr(RelationGetDescr(rel), info->key_attnums[0] - 1);
		*key_type_oid = attr->atttypid;
		*key_collation = attr->attcollation;
		if (key_opfamily != NULL)
			*key_opfamily = index_rel->rd_opfamily[0];
		supported = true;
	}

	index_close(index_rel, AccessShareLock);
	relation_close(rel, AccessShareLock);
	return supported;
}

static bool
fb_flashback_can_coerce_const(Const *con, Oid target_type_oid)
{
	Oid input_typeids[1];
	Oid target_typeids[1];

	if (con == NULL || con->constisnull)
		return false;
	if (con->consttype == target_type_oid)
		return true;

	input_typeids[0] = con->consttype;
	target_typeids[0] = target_type_oid;
	return can_coerce_type(1, input_typeids, target_typeids, COERCION_IMPLICIT);
}

static Const *
fb_flashback_coerce_const(PlannerInfo *root,
						  Const *con,
						  Oid target_type_oid)
{
	Node *coerced;

	if (con == NULL || con->constisnull)
		return NULL;
	if (con->consttype == target_type_oid)
		return (Const *) copyObject(con);
	if (!fb_flashback_can_coerce_const(con, target_type_oid))
		return NULL;

	coerced = coerce_to_target_type(NULL,
									(Node *) copyObject(con),
									con->consttype,
									target_type_oid,
									-1,
									COERCION_IMPLICIT,
									COERCE_IMPLICIT_CAST,
									-1);
	if (coerced == NULL)
		return NULL;

	coerced = eval_const_expressions(root, coerced);
	coerced = strip_implicit_coercions(coerced);
	if (coerced == NULL || !IsA(coerced, Const))
		return NULL;

	con = (Const *) coerced;
	if (con->constisnull || con->consttype != target_type_oid)
		return NULL;

	return con;
}

static bool
fb_flashback_is_key_var(Node *node, Index relid, AttrNumber key_attnum)
{
	Var *var = (Var *) strip_implicit_coercions(node);

	return var != NULL &&
		IsA(var, Var) &&
		var->varno == relid &&
		var->varattno == key_attnum &&
		var->varlevelsup == 0;
}

static Const *
fb_flashback_match_key_const(PlannerInfo *root, Node *node, Oid key_type_oid)
{
	Const *con = (Const *) strip_implicit_coercions(node);

	if (con == NULL || !IsA(con, Const))
		return NULL;
	if (con->constisnull)
		return NULL;

	return fb_flashback_coerce_const(root, con, key_type_oid);
}

static bool
fb_flashback_limit_const_u64(Node *node, uint64 *limit_out)
{
	Const *con = (Const *) strip_implicit_coercions(node);
	int64 value = 0;

	if (con == NULL || !IsA(con, Const) || con->constisnull)
		return false;

	switch (con->consttype)
	{
		case INT2OID:
			value = DatumGetInt16(con->constvalue);
			break;
		case INT4OID:
			value = DatumGetInt32(con->constvalue);
			break;
		case INT8OID:
			value = DatumGetInt64(con->constvalue);
			break;
		default:
			return false;
	}

	if (value <= 0)
		return false;

	*limit_out = (uint64) value;
	return true;
}

static bool
fb_flashback_op_matches_family_cmptype(Oid opno,
									   Oid key_opfamily,
									   CompareType target_cmptype)
{
	List *interpretations;
	ListCell *lc;

	interpretations = get_op_index_interpretation(opno);
	foreach(lc, interpretations)
	{
		OpIndexInterpretation *interp = lfirst(lc);

		if (interp->opfamily_id == key_opfamily &&
			interp->cmptype == target_cmptype)
			return true;
	}

	return false;
}

static bool
fb_flashback_match_key_eq_clause(Node *clause,
								   PlannerInfo *root,
								   Index relid,
								   AttrNumber key_attnum,
								   Oid key_type_oid,
								   Oid key_opfamily,
								   FbPlannerFastPathSpec *spec)
{
	OpExpr *op = (OpExpr *) strip_implicit_coercions(clause);
	Const *key_const;

	if (op == NULL || !IsA(op, OpExpr) || list_length(op->args) != 2)
		return false;
	if (!fb_flashback_op_matches_family_cmptype(op->opno,
												key_opfamily,
												COMPARE_EQ))
		return false;
	if (!fb_flashback_is_key_var(linitial(op->args), relid, key_attnum))
		return false;

	key_const = fb_flashback_match_key_const(root, lsecond(op->args), key_type_oid);
	if (key_const == NULL)
		return false;

	spec->mode = FB_FAST_PATH_KEY_EQ;
	spec->key_consts = list_make1(copyObject(key_const));
	return true;
}

static bool
fb_flashback_match_key_in_clause(Node *clause,
								   PlannerInfo *root,
								   Index relid,
								   AttrNumber key_attnum,
								   Oid key_type_oid,
								   Oid key_opfamily,
								   FbPlannerFastPathSpec *spec)
{
	ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) strip_implicit_coercions(clause);
	Const *array_const;
	ArrayType *array;
	Oid elmtype;
	int16 elmlen;
	bool elmbyval;
	char elmalign;
	Datum *values;
	bool *nulls;
	int nelems;
	int i;
	List *key_consts = NIL;

	if (saop == NULL || !IsA(saop, ScalarArrayOpExpr) || list_length(saop->args) != 2)
		return false;
	if (!saop->useOr)
		return false;
	if (!fb_flashback_op_matches_family_cmptype(saop->opno,
												key_opfamily,
												COMPARE_EQ))
		return false;
	if (!fb_flashback_is_key_var(linitial(saop->args), relid, key_attnum))
		return false;

	array_const = (Const *) strip_implicit_coercions(lsecond(saop->args));
	if (array_const == NULL || !IsA(array_const, Const) || array_const->constisnull)
		return false;

	array = DatumGetArrayTypeP(array_const->constvalue);
	elmtype = ARR_ELEMTYPE(array);

	get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);
	deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
					  &values, &nulls, &nelems);
	if (nelems <= 0)
		return false;

	for (i = 0; i < nelems; i++)
	{
		Const *elem_const;
		Const *coerced_const;

		if (nulls[i])
			return false;
		elem_const = makeConst(key_type_oid,
							   -1,
							   InvalidOid,
							   elmlen,
							   values[i],
							   false,
							   elmbyval);
		coerced_const = fb_flashback_coerce_const(root, elem_const, key_type_oid);
		if (coerced_const == NULL)
			return false;
		key_consts = lappend(key_consts, coerced_const);
	}

	spec->mode = FB_FAST_PATH_KEY_IN;
	spec->key_consts = key_consts;
	return true;
}

static int
fb_flashback_invert_strategy(int strategy)
{
	switch (strategy)
	{
		case BTLessStrategyNumber:
			return BTGreaterStrategyNumber;
		case BTLessEqualStrategyNumber:
			return BTGreaterEqualStrategyNumber;
		case BTGreaterStrategyNumber:
			return BTLessStrategyNumber;
		case BTGreaterEqualStrategyNumber:
			return BTLessEqualStrategyNumber;
		default:
			return strategy;
	}
}

static bool
fb_flashback_apply_range_strategy(FbPlannerFastPathSpec *spec,
								  int strategy,
								  Const *bound_const)
{
	if (spec == NULL || bound_const == NULL || bound_const->constisnull)
		return false;

	switch (strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			if (spec->has_upper_bound)
				return false;
			spec->has_upper_bound = true;
			spec->upper_inclusive = (strategy == BTLessEqualStrategyNumber);
			spec->upper_const = bound_const;
			return true;
		case BTGreaterStrategyNumber:
		case BTGreaterEqualStrategyNumber:
			if (spec->has_lower_bound)
				return false;
			spec->has_lower_bound = true;
			spec->lower_inclusive = (strategy == BTGreaterEqualStrategyNumber);
			spec->lower_const = bound_const;
			return true;
		default:
			return false;
	}
}

static bool
fb_flashback_range_strategy_for_op(Oid opno,
								   Oid key_opfamily,
								   int *strategy_out)
{
	List *interpretations;
	ListCell *lc;

	interpretations = get_op_index_interpretation(opno);
	foreach(lc, interpretations)
	{
		OpIndexInterpretation *interp = lfirst(lc);

		if (interp->opfamily_id != key_opfamily)
			continue;

		switch (interp->cmptype)
		{
			case COMPARE_LT:
				*strategy_out = BTLessStrategyNumber;
				return true;
			case COMPARE_LE:
				*strategy_out = BTLessEqualStrategyNumber;
				return true;
			case COMPARE_GT:
				*strategy_out = BTGreaterStrategyNumber;
				return true;
			case COMPARE_GE:
				*strategy_out = BTGreaterEqualStrategyNumber;
				return true;
			default:
				break;
		}
	}

	return false;
}

static bool
fb_flashback_match_key_range_clause(Node *clause,
									PlannerInfo *root,
									Index relid,
									AttrNumber key_attnum,
									Oid key_type_oid,
									Oid key_opfamily,
									FbPlannerFastPathSpec *spec)
{
	OpExpr *op = (OpExpr *) strip_implicit_coercions(clause);
	Node *left;
	Node *right;
	Const *bound_const = NULL;
	int strategy;

	if (op == NULL || !IsA(op, OpExpr) || list_length(op->args) != 2)
		return false;

	left = linitial(op->args);
	right = lsecond(op->args);
	if (!fb_flashback_range_strategy_for_op(op->opno, key_opfamily, &strategy))
		return false;

	if (fb_flashback_is_key_var(left, relid, key_attnum))
		bound_const = fb_flashback_match_key_const(root, right, key_type_oid);
	else if (fb_flashback_is_key_var(right, relid, key_attnum))
	{
		bound_const = fb_flashback_match_key_const(root, left, key_type_oid);
		strategy = fb_flashback_invert_strategy(strategy);
	}
	else
		return false;

	if (bound_const == NULL)
		return false;
	return fb_flashback_apply_range_strategy(spec, strategy, bound_const);
}

static bool
fb_flashback_collect_fast_clauses(Node *clause, List **clauses)
{
	BoolExpr *bool_clause;
	ListCell *lc;

	if (clause == NULL || clauses == NULL)
		return false;

	clause = strip_implicit_coercions(clause);
	if (clause == NULL)
		return false;

	if (!IsA(clause, BoolExpr))
	{
		*clauses = lappend(*clauses, clause);
		return true;
	}

	bool_clause = (BoolExpr *) clause;
	if (bool_clause->boolop != AND_EXPR)
		return false;

	foreach(lc, bool_clause->args)
	{
		if (!fb_flashback_collect_fast_clauses(lfirst(lc), clauses))
			return false;
	}

	return true;
}

static bool
fb_flashback_match_order_key(PlannerInfo *root,
							 Index relid,
							 AttrNumber key_attnum,
							 bool *order_asc_out)
{
	SortGroupClause *sortcl;
	TargetEntry *tle;

	if (root->parse->sortClause == NIL || list_length(root->parse->sortClause) != 1)
		return false;

	sortcl = linitial_node(SortGroupClause, root->parse->sortClause);
	tle = get_sortgroupclause_tle(sortcl, root->parse->targetList);
	if (tle == NULL || !fb_flashback_is_key_var((Node *) tle->expr, relid, key_attnum))
		return false;

	*order_asc_out = !sortcl->reverse_sort;
	return true;
}

static bool
fb_flashback_build_planner_fast_path(PlannerInfo *root,
									 RelOptInfo *rel,
									 Oid source_relid,
									 List *clauses,
									 FbPlannerFastPathSpec *spec)
{
	FbRelationInfo info;
	Oid key_type_oid = InvalidOid;
	Oid key_collation = InvalidOid;
	Oid key_opfamily = InvalidOid;
	List *actual_clauses = clauses;
	List *flat_clauses = NIL;
	bool has_order = false;
	bool has_limit = false;
	uint64 limit_count = 0;
	bool order_asc = true;
	ListCell *lc;

	MemSet(spec, 0, sizeof(*spec));
	if (!fb_flashback_load_fast_key_meta(source_relid,
										 &info,
										 &key_type_oid,
										 &key_collation,
										 &key_opfamily))
		return false;

	if (actual_clauses == NIL && rel != NULL)
		actual_clauses = extract_actual_clauses(rel->baserestrictinfo, false);

	if (actual_clauses != NIL)
	{
		foreach(lc, actual_clauses)
		{
			if (!fb_flashback_collect_fast_clauses(lfirst(lc), &flat_clauses))
				return false;
		}
		actual_clauses = flat_clauses;
	}

	spec->key_attnum = info.key_attnums[0];
	spec->key_type_oid = key_type_oid;
	spec->key_collation = key_collation;

	has_order = fb_flashback_match_order_key(root,
											 rel->relid,
											 spec->key_attnum,
											 &order_asc);
	has_limit = fb_flashback_limit_const_u64(root->parse->limitCount, &limit_count);

	if (actual_clauses != NIL)
	{
		foreach(lc, actual_clauses)
		{
			Node *clause = lfirst(lc);

			if (spec->mode == FB_FAST_PATH_NONE &&
				fb_flashback_match_key_eq_clause(clause,
												 root,
												 rel->relid,
												 spec->key_attnum,
												 key_type_oid,
												 key_opfamily,
												 spec))
				continue;
			

			if (spec->mode == FB_FAST_PATH_NONE &&
				fb_flashback_match_key_in_clause(clause,
												 root,
												 rel->relid,
												 spec->key_attnum,
												 key_type_oid,
												 key_opfamily,
												 spec))
				continue;
			

			if ((spec->mode == FB_FAST_PATH_NONE ||
				 spec->mode == FB_FAST_PATH_KEY_RANGE) &&
				fb_flashback_match_key_range_clause(clause,
													root,
													rel->relid,
													spec->key_attnum,
													key_type_oid,
													key_opfamily,
													spec))
			{
				spec->mode = FB_FAST_PATH_KEY_RANGE;
				continue;
			}

			return false;
		}
	}

	if (has_order)
	{
		TypeCacheEntry *typentry;

		typentry = lookup_type_cache(key_type_oid, TYPECACHE_CMP_PROC);
		if (typentry == NULL || typentry->cmp_proc == InvalidOid)
			return false;

		spec->ordered_output = true;
		spec->order_asc = order_asc;
		if (spec->mode == FB_FAST_PATH_NONE)
		{
			if (!has_limit)
				return false;
			spec->mode = FB_FAST_PATH_KEY_TOPN;
		}
		if (has_limit)
			spec->limit_count = limit_count;
	}

	return spec->mode != FB_FAST_PATH_NONE;
}

static List *
fb_flashback_serialize_fast_path(const FbPlannerFastPathSpec *spec)
{
	List *private = NIL;
	Const *mode_const;
	Const *attnum_const;
	Const *type_const;
	Const *collation_const;
	Const *ordered_const;
	Const *order_const;
	Const *limit_const;
	Const *has_lower_const;
	Const *lower_inclusive_const;
	Const *has_upper_const;
	Const *upper_inclusive_const;
	Node *lower_node = NULL;
	Node *upper_node = NULL;
	Node *keys_node = NULL;

	if (spec == NULL)
		return list_make1(makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
									Int32GetDatum(FB_FAST_PATH_NONE), false, true));

	mode_const = makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
						   Int32GetDatum(spec->mode), false, true);
	attnum_const = makeConst(INT2OID, -1, InvalidOid, sizeof(int16),
							 Int16GetDatum(spec->key_attnum), false, true);
	type_const = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
						   ObjectIdGetDatum(spec->key_type_oid), false, true);
	collation_const = makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
								ObjectIdGetDatum(spec->key_collation), false, true);
	ordered_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
							  BoolGetDatum(spec->ordered_output), false, true);
	order_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
							BoolGetDatum(spec->order_asc), false, true);
	limit_const = makeConst(INT8OID, -1, InvalidOid, sizeof(int64),
							Int64GetDatum((int64) spec->limit_count), false, true);
	has_lower_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
								BoolGetDatum(spec->has_lower_bound), false, true);
	lower_inclusive_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
									  BoolGetDatum(spec->lower_inclusive), false, true);
	has_upper_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
								BoolGetDatum(spec->has_upper_bound), false, true);
	upper_inclusive_const = makeConst(BOOLOID, -1, InvalidOid, sizeof(bool),
									  BoolGetDatum(spec->upper_inclusive), false, true);

	if (spec->key_consts != NIL)
		keys_node = (Node *) copyObject(spec->key_consts);
	if (spec->lower_const != NULL)
		lower_node = (Node *) copyObject(spec->lower_const);
	if (spec->upper_const != NULL)
		upper_node = (Node *) copyObject(spec->upper_const);

	private = lappend(private, mode_const);
	private = lappend(private, attnum_const);
	private = lappend(private, type_const);
	private = lappend(private, collation_const);
	private = lappend(private, ordered_const);
	private = lappend(private, order_const);
	private = lappend(private, limit_const);
	private = lappend(private, has_lower_const);
	private = lappend(private, lower_inclusive_const);
	private = lappend(private, lower_node);
	private = lappend(private, has_upper_const);
	private = lappend(private, upper_inclusive_const);
	private = lappend(private, upper_node);
	private = lappend(private, keys_node);
	return private;
}

static void
fb_flashback_deserialize_fast_path(List *custom_private,
								   FbFlashbackCustomScanState *state)
{
	ListCell *lc;
	Const *mode_const;
	Const *attnum_const;
	Const *type_const;
	Const *collation_const;
	Const *ordered_const;
	Const *order_const;
	Const *limit_const;
	Const *has_lower_const;
	Const *lower_inclusive_const;
	Const *has_upper_const;
	Const *upper_inclusive_const;
	Node *lower_node;
	Node *upper_node;
	Node *keys_node;
	List *key_consts = NIL;
	int16 typlen;
	bool typbyval;
	int i = 0;

	MemSet(&state->fast_path, 0, sizeof(state->fast_path));
	if (custom_private == NIL || list_length(custom_private) < 3)
		return;

	lc = list_nth_cell(custom_private, 2);
	mode_const = lfirst_node(Const, lc);
	if ((FbFastPathMode) DatumGetInt32(mode_const->constvalue) == FB_FAST_PATH_NONE)
		return;
	if (list_length(custom_private) < 16)
		return;

	lc = lnext(custom_private, lc);
	attnum_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	type_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	collation_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	ordered_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	order_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	limit_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	has_lower_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	lower_inclusive_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	lower_node = lfirst(lc);
	lc = lnext(custom_private, lc);
	has_upper_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	upper_inclusive_const = lfirst_node(Const, lc);
	lc = lnext(custom_private, lc);
	upper_node = lfirst(lc);
	lc = lnext(custom_private, lc);
	keys_node = lfirst(lc);

	state->fast_path.mode = (FbFastPathMode) DatumGetInt32(mode_const->constvalue);
	state->fast_path.key_attnum = DatumGetInt16(attnum_const->constvalue);
	state->fast_path.key_type_oid = DatumGetObjectId(type_const->constvalue);
	state->fast_path.key_collation = DatumGetObjectId(collation_const->constvalue);
	state->fast_path.ordered_output = DatumGetBool(ordered_const->constvalue);
	state->fast_path.order_asc = DatumGetBool(order_const->constvalue);
	state->fast_path.limit_count = (uint64) DatumGetInt64(limit_const->constvalue);
	state->fast_path.has_lower_bound = DatumGetBool(has_lower_const->constvalue);
	state->fast_path.lower_inclusive = DatumGetBool(lower_inclusive_const->constvalue);
	state->fast_path.has_upper_bound = DatumGetBool(has_upper_const->constvalue);
	state->fast_path.upper_inclusive = DatumGetBool(upper_inclusive_const->constvalue);

	get_typlenbyval(state->fast_path.key_type_oid, &typlen, &typbyval);
	if (state->fast_path.has_lower_bound && lower_node != NULL && IsA(lower_node, Const))
	{
		Const *lower_const = (Const *) lower_node;

		state->fast_path.lower_isnull = lower_const->constisnull;
		if (!lower_const->constisnull)
			state->fast_path.lower_value = datumCopy(lower_const->constvalue, typbyval, typlen);
	}
	if (state->fast_path.has_upper_bound && upper_node != NULL && IsA(upper_node, Const))
	{
		Const *upper_const = (Const *) upper_node;

		state->fast_path.upper_isnull = upper_const->constisnull;
		if (!upper_const->constisnull)
			state->fast_path.upper_value = datumCopy(upper_const->constvalue, typbyval, typlen);
	}

	if (keys_node == NULL || !IsA(keys_node, List))
		return;

	key_consts = (List *) keys_node;
	state->fast_path.key_count = list_length(key_consts);
	if (state->fast_path.key_count <= 0)
		return;

	state->fast_path.key_values = palloc0(sizeof(Datum) * state->fast_path.key_count);
	state->fast_path.key_nulls = palloc0(sizeof(bool) * state->fast_path.key_count);

	foreach(lc, key_consts)
	{
		Const *key_const = lfirst_node(Const, lc);

		state->fast_path.key_nulls[i] = key_const->constisnull;
		state->fast_path.key_values[i] = datumCopy(key_const->constvalue, typbyval, typlen);
		i++;
	}
}

static FbCustomNodeKind
fb_flashback_node_kind(List *custom_private)
{
	Const *kind_const;

	if (custom_private == NIL)
		elog(ERROR, "fb custom private must not be empty");

	kind_const = linitial_node(Const, custom_private);
	return (FbCustomNodeKind) DatumGetInt32(kind_const->constvalue);
}

static Oid
fb_flashback_private_source_relid(List *custom_private)
{
	Const *source_relid_const;

	if (custom_private == NIL || list_length(custom_private) < 2)
		elog(ERROR, "fb custom private lost source relid");

	source_relid_const = lsecond_node(Const, custom_private);
	return DatumGetObjectId(source_relid_const->constvalue);
}

static List *
fb_flashback_make_private(FbCustomNodeKind kind,
						  Oid source_relid,
						  const FbPlannerFastPathSpec *fast_path)
{
	List *private;

	private = list_make2(
		makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
				  Int32GetDatum((int32) kind), false, true),
		makeConst(OIDOID, -1, InvalidOid, sizeof(Oid),
				  ObjectIdGetDatum(source_relid), false, true));

	if (kind == FB_CUSTOM_NODE_APPLY)
	{
		if (fast_path != NULL)
			private = list_concat(private,
								  fb_flashback_serialize_fast_path(fast_path));
		else
			private = list_concat(private,
								  fb_flashback_serialize_fast_path(NULL));
	}

	return private;
}

static FbFlashbackCustomScanState *
fb_flashback_custom_state(CustomScanState *node)
{
	return (FbFlashbackCustomScanState *) node;
}

static FbFlashbackCustomScanState *
fb_flashback_child_state(FbFlashbackCustomScanState *state)
{
	if (state == NULL || state->css.custom_ps == NIL)
		return NULL;

	return fb_flashback_custom_state(linitial_node(CustomScanState,
												   state->css.custom_ps));
}

static FbFlashbackCustomScanState *
fb_flashback_find_state(FbFlashbackCustomScanState *state,
						FbCustomNodeKind kind)
{
	FbFlashbackCustomScanState *child;

	if (state == NULL)
		return NULL;
	if (state->kind == kind)
		return state;

	child = fb_flashback_child_state(state);
	return fb_flashback_find_state(child, kind);
}

static void
fb_flashback_cleanup_state(FbFlashbackCustomScanState *state)
{
	if (state == NULL)
		return;

	if (state->apply != NULL)
	{
		fb_apply_end(state->apply);
		state->apply = NULL;
	}
	if (state->reverse != NULL)
	{
		fb_reverse_source_destroy(state->reverse);
		state->reverse = NULL;
	}
	if (state->warm != NULL)
	{
		fb_replay_warm_destroy(state->warm);
		state->warm = NULL;
	}
	if (state->discover != NULL)
	{
		fb_replay_discover_destroy(state->discover);
		state->discover = NULL;
	}
	if (state->spool != NULL)
	{
		fb_spool_session_destroy(state->spool);
		state->spool = NULL;
	}
	if (state->tupdesc != NULL)
	{
		FreeTupleDesc(state->tupdesc);
		state->tupdesc = NULL;
	}

	state->stage_ready = false;
	state->query_done = false;
}

static void
fb_flashback_abort_progress(FbFlashbackCustomScanState *state)
{
	if (state == NULL || !state->progress_started ||
		state->progress_finished || state->progress_aborted)
		return;

	fb_progress_abort();
	state->progress_aborted = true;
}

static void
fb_flashback_finish_progress(FbFlashbackCustomScanState *state)
{
	if (state == NULL || !state->progress_started ||
		state->progress_finished || state->progress_aborted)
		return;

	fb_progress_finish();
	state->progress_finished = true;
}

static TimestampTz
fb_flashback_parse_target_ts_text(text *target_ts_text)
{
	char *target_ts_cstr = text_to_cstring(target_ts_text);

	return DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
												   CStringGetDatum(target_ts_cstr),
												   ObjectIdGetDatum(InvalidOid),
												   Int32GetDatum(-1)));
}

static void
fb_flashback_require_target_ts_not_future(TimestampTz target_ts)
{
	if (target_ts > GetCurrentTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target timestamp is in the future")));
}

static TupleDesc
fb_flashback_relation_tupledesc(Oid relid)
{
	Relation rel;
	TupleDesc tupdesc;

	rel = relation_open(relid, AccessShareLock);
	tupdesc = BlessTupleDesc(CreateTupleDescCopy(RelationGetDescr(rel)));
	relation_close(rel, AccessShareLock);
	return tupdesc;
}

typedef struct FbPreflightEstimate
{
	uint64 wal_bytes;
	uint64 reverse_bytes;
	uint64 apply_bytes;
	uint64 total_bytes;
} FbPreflightEstimate;

static uint64
fb_flashback_estimate_add_u64(uint64 left, uint64 right)
{
	if (PG_UINT64_MAX - left < right)
		return PG_UINT64_MAX;

	return left + right;
}

static uint64
fb_flashback_estimate_mul_u64(uint64 left, uint64 right)
{
	if (left == 0 || right == 0)
		return 0;
	if (left > PG_UINT64_MAX / right)
		return PG_UINT64_MAX;

	return left * right;
}

static uint64
fb_flashback_estimate_tuple_bytes(TupleDesc tupdesc)
{
	uint64 data_width = 0;
	int i;

	if (tupdesc == NULL)
		return UINT64CONST(256);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		int avgwidth;

		if (attr->attisdropped)
			continue;

		if (attr->attlen > 0)
			avgwidth = attr->attlen;
		else
		{
			avgwidth = get_typavgwidth(attr->atttypid, attr->atttypmod);
			if (avgwidth <= 0)
				avgwidth = 32;
		}

		data_width = fb_flashback_estimate_add_u64(data_width, (uint64) avgwidth);
	}

	return MAXALIGN(HEAPTUPLESIZE + SizeofHeapTupleHeader + data_width);
}

static void
fb_flashback_preflight_append_detail(StringInfo buf,
									 uint64 estimated_bytes,
									 uint64 limit_bytes,
									 FbSpillMode mode)
{
	appendStringInfoString(buf, "estimated=");
	fb_memory_append_bytes_value(buf, estimated_bytes);
	appendStringInfoString(buf, " limit=");
	fb_memory_append_bytes_value(buf, limit_bytes);
	appendStringInfo(buf, " mode=%s phase=preflight",
					 fb_spill_mode_name(mode));
}

static void
fb_flashback_preflight_estimate_working_set(const FbRelationInfo *info,
											TupleDesc tupdesc,
											const FbWalRecordIndex *index,
											FbPreflightEstimate *estimate_out)
{
	uint64 tuple_bytes;
	uint64 record_count;
	uint64 target_count;
	uint64 wal_bytes;
	uint64 reverse_bytes;
	uint64 apply_per_entry;

	MemSet(estimate_out, 0, sizeof(*estimate_out));
	if (index == NULL)
		return;

	tuple_bytes = fb_flashback_estimate_tuple_bytes(tupdesc);
	record_count = index->kept_record_count;
	target_count = index->target_record_count;

	wal_bytes = (uint64) fb_spool_log_size(index->record_log);
	wal_bytes = fb_flashback_estimate_add_u64(wal_bytes,
											  (uint64) fb_spool_log_size(index->record_tail_log));
	wal_bytes = fb_flashback_estimate_add_u64(wal_bytes,
											  fb_flashback_estimate_mul_u64(record_count,
																			 (uint64) sizeof(FbRecordRef)));

	reverse_bytes = fb_flashback_estimate_mul_u64(target_count,
												  (uint64) sizeof(FbReverseOp));
	reverse_bytes = fb_flashback_estimate_add_u64(reverse_bytes,
												  fb_flashback_estimate_mul_u64(target_count,
																			 tuple_bytes));
	reverse_bytes = fb_flashback_estimate_add_u64(reverse_bytes,
												  fb_flashback_estimate_mul_u64(index->target_update_count,
																			 tuple_bytes));

	apply_per_entry = tuple_bytes + ((info != NULL && info->mode == FB_APPLY_KEYED) ?
									 UINT64CONST(128) : UINT64CONST(64));
	estimate_out->wal_bytes = wal_bytes;
	estimate_out->reverse_bytes = reverse_bytes;
	estimate_out->apply_bytes =
		fb_flashback_estimate_mul_u64(target_count, apply_per_entry);
	estimate_out->total_bytes =
		fb_flashback_estimate_add_u64(estimate_out->wal_bytes,
									  estimate_out->reverse_bytes);
	estimate_out->total_bytes =
		fb_flashback_estimate_add_u64(estimate_out->total_bytes,
									  estimate_out->apply_bytes);
}

static bool
fb_flashback_allow_disk_spill(const FbRelationInfo *info,
							  TupleDesc tupdesc,
							  const FbWalRecordIndex *index)
{
	FbPreflightEstimate estimate;
	FbSpillMode mode = fb_get_spill_mode();
	uint64 limit_bytes = fb_get_memory_limit_bytes();
	StringInfoData detail;

	fb_flashback_preflight_estimate_working_set(info, tupdesc, index, &estimate);
	if (limit_bytes == 0 || estimate.total_bytes <= limit_bytes)
		return mode == FB_SPILL_MODE_DISK;

	initStringInfo(&detail);
	fb_flashback_preflight_append_detail(&detail, estimate.total_bytes,
										 limit_bytes, mode);

	if (mode == FB_SPILL_MODE_DISK)
	{
		ereport(NOTICE,
				(errmsg("pg_flashback estimated working set exceeds pg_flashback.memory_limit; continuing with disk spill allowed"),
				 errdetail_internal("%s", detail.data)));
		return true;
	}

	if (mode == FB_SPILL_MODE_MEMORY)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("flashback is configured to run in memory-only mode, but the estimated working set exceeds pg_flashback.memory_limit"),
				 errdetail_internal("%s", detail.data),
				 errhint("Increase pg_flashback.memory_limit, or set pg_flashback.spill_mode = 'disk'.")));

	ereport(ERROR,
			(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
			 errmsg("estimated flashback working set exceeds pg_flashback.memory_limit"),
			 errdetail_internal("%s", detail.data),
			 errhint("Increase pg_flashback.memory_limit, or set pg_flashback.spill_mode = 'disk' to allow spill.")));
	return false;
}

static void
fb_flashback_append_qualified_relation_name(StringInfo buf, Oid relid)
{
	char *relname;
	char *nspname;

	relname = get_rel_name(relid);
	nspname = get_namespace_name(get_rel_namespace(relid));

	if (relname == NULL || nspname == NULL)
	{
		appendStringInfo(buf, "%u", relid);
		return;
	}

	appendStringInfo(buf, "%s.%s",
					 quote_identifier(nspname),
					 quote_identifier(relname));
}

static char *
fb_flashback_build_unsafe_detail(const FbRelationInfo *info,
								 const FbWalRecordIndex *index)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "scope=%s",
					 fb_wal_unsafe_scope_name(index->unsafe_scope));

	if (index->unsafe_reason == FB_WAL_UNSAFE_STORAGE_CHANGE)
		appendStringInfo(&buf, " operation=%s",
						 fb_wal_storage_change_op_name(index->unsafe_storage_op));

	appendStringInfoString(&buf, " target=");
	fb_flashback_append_qualified_relation_name(&buf, info->relid);

	if (index->unsafe_scope == FB_WAL_UNSAFE_SCOPE_TOAST &&
		OidIsValid(info->toast_relid))
	{
		appendStringInfoString(&buf, " toast=");
		fb_flashback_append_qualified_relation_name(&buf, info->toast_relid);
	}

	if (TransactionIdIsValid(index->unsafe_xid))
		appendStringInfo(&buf, " xid=%u", index->unsafe_xid);
	if (index->unsafe_commit_ts != 0)
	{
		char *commit_ts_text;

		commit_ts_text =
			TextDatumGetCString(DirectFunctionCall2(
				timestamptz_to_char,
				TimestampTzGetDatum(index->unsafe_commit_ts),
				CStringGetTextDatum("YYYY-MM-DD HH24:MI:SS.USOF")));
		appendStringInfo(&buf, " commit_ts=%s", commit_ts_text);
	}

	if (XLogRecPtrIsInvalid(index->unsafe_record_lsn) == false)
		appendStringInfo(&buf, " lsn=%X/%X",
						 LSN_FORMAT_ARGS(index->unsafe_record_lsn));

	return buf.data;
}

static void
fb_flashback_add_custom_path(PlannerInfo *root,
							 RelOptInfo *rel,
							 RangeTblEntry *rte)
{
	CustomPath *apply_path;
	CustomPath *reverse_path;
	CustomPath *replay_final_path;
	CustomPath *replay_warm_path;
	CustomPath *replay_discover_path;
	CustomPath *wal_index_path;
	Oid			source_relid = InvalidOid;
	FbPlannerFastPathSpec fast_path;
	bool has_fast_path = false;

	if (fb_flashback_match_rte_function(rte, &source_relid) == NULL)
		return;
	if (!bms_is_empty(rel->lateral_relids))
		return;

	has_fast_path = fb_flashback_build_planner_fast_path(root,
														 rel,
														 source_relid,
														 NIL,
														 &fast_path);

	wal_index_path = fb_flashback_make_custom_path(root,
												 rel,
												 source_relid,
												 FB_CUSTOM_NODE_WAL_INDEX,
												 NULL,
												 false,
												 NULL);
	replay_discover_path = fb_flashback_make_custom_path(root,
													  rel,
													  source_relid,
													  FB_CUSTOM_NODE_REPLAY_DISCOVER,
													  &wal_index_path->path,
													  false,
													  NULL);
	replay_warm_path = fb_flashback_make_custom_path(root,
												 rel,
												 source_relid,
												 FB_CUSTOM_NODE_REPLAY_WARM,
												 &replay_discover_path->path,
												 false,
												 NULL);
	replay_final_path = fb_flashback_make_custom_path(root,
												  rel,
												  source_relid,
												  FB_CUSTOM_NODE_REPLAY_FINAL,
												  &replay_warm_path->path,
												  false,
												  NULL);
	reverse_path = fb_flashback_make_custom_path(root,
											 rel,
											 source_relid,
											 FB_CUSTOM_NODE_REVERSE_SOURCE,
											 &replay_final_path->path,
											 false,
											 NULL);
	apply_path = fb_flashback_make_custom_path(root,
										   rel,
										   source_relid,
										   FB_CUSTOM_NODE_APPLY,
										   &reverse_path->path,
										   has_fast_path,
										   has_fast_path ? &fast_path : NULL);

	add_path(rel, &apply_path->path);
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

static CustomPath *
fb_flashback_make_custom_path(PlannerInfo *root,
							 RelOptInfo *rel,
							 Oid source_relid,
							 FbCustomNodeKind kind,
							 Path *child_path,
							 bool has_fast_path,
							 const FbPlannerFastPathSpec *fast_path)
{
	CustomPath *cpath;

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = false;
	cpath->path.parallel_workers = 0;
	cpath->path.pathkeys = (kind == FB_CUSTOM_NODE_APPLY &&
							has_fast_path &&
							fast_path != NULL &&
							fast_path->ordered_output) ?
		root->query_pathkeys : NIL;
	if (kind == FB_CUSTOM_NODE_APPLY)
	{
		cost_functionscan(&cpath->path, root, rel, NULL);
		if (cpath->path.startup_cost >= 1.0)
			cpath->path.startup_cost -= 1.0;
		if (cpath->path.total_cost >= cpath->path.startup_cost + 1.0)
			cpath->path.total_cost -= 1.0;
	}
	else
	{
		cpath->path.rows = 0;
		cpath->path.startup_cost = 0;
		cpath->path.total_cost = 0;
	}
	cpath->flags = 0;
	cpath->custom_paths = (child_path != NULL) ? list_make1(child_path) : NIL;
	cpath->custom_restrictinfo = NIL;
	cpath->custom_private = fb_flashback_make_private(kind,
													 source_relid,
													 fast_path);
	cpath->methods = fb_custom_path_methods(kind);
	return cpath;
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
	Expr	   *target_ts_expr = NULL;
	FbCustomNodeKind kind;
	Oid source_relid;
	List	   *scan_tlist;
	FbPlannerFastPathSpec fast_path;
	bool has_fast_path;
	List	   *private;

	rte = planner_rt_fetch(rel->relid, root);
	func = fb_flashback_match_rte_function(rte, NULL);
	if (func == NULL)
		elog(ERROR, "fb flashback custom path lost pg_flashback function expression");

	kind = fb_flashback_node_kind(best_path->custom_private);
	source_relid = fb_flashback_private_source_relid(best_path->custom_private);
	scan_tlist = fb_flashback_build_scan_tlist(rel->relid, source_relid);
	has_fast_path = false;
	if (kind == FB_CUSTOM_NODE_APPLY)
		has_fast_path = fb_flashback_build_planner_fast_path(root,
															 rel,
															 source_relid,
															 extract_actual_clauses(clauses, false),
															 &fast_path);
	private = fb_flashback_make_private(kind,
										source_relid,
										(kind == FB_CUSTOM_NODE_APPLY && has_fast_path) ?
										&fast_path : NULL);
	if (kind == FB_CUSTOM_NODE_WAL_INDEX)
		target_ts_expr = copyObject(lsecond(func->args));

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = (kind == FB_CUSTOM_NODE_APPLY) ? tlist : scan_tlist;
	cscan->scan.plan.qual = (kind == FB_CUSTOM_NODE_APPLY && !has_fast_path) ?
		extract_actual_clauses(clauses, false) : NIL;
	cscan->scan.scanrelid = 0;
	cscan->flags = 0;
	cscan->custom_plans = custom_plans;
	cscan->custom_exprs = (target_ts_expr != NULL) ? list_make1(target_ts_expr) : NIL;
	cscan->custom_private = private;
	cscan->custom_scan_tlist = scan_tlist;
	cscan->methods = fb_custom_scan_methods(kind);

	return &cscan->scan.plan;
}

static void
fb_flashback_ensure_wal_index_ready(FbFlashbackCustomScanState *state)
{
	Expr	   *target_ts_expr;
	Datum		target_ts_datum;
	bool		isnull = false;
	text	   *target_ts_text;

	if (state->stage_ready)
		return;

	target_ts_expr = linitial_node(Expr, castNode(CustomScan, state->css.ss.ps.plan)->custom_exprs);
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
	state->target_ts = fb_flashback_parse_target_ts_text(target_ts_text);

	fb_progress_enter_stage(FB_PROGRESS_STAGE_VALIDATE, NULL);
	fb_flashback_require_target_ts_not_future(state->target_ts);
	fb_require_archive_dir();
	fb_catalog_load_relation_info(state->source_relid, &state->info);
	state->tupdesc = fb_flashback_relation_tupledesc(state->source_relid);

	state->spool = fb_spool_session_create();
	fb_progress_enter_stage(FB_PROGRESS_STAGE_PREPARE_WAL, NULL);
	fb_wal_prepare_scan_context(state->target_ts, state->spool, &state->scan_ctx);
	fb_wal_build_record_index(&state->info, &state->scan_ctx, &state->index);
	if (state->index.unsafe)
	{
		char *detail = fb_flashback_build_unsafe_detail(&state->info, &state->index);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb does not support WAL windows containing %s operations",
						fb_wal_unsafe_reason_name(state->index.unsafe_reason)),
				 errdetail_internal("%s", detail)));
	}

	state->stage_ready = true;
}

static TupleTableSlot *
fb_flashback_apply_next(ScanState *scanstate)
{
	FbFlashbackCustomScanState *state = (FbFlashbackCustomScanState *) scanstate;
	FbFlashbackCustomScanState *child;
	FbFlashbackCustomScanState *wal_state;
	FbFlashbackCustomScanState *reverse_state;
	TupleTableSlot *slot;

	if (state->query_done)
		return ExecClearTuple(scanstate->ss_ScanTupleSlot);

	if (!state->progress_started)
	{
		fb_progress_begin();
		state->progress_started = true;
	}

	if (state->apply == NULL)
	{
		child = fb_flashback_child_state(state);
		if (child != NULL)
			ExecProcNode(&child->css.ss.ps);

		wal_state = fb_flashback_find_state(state, FB_CUSTOM_NODE_WAL_INDEX);
		reverse_state = fb_flashback_find_state(state, FB_CUSTOM_NODE_REVERSE_SOURCE);
		if (wal_state == NULL || reverse_state == NULL || reverse_state->reverse == NULL)
			elog(ERROR, "fb custom apply node missing prerequisite state");

		state->apply = fb_apply_begin(&wal_state->info,
									  wal_state->tupdesc,
									  reverse_state->reverse,
									  (state->fast_path.mode == FB_FAST_PATH_NONE) ?
									  NULL : &state->fast_path);
		fb_apply_bind_output_slot(state->apply, scanstate->ss_ScanTupleSlot);
		state->stage_ready = true;
	}

	slot = fb_apply_next_output_slot(state->apply);
	if (slot == NULL || TupIsNull(slot))
	{
		state->query_done = true;
		fb_flashback_finish_progress(state);
		return ExecClearTuple(scanstate->ss_ScanTupleSlot);
	}

	if (slot == scanstate->ss_ScanTupleSlot)
		return slot;
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
	FbCustomNodeKind kind;

	kind = fb_flashback_node_kind(cscan->custom_private);
	state = palloc0(sizeof(*state));
	NodeSetTag(state, T_CustomScanState);
	state->kind = kind;
	state->css.methods = fb_custom_exec_methods(kind);
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
	ListCell   *lc;

	source_relid_const = lsecond_node(Const, cscan->custom_private);
	state->source_relid = DatumGetObjectId(source_relid_const->constvalue);
	fb_flashback_deserialize_fast_path(cscan->custom_private, state);

	if (state->kind == FB_CUSTOM_NODE_APPLY && OidIsValid(state->source_relid))
	{
		Relation rel;

		rel = relation_open(state->source_relid, AccessShareLock);
		ExecInitScanTupleSlot(estate,
							  &state->css.ss,
							  RelationGetDescr(rel),
							  table_slot_callbacks(rel));
		if (cscan->scan.scanrelid > 0)
			ExecAssignScanProjectionInfoWithVarno(&state->css.ss,
												  cscan->scan.scanrelid);
		else
			ExecAssignScanProjectionInfo(&state->css.ss);
		relation_close(rel, AccessShareLock);
	}

	foreach(lc, cscan->custom_plans)
	{
		Plan *child_plan = lfirst_node(Plan, lc);
		PlanState *child_ps;

		child_ps = ExecInitNode(child_plan, estate, eflags);
		node->custom_ps = lappend(node->custom_ps, child_ps);
	}
}

static TupleTableSlot *
fb_flashback_exec_custom_scan(CustomScanState *node)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (state->kind == FB_CUSTOM_NODE_APPLY)
	{
		PG_TRY();
		{
			return ExecScan(&state->css.ss,
							fb_flashback_apply_next,
							fb_flashback_custom_recheck);
		}
		PG_CATCH();
		{
			fb_flashback_abort_progress(state);
			fb_flashback_cleanup_state(state);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	ExecClearTuple(slot);
	if (state->stage_ready)
		return slot;

	if (state->kind == FB_CUSTOM_NODE_WAL_INDEX)
	{
		fb_flashback_ensure_wal_index_ready(state);
		state->stage_ready = true;
		return slot;
	}

	{
		FbFlashbackCustomScanState *child = fb_flashback_child_state(state);
		FbFlashbackCustomScanState *wal_state;
		FbFlashbackCustomScanState *discover_state;
		FbFlashbackCustomScanState *warm_state;
		bool allow_disk_spill;
		bool shareable_reverse = false;

		if (child != NULL)
			ExecProcNode(&child->css.ss.ps);

		wal_state = fb_flashback_find_state(state, FB_CUSTOM_NODE_WAL_INDEX);
		discover_state = fb_flashback_find_state(state, FB_CUSTOM_NODE_REPLAY_DISCOVER);
		warm_state = fb_flashback_find_state(state, FB_CUSTOM_NODE_REPLAY_WARM);
		if (wal_state == NULL)
			elog(ERROR, "fb custom node lost wal state");

		switch (state->kind)
		{
			case FB_CUSTOM_NODE_REPLAY_DISCOVER:
				state->discover = fb_replay_discover(&wal_state->info,
													 &wal_state->index);
				break;
			case FB_CUSTOM_NODE_REPLAY_WARM:
				state->warm = fb_replay_warm(&wal_state->info,
											 &wal_state->index,
											 discover_state != NULL ?
											 discover_state->discover : NULL,
											 &state->replay_result);
				break;
			case FB_CUSTOM_NODE_REPLAY_FINAL:
				if (warm_state == NULL || warm_state->warm == NULL)
					elog(ERROR, "fb custom replay final missing warm state");
				allow_disk_spill =
					fb_flashback_allow_disk_spill(&wal_state->info,
												  wal_state->tupdesc,
												  &wal_state->index);
				shareable_reverse =
					fb_apply_parallel_candidate(&wal_state->info,
												(state->fast_path.mode == FB_FAST_PATH_NONE) ?
												NULL : &state->fast_path,
												wal_state->info.relid);
				state->replay_result.tracked_bytes = wal_state->index.tracked_bytes;
				state->replay_result.memory_limit_bytes =
					wal_state->index.memory_limit_bytes;
				state->reverse =
					fb_reverse_source_create((allow_disk_spill || shareable_reverse) ?
											 wal_state->spool : NULL,
											 &state->replay_result.tracked_bytes,
											 state->replay_result.memory_limit_bytes);
				fb_replay_final_build_reverse_source(&wal_state->info,
													 &wal_state->index,
													 wal_state->tupdesc,
													 warm_state->warm,
													 &state->replay_result,
													 state->reverse);
				break;
			case FB_CUSTOM_NODE_REVERSE_SOURCE:
				{
					FbFlashbackCustomScanState *final_state;

					final_state = fb_flashback_find_state(state,
														  FB_CUSTOM_NODE_REPLAY_FINAL);
					if (final_state == NULL || final_state->reverse == NULL)
						elog(ERROR, "fb custom reverse source missing final reverse");
					state->reverse = final_state->reverse;
					final_state->reverse = NULL;
					fb_reverse_source_finish(state->reverse);
					if (fb_apply_parallel_candidate(&wal_state->info,
												   (state->fast_path.mode == FB_FAST_PATH_NONE) ?
												   NULL : &state->fast_path,
												   wal_state->info.relid))
						fb_reverse_source_materialize(state->reverse);
				}
				break;
			case FB_CUSTOM_NODE_WAL_INDEX:
			case FB_CUSTOM_NODE_APPLY:
				break;
		}
	}

	state->stage_ready = true;
	return slot;
}

static void
fb_flashback_end_custom_scan(CustomScanState *node)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);

	if (state->kind == FB_CUSTOM_NODE_APPLY)
	{
		if (state->query_done)
			fb_flashback_finish_progress(state);
		else
			fb_flashback_abort_progress(state);
	}

	fb_flashback_cleanup_state(state);

	while (node->custom_ps != NIL)
	{
		PlanState *child_ps = linitial_node(PlanState, node->custom_ps);

		ExecEndNode(child_ps);
		node->custom_ps = list_delete_first(node->custom_ps);
	}
}

static void
fb_flashback_rescan_custom_scan(CustomScanState *node)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);
	FbFlashbackCustomScanState *child;

	if (state->kind == FB_CUSTOM_NODE_APPLY)
	{
		if (state->query_done)
			fb_flashback_finish_progress(state);
		else
			fb_flashback_abort_progress(state);
		state->progress_started = false;
		state->progress_finished = false;
		state->progress_aborted = false;
	}

	fb_flashback_cleanup_state(state);
	child = fb_flashback_child_state(state);
	if (child != NULL)
		ExecReScan(&child->css.ss.ps);
	if (state->kind == FB_CUSTOM_NODE_APPLY)
		ExecScanReScan(&state->css.ss);
}

static void
fb_flashback_explain_custom_scan(CustomScanState *node,
								 List *ancestors,
								 ExplainState *es)
{
	FbFlashbackCustomScanState *state = fb_flashback_custom_state(node);
	char	   *rel_label;
	char	   *attname = NULL;

	(void) ancestors;
	if (state->kind != FB_CUSTOM_NODE_APPLY)
		return;

	rel_label = fb_flashback_relation_label(state->source_relid);
	ExplainPropertyText("Flashback Relation", rel_label, es);
	if (state->fast_path.mode != FB_FAST_PATH_NONE)
	{
		ExplainPropertyText("Fast Path",
							fb_fast_path_mode_name(state->fast_path.mode),
							es);
		attname = get_attname(state->source_relid,
							 state->fast_path.key_attnum,
							 false);
		ExplainPropertyText("Fast Path Key", attname, es);
	}
	pfree(rel_label);
	if (attname != NULL)
		pfree(attname);
}

void
fb_custom_scan_init(void)
{
	static bool initialized = false;

	if (initialized)
		return;

	RegisterCustomScanMethods(&fb_wal_index_scan_methods);
	RegisterCustomScanMethods(&fb_replay_discover_scan_methods);
	RegisterCustomScanMethods(&fb_replay_warm_scan_methods);
	RegisterCustomScanMethods(&fb_replay_final_scan_methods);
	RegisterCustomScanMethods(&fb_reverse_source_scan_methods);
	RegisterCustomScanMethods(&fb_apply_scan_methods);
	fb_prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = fb_flashback_set_rel_pathlist;
	initialized = true;
}
