/*
 * fb_entry.c
 *    SQL entry points and direct-query orchestration.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_entry.h"
#include "fb_error.h"
#include "fb_guc.h"
#include "fb_progress.h"
#include "fb_reverse_ops.h"
#include "fb_wal.h"

/*
 * FbFlashbackQueryState
 *    Tracks one SRF execution state.
 */

typedef struct FbFlashbackQueryState
{
	FbRelationInfo info;
	TupleDesc tupdesc;
	FbReverseOpStream reverse;
	FbApplyContext *apply;
	ExprContext *econtext;
	bool cleanup_registered;
} FbFlashbackQueryState;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fb_version);
PG_FUNCTION_INFO_V1(fb_check_relation);
PG_FUNCTION_INFO_V1(pg_flashback);
PG_FUNCTION_INFO_V1(fb_export_undo);

/*
 * fb_require_target_ts_not_future
 *    SQL entry helper.
 */

static void
fb_require_target_ts_not_future(TimestampTz target_ts)
{
	if (target_ts > GetCurrentTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target timestamp is in the future")));
}

/*
 * fb_require_supported_target_relation
 *    SQL entry helper.
 */

static void
fb_require_supported_target_relation(Oid relid, FbRelationInfo *info)
{
	fb_catalog_load_relation_info(relid, info);
}

/*
 * fb_relation_tupledesc
 *    SQL entry helper.
 */

static TupleDesc
fb_relation_tupledesc(Oid relid)
{
	Relation rel;
	TupleDesc tupdesc;

	rel = relation_open(relid, AccessShareLock);
	tupdesc = BlessTupleDesc(CreateTupleDescCopy(RelationGetDescr(rel)));
	relation_close(rel, AccessShareLock);

	return tupdesc;
}

/*
 * fb_parse_target_ts_text
 *    SQL entry helper.
 */

static TimestampTz
fb_parse_target_ts_text(text *target_ts_text)
{
	char *target_ts_cstr = text_to_cstring(target_ts_text);

	return DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
												   CStringGetDatum(target_ts_cstr),
												   ObjectIdGetDatum(InvalidOid),
												   Int32GetDatum(-1)));
}

/*
 * fb_resolve_target_type_relid
 *    SQL entry helper.
 */

static Oid
fb_resolve_target_type_relid(FunctionCallInfo fcinfo)
{
	Oid type_oid;
	Oid relid;

	type_oid = get_fn_expr_argtype(fcinfo->flinfo, 0);
	if (!OidIsValid(type_oid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("pg_flashback target type could not be resolved")));

	relid = typeidTypeRelid(type_oid);
	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("pg_flashback target must be a table row type"),
				 errhint("Call pg_flashback as SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text).")));

	return relid;
}

/*
 * fb_validate_flashback_target
 *    SQL entry helper.
 */

static void
fb_validate_flashback_target(Oid relid, TimestampTz target_ts,
							 FbRelationInfo *info_out, TupleDesc *tupdesc_out)
{
	fb_progress_enter_stage(FB_PROGRESS_STAGE_VALIDATE, NULL);
	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_require_supported_target_relation(relid, info_out);
	*tupdesc_out = fb_relation_tupledesc(relid);
}

/*
 * fb_build_flashback_reverse_ops
 *    SQL entry helper.
 */

static void
fb_build_flashback_reverse_ops(TimestampTz target_ts,
							   const FbRelationInfo *info,
							   TupleDesc tupdesc,
							   FbReverseOpStream *reverse)
{
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbForwardOpStream forward;

	fb_progress_enter_stage(FB_PROGRESS_STAGE_PREPARE_WAL, NULL);
	fb_wal_prepare_scan_context(target_ts, &scan_ctx);
	fb_wal_build_record_index(info, &scan_ctx, &index);
	if (index.unsafe)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb does not support WAL windows containing %s operations",
						fb_wal_unsafe_reason_name(index.unsafe_reason))));

	fb_build_forward_ops(info, &index, tupdesc, &forward);
	fb_build_reverse_ops(&forward, reverse);
}

/*
 * fb_flashback_cleanup_callback
 *    SQL entry helper.
 */

static void
fb_flashback_cleanup_callback(Datum arg)
{
	FbFlashbackQueryState *state = (FbFlashbackQueryState *) DatumGetPointer(arg);

	if (state != NULL && state->apply != NULL)
	{
		fb_apply_end(state->apply);
		state->apply = NULL;
	}

	fb_progress_abort();
}

/*
 * fb_unregister_flashback_cleanup
 *    SQL entry helper.
 */

static void
fb_unregister_flashback_cleanup(FbFlashbackQueryState *state)
{
	if (state == NULL || !state->cleanup_registered || state->econtext == NULL)
		return;

	UnregisterExprContextCallback(state->econtext,
								  fb_flashback_cleanup_callback,
								  PointerGetDatum(state));
	state->cleanup_registered = false;
}

/*
 * fb_version
 *    SQL entry point.
 */

Datum
fb_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("0.1.0-dev"));
}

/*
 * fb_check_relation
 *    SQL entry point.
 */

Datum
fb_check_relation(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	FbRelationInfo info;
	StringInfoData buf;

	fb_catalog_load_relation_info(relid, &info);

	initStringInfo(&buf);
	appendStringInfo(&buf, "mode=%s has_toast=%s",
					 info.mode_name,
					 OidIsValid(info.toast_relid) ? "true" : "false");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * pg_flashback
 *    SQL entry point.
 */

Datum
pg_flashback(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	FbFlashbackQueryState *state;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		Oid source_relid;
		TimestampTz target_ts;

		if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pg_flashback must be called in a set-returning context")));

		if ((rsinfo->allowedModes & SFRM_ValuePerCall) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pg_flashback requires value-per-call SRF mode")));

		if (rsinfo->econtext == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_flashback is missing executor expression context")));

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		state = palloc0(sizeof(*state));
		funcctx->user_fctx = state;

		fb_progress_begin();
		state->econtext = rsinfo->econtext;
		RegisterExprContextCallback(rsinfo->econtext,
									fb_flashback_cleanup_callback,
									PointerGetDatum(state));
		state->cleanup_registered = true;

		PG_TRY();
		{
			source_relid = fb_resolve_target_type_relid(fcinfo);
			target_ts = fb_parse_target_ts_text(PG_GETARG_TEXT_PP(1));
			fb_validate_flashback_target(source_relid, target_ts, &state->info, &state->tupdesc);
			fb_build_flashback_reverse_ops(target_ts, &state->info, state->tupdesc, &state->reverse);
			state->apply = fb_apply_begin(&state->info,
										 state->tupdesc,
										 &state->reverse);
		}
		PG_CATCH();
		{
			fb_flashback_cleanup_callback(PointerGetDatum(state));
			MemoryContextSwitchTo(oldcontext);
			PG_RE_THROW();
		}
		PG_END_TRY();

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	state = (FbFlashbackQueryState *) funcctx->user_fctx;

	PG_TRY();
	{
		HeapTuple tuple;
		Datum result;

		tuple = fb_apply_next(state->apply);
		if (tuple != NULL)
		{
			result = heap_copy_tuple_as_datum(tuple, state->tupdesc);
			SRF_RETURN_NEXT(funcctx, result);
		}

		if (state->apply != NULL)
		{
			fb_apply_end(state->apply);
			state->apply = NULL;
		}

		fb_unregister_flashback_cleanup(state);
		fb_progress_finish();
	}
	PG_CATCH();
	{
		fb_flashback_cleanup_callback(PointerGetDatum(state));
		PG_RE_THROW();
	}
	PG_END_TRY();

	SRF_RETURN_DONE(funcctx);
}

/*
 * fb_export_undo
 *    SQL entry point.
 */

Datum
fb_export_undo(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_require_supported_target_relation(relid, &info);
	fb_wal_prepare_scan_context(target_ts, &scan_ctx);
	fb_wal_build_record_index(&info, &scan_ctx, &index);
	if (index.unsafe)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb does not support WAL windows containing %s operations",
						fb_wal_unsafe_reason_name(index.unsafe_reason))));
	fb_raise_not_implemented("fb_export_undo page replay + reverse op stream");
	PG_RETURN_NULL();
}
