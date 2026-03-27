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
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_entry.h"
#include "fb_error.h"
#include "fb_guc.h"
#include "fb_progress.h"
#include "fb_replay.h"
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
	FbSpoolSession *spool;
	FbReverseOpSource *reverse;
	FbApplyContext *apply;
	ExprContext *econtext;
	bool cleanup_registered;
} FbFlashbackQueryState;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fb_version);
PG_FUNCTION_INFO_V1(fb_check_relation);
PG_FUNCTION_INFO_V1(fb_recordref_debug);
PG_FUNCTION_INFO_V1(fb_wal_sidecar_debug);
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
fb_append_qualified_relation_name(StringInfo buf, Oid relid)
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
fb_build_unsafe_detail(const FbRelationInfo *info,
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
	fb_append_qualified_relation_name(&buf, info->relid);

	if (index->unsafe_scope == FB_WAL_UNSAFE_SCOPE_TOAST &&
		OidIsValid(info->toast_relid))
	{
		appendStringInfoString(&buf, " toast=");
		fb_append_qualified_relation_name(&buf, info->toast_relid);
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
fb_build_flashback_reverse_ops(TimestampTz target_ts,
							   const FbRelationInfo *info,
							   TupleDesc tupdesc,
							   FbSpoolSession *spool,
							   FbReverseOpSource **reverse_out)
{
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbReplayResult replay_result;
	FbReverseOpSource *reverse;

	fb_progress_enter_stage(FB_PROGRESS_STAGE_PREPARE_WAL, NULL);
	fb_wal_prepare_scan_context(target_ts, spool, &scan_ctx);
	fb_wal_build_record_index(info, &scan_ctx, &index);
	if (index.unsafe)
	{
		char *detail = fb_build_unsafe_detail(info, &index);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb does not support WAL windows containing %s operations",
						fb_wal_unsafe_reason_name(index.unsafe_reason)),
				 errdetail_internal("%s", detail)));
	}

	MemSet(&replay_result, 0, sizeof(replay_result));
	replay_result.tracked_bytes = index.tracked_bytes;
	replay_result.memory_limit_bytes = index.memory_limit_bytes;
	reverse = fb_reverse_source_create(spool,
									   &replay_result.tracked_bytes,
									   replay_result.memory_limit_bytes);
	fb_replay_build_reverse_source(info, &index, tupdesc, &replay_result, reverse);
	fb_reverse_source_finish(reverse);
	*reverse_out = reverse;
}

static void
fb_prepare_flashback_query(FbFlashbackQueryState *state,
						   Oid source_relid,
						   TimestampTz target_ts)
{
	state->spool = fb_spool_session_create();
	fb_validate_flashback_target(source_relid, target_ts, &state->info, &state->tupdesc);
	fb_build_flashback_reverse_ops(target_ts,
								   &state->info,
								   state->tupdesc,
								   state->spool,
								   &state->reverse);
	state->apply = fb_apply_begin(&state->info,
								  state->tupdesc,
								  state->reverse);
	fb_reverse_source_destroy(state->reverse);
	state->reverse = NULL;
	fb_spool_session_destroy(state->spool);
	state->spool = NULL;
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
	if (state != NULL && state->reverse != NULL)
	{
		fb_reverse_source_destroy(state->reverse);
		state->reverse = NULL;
	}
	if (state != NULL && state->spool != NULL)
	{
		fb_spool_session_destroy(state->spool);
		state->spool = NULL;
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
 * fb_recordref_debug
 *    SQL entry point for regression-only index diagnostics.
 */

Datum
fb_recordref_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbSpoolSession *spool = NULL;
	StringInfoData buf;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_catalog_load_relation_info(relid, &info);

	PG_TRY();
	{
		spool = fb_spool_session_create();
		fb_wal_prepare_scan_context(target_ts, spool, &scan_ctx);
		fb_wal_build_record_index(&info, &scan_ctx, &index);

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "anchor=%s unsafe=%s reason=%s meta_refs=%llu payload_refs=%u kept=%llu target_dml=%llu commits=%llu aborts=%llu tail_inline=%s head_gap_refs=%u tail_refs=%u",
						 index.anchor_found ? "true" : "false",
						 index.unsafe ? "true" : "false",
						 fb_wal_unsafe_reason_name(index.unsafe_reason),
						 (unsigned long long) index.total_record_count,
						 index.record_count,
						 (unsigned long long) index.kept_record_count,
						 (unsigned long long) index.target_record_count,
						 (unsigned long long) index.target_commit_count,
						 (unsigned long long) index.target_abort_count,
						 index.tail_inline_payload ? "true" : "false",
						 fb_spool_log_count(index.record_log),
						 fb_spool_log_count(index.record_tail_log));

		fb_spool_session_destroy(spool);
		spool = NULL;
		PG_RETURN_TEXT_P(cstring_to_text(buf.data));
	}
	PG_CATCH();
	{
		if (spool != NULL)
			fb_spool_session_destroy(spool);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * fb_wal_sidecar_debug
 *    SQL entry point for regression-only sidecar diagnostics.
 */

Datum
fb_wal_sidecar_debug(PG_FUNCTION_ARGS)
{
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbWalScanContext scan_ctx;
	StringInfoData buf;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_wal_prepare_scan_context(target_ts, NULL, &scan_ctx);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "anchor_hint=%s start_pruned=%s checkpoint_entries=%u",
					 scan_ctx.anchor_hint_found ? "true" : "false",
					 scan_ctx.start_lsn_pruned ? "true" : "false",
					 scan_ctx.checkpoint_sidecar_entries);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * pg_flashback
 *    SQL entry point.
 */

Datum
pg_flashback(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	FuncCallContext *funcctx;
	FbFlashbackQueryState *state;
	Oid source_relid = InvalidOid;
	bool prefer_materialize = false;

	if (rsinfo != NULL && IsA(rsinfo, ReturnSetInfo))
	{
		source_relid = fb_resolve_target_type_relid(fcinfo);
		prefer_materialize = ((rsinfo->allowedModes & SFRM_Materialize) != 0);
	}

	if (prefer_materialize)
	{
		FbFlashbackQueryState state;
		TimestampTz target_ts;
		bits32 srf_flags = 0;

		if (rsinfo->econtext == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("pg_flashback is missing executor expression context")));

		MemSet(&state, 0, sizeof(state));
		target_ts = fb_parse_target_ts_text(PG_GETARG_TEXT_PP(1));

		fb_progress_begin();
		PG_TRY();
		{
			fb_prepare_flashback_query(&state, source_relid, target_ts);
			if (rsinfo->expectedDesc != NULL)
				srf_flags |= MAT_SRF_USE_EXPECTED_DESC;
			InitMaterializedSRF(fcinfo, srf_flags);
			fb_apply_materialize(state.apply, rsinfo->setResult);
			fb_apply_end(state.apply);
			state.apply = NULL;
			fb_progress_finish();
			PG_RETURN_NULL();
		}
		PG_CATCH();
		{
			fb_flashback_cleanup_callback(PointerGetDatum(&state));
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
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
			if (!OidIsValid(source_relid))
				source_relid = fb_resolve_target_type_relid(fcinfo);
			target_ts = fb_parse_target_ts_text(PG_GETARG_TEXT_PP(1));
			fb_prepare_flashback_query(state, source_relid, target_ts);
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
		Datum result;

		if (fb_apply_next(state->apply, &result))
		{
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
	FbSpoolSession *spool;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_require_supported_target_relation(relid, &info);
	spool = fb_spool_session_create();
	fb_wal_prepare_scan_context(target_ts, spool, &scan_ctx);
	fb_wal_build_record_index(&info, &scan_ctx, &index);
	if (index.unsafe)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("fb does not support WAL windows containing %s operations",
						fb_wal_unsafe_reason_name(index.unsafe_reason))));
	fb_spool_session_destroy(spool);
	fb_raise_not_implemented("fb_export_undo page replay + reverse op stream");
	PG_RETURN_NULL();
}
