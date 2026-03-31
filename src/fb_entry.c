/*
 * fb_entry.c
 *    SQL entry points and direct-query orchestration.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/supportnodes.h"
#include "parser/parse_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
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
#include "fb_memory.h"
#include "fb_progress.h"
#include "fb_replay.h"
#include "fb_reverse_ops.h"
#include "fb_runtime.h"
#include "fb_wal.h"

/*
 * FbFlashbackQueryState
 *    Tracks one SRF execution state.
 */

struct FbFlashbackQueryState
{
	FbRelationInfo info;
	FbFastPathSpec fast_path;
	TupleDesc tupdesc;
	FbSpoolSession *spool;
	FbReverseOpSource *reverse;
	FbApplyContext *apply;
	ExprContext *econtext;
	bool cleanup_registered;
	bool finished;
	bool aborted;
};

typedef struct FbPreflightEstimate
{
	uint64 wal_bytes;
	uint64 reverse_bytes;
	uint64 apply_bytes;
	uint64 total_bytes;
} FbPreflightEstimate;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fb_version);
PG_FUNCTION_INFO_V1(fb_check_relation);
PG_FUNCTION_INFO_V1(fb_archive_resolve_debug);
PG_FUNCTION_INFO_V1(fb_apply_debug);
PG_FUNCTION_INFO_V1(fb_keyed_key_debug);
PG_FUNCTION_INFO_V1(fb_prepare_wal_scan_debug);
PG_FUNCTION_INFO_V1(fb_recordref_debug);
PG_FUNCTION_INFO_V1(fb_progress_debug_set_clock);
PG_FUNCTION_INFO_V1(fb_progress_debug_reset_clock);
PG_FUNCTION_INFO_V1(fb_srf_mode_debug);
PG_FUNCTION_INFO_V1(fb_wal_sidecar_debug);
PG_FUNCTION_INFO_V1(fb_pg_flashback_support);
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

static const char *
fb_choose_srf_mode(ReturnSetInfo *rsinfo)
{
	bool allow_value_per_call = false;
	bool allow_materialize = false;

	if (rsinfo != NULL && IsA(rsinfo, ReturnSetInfo))
	{
		allow_value_per_call = ((rsinfo->allowedModes & SFRM_ValuePerCall) != 0);
		allow_materialize = ((rsinfo->allowedModes & SFRM_Materialize) != 0);
	}

	if (allow_value_per_call)
		return "value_per_call";
	if (allow_materialize)
		return "materialize_only";
	return "invalid";
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

static uint64
fb_estimate_add_u64(uint64 left, uint64 right)
{
	if (PG_UINT64_MAX - left < right)
		return PG_UINT64_MAX;

	return left + right;
}

static uint64
fb_estimate_mul_u64(uint64 left, uint64 right)
{
	if (left == 0 || right == 0)
		return 0;
	if (left > PG_UINT64_MAX / right)
		return PG_UINT64_MAX;

	return left * right;
}

static uint64
fb_estimate_tuple_bytes(TupleDesc tupdesc)
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

		data_width = fb_estimate_add_u64(data_width, (uint64) avgwidth);
	}

	return MAXALIGN(HEAPTUPLESIZE + SizeofHeapTupleHeader + data_width);
}

static void
fb_copy_fast_path_spec(FbFastPathSpec *dst, const FbFastPathSpec *src)
{
	int16 typlen = -1;
	bool typbyval = false;
	int i;

	MemSet(dst, 0, sizeof(*dst));
	if (src == NULL || src->mode == FB_FAST_PATH_NONE)
		return;

	*dst = *src;
	get_typlenbyval(src->key_type_oid, &typlen, &typbyval);

	if (src->has_lower_bound && !src->lower_isnull)
		dst->lower_value = datumCopy(src->lower_value, typbyval, typlen);
	if (src->has_upper_bound && !src->upper_isnull)
		dst->upper_value = datumCopy(src->upper_value, typbyval, typlen);

	if (src->key_count <= 0 || src->key_values == NULL)
		return;
	dst->key_values = palloc0(sizeof(Datum) * src->key_count);
	dst->key_nulls = palloc0(sizeof(bool) * src->key_count);

	for (i = 0; i < src->key_count; i++)
	{
		dst->key_nulls[i] = src->key_nulls[i];
		dst->key_values[i] = datumCopy(src->key_values[i], typbyval, typlen);
	}
}

static void
fb_preflight_append_detail(StringInfo buf,
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
fb_preflight_estimate_working_set(const FbRelationInfo *info,
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

	tuple_bytes = fb_estimate_tuple_bytes(tupdesc);
	record_count = index->kept_record_count;
	target_count = index->target_record_count;

	wal_bytes = (uint64) fb_spool_log_size(index->record_log);
	wal_bytes = fb_estimate_add_u64(wal_bytes,
									(uint64) fb_spool_log_size(index->record_tail_log));
	wal_bytes = fb_estimate_add_u64(wal_bytes,
									fb_estimate_mul_u64(record_count,
														(uint64) sizeof(FbRecordRef)));

	reverse_bytes = fb_estimate_mul_u64(target_count,
										 (uint64) sizeof(FbReverseOp));
	reverse_bytes = fb_estimate_add_u64(reverse_bytes,
										 fb_estimate_mul_u64(target_count,
														 tuple_bytes));
	reverse_bytes = fb_estimate_add_u64(reverse_bytes,
										 fb_estimate_mul_u64(index->target_update_count,
														 tuple_bytes));

	apply_per_entry = tuple_bytes + ((info != NULL && info->mode == FB_APPLY_KEYED) ?
									 UINT64CONST(128) : UINT64CONST(64));
	estimate_out->wal_bytes = wal_bytes;
	estimate_out->reverse_bytes = reverse_bytes;
	estimate_out->apply_bytes = fb_estimate_mul_u64(target_count, apply_per_entry);
	estimate_out->total_bytes = fb_estimate_add_u64(estimate_out->wal_bytes,
													 estimate_out->reverse_bytes);
	estimate_out->total_bytes = fb_estimate_add_u64(estimate_out->total_bytes,
													 estimate_out->apply_bytes);
}

static bool
fb_preflight_allow_disk_spill(const FbRelationInfo *info,
							  TupleDesc tupdesc,
							  const FbWalRecordIndex *index)
{
	FbPreflightEstimate estimate;
	FbSpillMode mode = fb_get_spill_mode();
	uint64 limit_bytes = fb_get_memory_limit_bytes();
	StringInfoData detail;

	fb_preflight_estimate_working_set(info, tupdesc, index, &estimate);
	if (limit_bytes == 0 || estimate.total_bytes <= limit_bytes)
		return mode == FB_SPILL_MODE_DISK;

	initStringInfo(&detail);
	fb_preflight_append_detail(&detail, estimate.total_bytes, limit_bytes, mode);

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
							   bool shareable_reverse,
							   FbReverseOpSource **reverse_out)
{
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbReplayResult replay_result;
	FbReverseOpSource *reverse;
	bool allow_disk_spill;

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

	allow_disk_spill = fb_preflight_allow_disk_spill(info, tupdesc, &index);

	MemSet(&replay_result, 0, sizeof(replay_result));
	replay_result.tracked_bytes = index.tracked_bytes;
	replay_result.memory_limit_bytes = index.memory_limit_bytes;
	reverse = fb_reverse_source_create((allow_disk_spill || shareable_reverse) ? spool : NULL,
									   &replay_result.tracked_bytes,
									   replay_result.memory_limit_bytes);
	fb_replay_build_reverse_source(info, &index, tupdesc, &replay_result, reverse);
	fb_reverse_source_finish(reverse);
	if (shareable_reverse)
		fb_reverse_source_materialize(reverse);
	*reverse_out = reverse;
}

void
fb_flashback_build_reverse_state(Oid source_relid,
								 text *target_ts_text,
								 bool shareable_reverse,
								 FbFlashbackReverseBuildState *state)
{
	TimestampTz target_ts;

	if (state == NULL)
		return;

	MemSet(state, 0, sizeof(*state));
	target_ts = fb_parse_target_ts_text(target_ts_text);
	fb_progress_begin();

	PG_TRY();
	{
		state->spool = fb_spool_session_create();
		fb_validate_flashback_target(source_relid,
									 target_ts,
									 &state->info,
									 &state->tupdesc);
		fb_build_flashback_reverse_ops(target_ts,
									   &state->info,
									   state->tupdesc,
									   state->spool,
									   shareable_reverse,
									   &state->reverse);
	}
	PG_CATCH();
	{
		fb_flashback_release_reverse_state(state);
		fb_progress_abort();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
fb_flashback_release_reverse_state(FbFlashbackReverseBuildState *state)
{
	if (state == NULL)
		return;

	if (state->reverse != NULL)
	{
		fb_reverse_source_destroy(state->reverse);
		state->reverse = NULL;
	}
	if (state->spool != NULL)
	{
		fb_spool_session_destroy(state->spool);
		state->spool = NULL;
	}

	fb_runtime_cleanup_stale();
}

static void
fb_prepare_flashback_query(FbFlashbackQueryState *state,
						   Oid source_relid,
						   TimestampTz target_ts)
{
	bool shareable_reverse = false;

	state->spool = fb_spool_session_create();
	fb_validate_flashback_target(source_relid, target_ts, &state->info, &state->tupdesc);
	shareable_reverse = fb_apply_parallel_candidate(&state->info,
													 &state->fast_path,
													 state->info.relid);
	fb_build_flashback_reverse_ops(target_ts,
								   &state->info,
								   state->tupdesc,
								   state->spool,
								   shareable_reverse,
								   &state->reverse);
	state->apply = fb_apply_begin(&state->info,
								  state->tupdesc,
								  state->reverse,
								  &state->fast_path);
	fb_reverse_source_destroy(state->reverse);
	state->reverse = NULL;
	fb_spool_session_destroy(state->spool);
	state->spool = NULL;
}

static void
fb_flashback_query_release(FbFlashbackQueryState *state)
{
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

	fb_runtime_cleanup_stale();
}

/*
 * fb_flashback_cleanup_callback
 *    SQL entry helper.
 */

static void
fb_flashback_cleanup_callback(Datum arg)
{
	FbFlashbackQueryState *state = (FbFlashbackQueryState *) DatumGetPointer(arg);

	if (state == NULL || state->aborted || state->finished)
		return;

	fb_flashback_query_release(state);
	state->aborted = true;
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

FbFlashbackQueryState *
fb_flashback_query_begin(Oid source_relid,
						 text *target_ts_text,
						 const FbFastPathSpec *fast_path)
{
	FbFlashbackQueryState *state;
	TimestampTz target_ts;

	state = palloc0(sizeof(*state));
	fb_copy_fast_path_spec(&state->fast_path, fast_path);
	target_ts = fb_parse_target_ts_text(target_ts_text);
	fb_progress_begin();

	PG_TRY();
	{
		fb_prepare_flashback_query(state, source_relid, target_ts);
	}
	PG_CATCH();
	{
		fb_flashback_query_abort(state);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return state;
}

bool
fb_flashback_query_next_datum(FbFlashbackQueryState *state, Datum *result)
{
	if (state == NULL || state->apply == NULL)
		return false;

	return fb_apply_next(state->apply, result);
}

bool
fb_flashback_query_next_slot(FbFlashbackQueryState *state, TupleTableSlot *slot)
{
	if (state == NULL || state->apply == NULL || slot == NULL)
		return false;

	return fb_apply_next_slot(state->apply, slot);
}

TupleDesc
fb_flashback_query_tupdesc(FbFlashbackQueryState *state)
{
	if (state == NULL)
		return NULL;

	return state->tupdesc;
}

void
fb_flashback_query_finish(FbFlashbackQueryState *state)
{
	if (state == NULL || state->finished)
		return;

	fb_flashback_query_release(state);
	state->finished = true;
	fb_progress_finish();
}

void
fb_flashback_query_abort(FbFlashbackQueryState *state)
{
	if (state == NULL)
		return;

	fb_flashback_cleanup_callback(PointerGetDatum(state));
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
 * fb_archive_resolve_debug
 *    SQL entry point for regression-only archive source diagnostics.
 */

Datum
fb_archive_resolve_debug(PG_FUNCTION_ARGS)
{
	FbArchiveDirSource source;
	const char *setting_name = NULL;
	char *archive_dir;
	StringInfoData buf;

	archive_dir = fb_resolve_archive_dir(&source, &setting_name);
	fb_require_archive_dir();

	initStringInfo(&buf);
	appendStringInfo(&buf, "mode=%s setting=%s path=%s",
					 fb_archive_dir_source_name(source),
					 setting_name,
					 archive_dir);

	pfree(archive_dir);
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

Datum
fb_prepare_wal_scan_debug(PG_FUNCTION_ARGS)
{
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(0);
	FbWalScanContext scan_ctx;
	StringInfoData buf;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_wal_prepare_scan_context(target_ts, NULL, &scan_ctx);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "timeline=%u first=%s last=%s total=%u archive=%u pg_wal=%u ckwal=%u",
					 scan_ctx.timeline_id,
					 scan_ctx.first_segment,
					 scan_ctx.last_segment,
					 scan_ctx.total_segments,
					 scan_ctx.archive_segment_count,
					 scan_ctx.pg_wal_segment_count,
					 scan_ctx.ckwal_segment_count);

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
						 "anchor=%s unsafe=%s reason=%s meta_refs=%llu payload_refs=%u kept=%llu target_dml=%llu commits=%llu aborts=%llu tail_inline=%s head_gap_refs=%u tail_refs=%u parallel=%s prefilter=%s visited_segments=%u/%u payload_windows=%u payload_parallel_workers=%u summary_span_windows=%u summary_xid_hits=%u summary_xid_fallback=%u summary_xid_segments_read=%u summary_unsafe_hits=%u metadata_fallback_windows=%u",
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
						 fb_spool_log_count(index.record_tail_log),
						 scan_ctx.parallel_workers > 0 ? "on" : "off",
						 scan_ctx.segment_prefilter_used ? "on" : "off",
						 scan_ctx.visited_segment_count,
						 scan_ctx.progress_segment_total,
						 index.payload_window_count,
						 index.payload_parallel_workers,
						 scan_ctx.summary_span_windows,
						 scan_ctx.summary_xid_hits,
						 scan_ctx.summary_xid_fallback,
						 scan_ctx.summary_xid_segments_read,
						 scan_ctx.summary_unsafe_hits,
						 scan_ctx.metadata_fallback_windows);

		fb_spool_session_destroy(spool);
		spool = NULL;
		fb_runtime_cleanup_stale();
		PG_RETURN_TEXT_P(cstring_to_text(buf.data));
	}
	PG_CATCH();
	{
		if (spool != NULL)
			fb_spool_session_destroy(spool);
		fb_runtime_cleanup_stale();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * fb_apply_debug
 *    SQL entry point for regression-only apply diagnostics.
 */

Datum
fb_apply_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbFlashbackQueryState *state = NULL;
	StringInfoData buf;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();

	PG_TRY();
	{
		state = palloc0(sizeof(*state));
		fb_progress_begin();
		fb_prepare_flashback_query(state, relid, target_ts);

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "mode=%s apply_parallel=%s parallel_logs=%d fast_path=%s",
						 state->info.mode == FB_APPLY_KEYED ? "keyed" : "bag",
						 fb_apply_parallel_materialized(state->apply) ? "on" : "off",
						 fb_apply_parallel_log_count(state->apply),
						 state->fast_path.mode == FB_FAST_PATH_NONE ? "off" : "on");

		fb_flashback_query_release(state);
		state->finished = true;
		fb_progress_finish();
		PG_RETURN_TEXT_P(cstring_to_text(buf.data));
	}
	PG_CATCH();
	{
		if (state != NULL)
			fb_flashback_query_abort(state);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * fb_keyed_key_debug
 *    SQL entry point for regression-only keyed state diagnostics.
 */

Datum
fb_keyed_key_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	int64 key = PG_GETARG_INT64(2);
	FbFlashbackQueryState *state = NULL;
	void *keyed_state = NULL;
	FbKeyedResidualItem *residual_items = NULL;
	FbKeyedDeleteKey *delete_keys = NULL;
	uint64 residual_count = 0;
	uint64 delete_count = 0;
	bool supports_single_typed = false;
	bool target_in_residual = false;
	bool target_in_delete = false;
	bool emitted = false;
	int64 emitted_tuple_id = 0;
	bool emitted_tuple_id_isnull = true;
	char *emitted_code = NULL;
	FbApplyEmit emit = {0};
	StringInfoData buf;
	uint64 i;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();

	PG_TRY();
	{
		state = palloc0(sizeof(*state));
		fb_progress_begin();
		fb_prepare_flashback_query(state, relid, target_ts);

		if (state->info.mode != FB_APPLY_KEYED)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("fb_keyed_key_debug only supports keyed relations")));

		keyed_state = fb_keyed_apply_begin(&state->info,
										   state->tupdesc,
										   state->reverse);
		supports_single_typed = fb_keyed_apply_supports_single_typed_key(keyed_state);
		residual_items = fb_keyed_apply_collect_residual_items(keyed_state, &residual_count);
		delete_keys = fb_keyed_apply_collect_delete_keys(keyed_state, &delete_count);

		for (i = 0; i < residual_count; i++)
		{
			bool tuple_id_isnull = true;
			int64 tuple_id = DatumGetInt64(heap_getattr(residual_items[i].tuple,
														1,
														state->tupdesc,
														&tuple_id_isnull));

			if (!residual_items[i].key_isnull &&
				DatumGetInt64(residual_items[i].key_value) == key)
			{
				target_in_residual = true;
				if (!tuple_id_isnull)
				{
					emitted_tuple_id = tuple_id;
					emitted_tuple_id_isnull = false;
				}
			}
		}

		for (i = 0; i < delete_count; i++)
		{
			if (!delete_keys[i].key_isnull &&
				DatumGetInt64(delete_keys[i].key_value) == key)
			{
				target_in_delete = true;
				break;
			}
		}

		emitted = fb_keyed_apply_emit_missing_key(keyed_state,
												  Int64GetDatum(key),
												  false,
												  &emit);
		if (emitted && emit.kind == FB_APPLY_EMIT_TUPLE && emit.tuple != NULL)
		{
			bool tuple_id_isnull = true;
			bool code_isnull = true;
			Datum code_value;

			emitted_tuple_id = DatumGetInt64(heap_getattr(emit.tuple,
														  1,
														  state->tupdesc,
														  &tuple_id_isnull));
			emitted_tuple_id_isnull = tuple_id_isnull;
			code_value = heap_getattr(emit.tuple,
									  9,
									  state->tupdesc,
									  &code_isnull);
			if (!code_isnull)
				emitted_code = TextDatumGetCString(code_value);
		}

		initStringInfo(&buf);
		appendStringInfo(&buf,
						 "supports_single_typed=%s residual_count=%llu delete_count=%llu target_in_residual=%s target_in_delete=%s emit_missing_key=%s emitted_tuple_id=%s%s%s emitted_code=%s",
						 supports_single_typed ? "true" : "false",
						 (unsigned long long) residual_count,
						 (unsigned long long) delete_count,
						 target_in_residual ? "true" : "false",
						 target_in_delete ? "true" : "false",
						 emitted ? "true" : "false",
						 emitted_tuple_id_isnull ? "NULL" : psprintf("%lld", (long long) emitted_tuple_id),
						 target_in_residual ? " residual_tuple_id=" : "",
						 (target_in_residual && !emitted_tuple_id_isnull) ? psprintf("%lld", (long long) emitted_tuple_id) : "",
						 emitted_code != NULL ? emitted_code : "NULL");

		if (keyed_state != NULL)
			fb_keyed_apply_end(keyed_state);
		fb_flashback_query_release(state);
		state->finished = true;
		fb_progress_finish();
		PG_RETURN_TEXT_P(cstring_to_text(buf.data));
	}
	PG_CATCH();
	{
		if (keyed_state != NULL)
			fb_keyed_apply_end(keyed_state);
		if (state != NULL)
			fb_flashback_query_abort(state);
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
 * fb_progress_debug_set_clock
 *    SQL entry point for regression-only deterministic progress timing.
 */

Datum
fb_progress_debug_set_clock(PG_FUNCTION_ARGS)
{
	ArrayType  *values = PG_GETARG_ARRAYTYPE_P(0);
	Datum	   *datums;
	bool	   *nulls;
	int			nelems;
	int64	   *script;
	int			i;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	if (ARR_NDIM(values) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("fb_progress_debug_set_clock expects a one-dimensional bigint array")));
	if (ARR_ELEMTYPE(values) != INT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("fb_progress_debug_set_clock expects bigint[]")));

	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(values,
					  INT8OID,
					  typlen,
					  typbyval,
					  typalign,
					  &datums,
					  &nulls,
					  &nelems);

	script = palloc(sizeof(int64) * Max(nelems, 1));
	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("fb_progress_debug_set_clock does not accept NULL elements")));
		script[i] = DatumGetInt64(datums[i]);
	}

	fb_progress_debug_set_clock_script(script, nelems);
	pfree(script);
	pfree(datums);
	pfree(nulls);

	PG_RETURN_VOID();
}

/*
 * fb_progress_debug_reset_clock
 *    SQL entry point for regression-only deterministic progress timing.
 */

Datum
fb_progress_debug_reset_clock(PG_FUNCTION_ARGS)
{
	fb_progress_debug_clear_clock();
	PG_RETURN_VOID();
}

Datum
fb_srf_mode_debug(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		funcctx->user_fctx = psprintf("chosen=%s",
									  fb_choose_srf_mode(rsinfo));
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	if (funcctx->call_cntr == 0)
		SRF_RETURN_NEXT(funcctx,
						CStringGetTextDatum((char *) funcctx->user_fctx));

	SRF_RETURN_DONE(funcctx);
}

/*
 * fb_pg_flashback_support
 *    Planner support stub. Its main job is to load the module early enough
 *    that _PG_init() can register the CustomScan hook before set_rel_pathlist.
 */

Datum
fb_pg_flashback_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);

	if (rawreq == NULL)
		PG_RETURN_POINTER(NULL);

	if (IsA(rawreq, SupportRequestRows))
		PG_RETURN_POINTER(NULL);
	if (IsA(rawreq, SupportRequestCost))
		PG_RETURN_POINTER(NULL);

	PG_RETURN_POINTER(NULL);
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

	if (rsinfo != NULL && IsA(rsinfo, ReturnSetInfo))
		source_relid = fb_resolve_target_type_relid(fcinfo);

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

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
			state->econtext = rsinfo->econtext;
			funcctx->user_fctx = state;
			RegisterExprContextCallback(rsinfo->econtext,
										fb_flashback_cleanup_callback,
										PointerGetDatum(state));
			state->cleanup_registered = true;

			PG_TRY();
			{
				if (!OidIsValid(source_relid))
					source_relid = fb_resolve_target_type_relid(fcinfo);
				fb_progress_begin();
				fb_prepare_flashback_query(state,
										   source_relid,
										   fb_parse_target_ts_text(PG_GETARG_TEXT_PP(1)));
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

			if (fb_flashback_query_next_datum(state, &result))
			{
				SRF_RETURN_NEXT(funcctx, result);
			}

			fb_flashback_query_finish(state);
			fb_unregister_flashback_cleanup(state);
		}
		PG_CATCH();
		{
			fb_flashback_query_abort(state);
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
	fb_runtime_cleanup_stale();
	fb_raise_not_implemented("fb_export_undo page replay + reverse op stream");
	PG_RETURN_NULL();
}
