#include "postgres.h"

#include "access/relation.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/table.h"
#include "funcapi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_decode.h"
#include "fb_entry.h"
#include "fb_error.h"
#include "fb_guc.h"
#include "fb_runtime.h"
#include "fb_replay.h"
#include "fb_reverse_ops.h"
#include "fb_wal.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fb_version);
PG_FUNCTION_INFO_V1(fb_check_relation);
PG_FUNCTION_INFO_V1(fb_wal_source_debug);
PG_FUNCTION_INFO_V1(fb_scan_wal_debug);
PG_FUNCTION_INFO_V1(fb_recordref_debug);
PG_FUNCTION_INFO_V1(fb_replay_debug);
PG_FUNCTION_INFO_V1(fb_wal_window_debug);
PG_FUNCTION_INFO_V1(fb_decode_insert_debug);
PG_FUNCTION_INFO_V1(pg_flashback);
PG_FUNCTION_INFO_V1(fb_export_undo);

static void
fb_require_target_ts_not_future(TimestampTz target_ts)
{
	if (target_ts > GetCurrentTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target timestamp is in the future")));
}

static void
fb_require_supported_target_relation(Oid relid, FbRelationInfo *info)
{
	fb_catalog_load_relation_info(relid, info);
}

static TupleDesc
fb_relation_tupledesc(Oid relid)
{
	Relation rel;
	TupleDesc tupdesc;

	rel = relation_open(relid, AccessShareLock);
	tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));
	relation_close(rel, AccessShareLock);

	return tupdesc;
}

static ReturnSetInfo *
fb_require_materialize_mode(FunctionCallInfo fcinfo)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required")));

	rsinfo->returnMode = SFRM_Materialize;
	return rsinfo;
}

static char *
fb_lsn_text(XLogRecPtr lsn)
{
	return psprintf("%X/%08X", LSN_FORMAT_ARGS(lsn));
}

static Tuplestorestate *
fb_begin_materialized_result(FunctionCallInfo fcinfo, TupleDesc stored_tupdesc)
{
	ReturnSetInfo *rsinfo = fb_require_materialize_mode(fcinfo);
	MemoryContext old_context;
	MemoryContext per_query_ctx;
	Tuplestorestate *tuplestore;
	bool random_access;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	old_context = MemoryContextSwitchTo(per_query_ctx);
	random_access = (rsinfo->allowedModes & SFRM_Materialize_Random) != 0;
	tuplestore = tuplestore_begin_heap(random_access, false, work_mem);
	rsinfo->setResult = tuplestore;
	rsinfo->setDesc = stored_tupdesc;
	MemoryContextSwitchTo(old_context);

	return tuplestore;
}

Datum
fb_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("0.1.0-dev"));
}

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

Datum
fb_wal_source_debug(PG_FUNCTION_ARGS)
{
	FbWalScanContext scan_ctx;
	char *summary;

	fb_require_archive_dir();
	fb_wal_prepare_scan_context(GetCurrentTimestamp(), &scan_ctx);
	summary = fb_wal_source_debug_summary(&scan_ctx);

	PG_RETURN_TEXT_P(cstring_to_text(summary));
}

Datum
fb_scan_wal_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext scan_ctx;
	char *summary;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_require_supported_target_relation(relid, &info);
	fb_wal_prepare_scan_context(target_ts, &scan_ctx);
	fb_wal_scan_relation_window(&info, &scan_ctx);
	summary = fb_wal_debug_summary(&scan_ctx);

	PG_RETURN_TEXT_P(cstring_to_text(summary));
}

Datum
fb_recordref_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	char *summary;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_require_supported_target_relation(relid, &info);
	fb_wal_prepare_scan_context(target_ts, &scan_ctx);
	fb_wal_build_record_index(&info, &scan_ctx, &index);
	summary = fb_wal_index_debug_summary(&scan_ctx, &index);

	PG_RETURN_TEXT_P(cstring_to_text(summary));
}

Datum
fb_replay_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbReplayResult replay_result;
	char *summary;

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

	fb_replay_execute(&info, &index, &replay_result);
	summary = fb_replay_debug_summary(&replay_result);

	PG_RETURN_TEXT_P(cstring_to_text(summary));
}

Datum
fb_wal_window_debug(PG_FUNCTION_ARGS)
{
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(0);
	FbWalScanContext scan_ctx;
	XLogRecPtr current_lsn;
	TimeLineID current_tli;
	XLogSegNo current_segno;
	char *current_lsn_text;
	char current_wal[MAXFNAMELEN];
	char *start_lsn_text;
	char *end_lsn_text;
	char *summary;

	fb_require_target_ts_not_future(target_ts);
	fb_require_archive_dir();
	fb_wal_prepare_scan_context(target_ts, &scan_ctx);

	current_lsn = scan_ctx.end_lsn;
	current_tli = GetWALInsertionTimeLineIfSet();
	if (current_tli == 0)
		current_tli = scan_ctx.timeline_id;

	current_lsn_text = fb_lsn_text(current_lsn);
	start_lsn_text = fb_lsn_text(scan_ctx.start_lsn);
	end_lsn_text = fb_lsn_text(scan_ctx.end_lsn);
	XLByteToSeg(current_lsn, current_segno, scan_ctx.wal_seg_size);
	XLogFileName(current_wal, current_tli, current_segno, scan_ctx.wal_seg_size);

	summary = psprintf("current_lsn=%s current_wal=%s start_lsn=%s start_wal=%s end_lsn=%s end_wal=%s",
					   current_lsn_text,
					   current_wal,
					   start_lsn_text,
					   scan_ctx.first_segment,
					   end_lsn_text,
					   scan_ctx.last_segment);

	pfree(current_lsn_text);
	pfree(start_lsn_text);
	pfree(end_lsn_text);

	PG_RETURN_TEXT_P(cstring_to_text(summary));
}

Datum
fb_decode_insert_debug(PG_FUNCTION_ARGS)
{
	fb_raise_not_implemented("fb_decode_insert_debug forward-op debug output");
	PG_RETURN_NULL();
}

Datum
pg_flashback(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext scan_ctx;
	FbWalRecordIndex index;
	FbForwardOpStream forward;
	FbReverseOpStream reverse;
	TupleDesc tupdesc;
	Tuplestorestate *tuplestore;

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

	tupdesc = fb_relation_tupledesc(relid);
	BlessTupleDesc(tupdesc);
	fb_build_forward_ops(&info, &index, tupdesc, &forward);
	fb_build_reverse_ops(&forward, &reverse);
	tuplestore = fb_begin_materialized_result(fcinfo, tupdesc);
	fb_apply_reverse_ops(&info, tupdesc, &reverse, tuplestore);
	PG_RETURN_NULL();
}

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
