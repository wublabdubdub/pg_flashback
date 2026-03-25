#include "postgres.h"

#include "catalog/namespace.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_entry.h"
#include "fb_error.h"
#include "fb_guc.h"
#include "fb_parallel.h"
#include "fb_progress.h"
#include "fb_reverse_ops.h"
#include "fb_wal.h"

typedef struct FbTableSinkState
{
	Relation rel;
	TupleTableSlot *slot;
	BulkInsertState bistate;
	CommandId cid;
} FbTableSinkState;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(fb_version);
PG_FUNCTION_INFO_V1(fb_check_relation);
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

static Oid
fb_resolve_relation_name(text *name_text, LOCKMODE lockmode, bool missing_ok)
{
	List	   *names;
	RangeVar   *rv;

	names = textToQualifiedNameList(name_text);
	rv = makeRangeVarFromNameList(names);
	return RangeVarGetRelid(rv, lockmode, missing_ok);
}

static char *
fb_require_result_name(text *name_text)
{
	List	   *names = textToQualifiedNameList(name_text);

	if (list_length(names) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("flashback result relation name must be unqualified")));

	return pstrdup(strVal(linitial(names)));
}

static TimestampTz
fb_parse_target_ts_text(text *target_ts_text)
{
	char	   *target_ts_cstr = text_to_cstring(target_ts_text);

	return DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
												   CStringGetDatum(target_ts_cstr),
												   ObjectIdGetDatum(InvalidOid),
												   Int32GetDatum(-1)));
}

static char *
fb_build_create_table_sql(const char *result_name, TupleDesc tupdesc)
{
	StringInfoData buf;
	bool		first = true;
	int			i;

	initStringInfo(&buf);
	appendStringInfo(&buf, "CREATE UNLOGGED TABLE %s (",
					 quote_identifier(result_name));

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		char	   *type_sql;

		if (attr->attisdropped)
			continue;

		type_sql = format_type_with_typemod(attr->atttypid, attr->atttypmod);
		appendStringInfo(&buf, "%s%s %s",
						 first ? "" : ", ",
						 quote_identifier(NameStr(attr->attname)),
						 type_sql);
		first = false;
	}

	if (first)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("could not derive columns for flashback result table")));

	appendStringInfoChar(&buf, ')');
	return buf.data;
}

static void
fb_table_sink_put_tuple(HeapTuple tuple, TupleDesc tupdesc, void *arg)
{
	FbTableSinkState *state = (FbTableSinkState *) arg;

	(void) tupdesc;

	ExecStoreHeapTuple(tuple, state->slot, false);
	table_tuple_insert(state->rel, state->slot, state->cid, 0, state->bistate);
	ExecClearTuple(state->slot);
}

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
pg_flashback(PG_FUNCTION_ARGS)
{
	text	   *result_name_text = PG_GETARG_TEXT_PP(0);
	text	   *source_name_text = PG_GETARG_TEXT_PP(1);
	text	   *target_ts_text = PG_GETARG_TEXT_PP(2);
	char	   *result_name;
	Oid			source_relid;
	TimestampTz target_ts;
	FbRelationInfo info;
	TupleDesc	tupdesc;
	char	   *create_sql;
	FbReverseOpStream reverse;
	Relation	result_rel;
	TupleTableSlot *write_slot;
	FbTableSinkState sink_state;
	FbResultSink sink;
	uint64		result_count;
	bool		use_parallel = false;

	fb_progress_begin();

	PG_TRY();
	{
		result_name = fb_require_result_name(result_name_text);
		if (fb_resolve_relation_name(result_name_text, NoLock, true) != InvalidOid)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("flashback target relation \"%s\" already exists",
							result_name)));

		source_relid = fb_resolve_relation_name(source_name_text, AccessShareLock, false);
		target_ts = fb_parse_target_ts_text(target_ts_text);
		fb_validate_flashback_target(source_relid, target_ts, &info, &tupdesc);
		create_sql = fb_build_create_table_sql(result_name, tupdesc);
		fb_build_flashback_reverse_ops(target_ts, &info, tupdesc, &reverse);
		use_parallel = fb_parallel_apply_workers() > 0;

		if (use_parallel)
			result_count = fb_parallel_apply_reverse_ops(result_name,
														 create_sql,
														 &info,
														 tupdesc,
														 &reverse);
		else
		{
			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "SPI_connect failed");
			if (SPI_execute(create_sql, false, 0) != SPI_OK_UTILITY)
				elog(ERROR, "CREATE UNLOGGED TABLE failed");
			if (SPI_finish() != SPI_OK_FINISH)
				elog(ERROR, "SPI_finish failed");

			result_rel = relation_open(fb_resolve_relation_name(cstring_to_text(result_name),
															 RowExclusiveLock,
															 false),
									   RowExclusiveLock);
			write_slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsHeapTuple);
			sink_state.rel = result_rel;
			sink_state.slot = write_slot;
			sink_state.bistate = GetBulkInsertState();
			sink_state.cid = GetCurrentCommandId(true);
			sink.put_tuple = fb_table_sink_put_tuple;
			sink.arg = &sink_state;

			result_count = fb_apply_reverse_ops(&info, tupdesc, &reverse, &sink);

			table_finish_bulk_insert(result_rel, 0);
			FreeBulkInsertState(sink_state.bistate);
			ExecDropSingleTupleTableSlot(write_slot);
			relation_close(result_rel, RowExclusiveLock);
		}
		(void) result_count;
		fb_progress_finish();

		PG_RETURN_TEXT_P(cstring_to_text(result_name));
	}
	PG_CATCH();
	{
		fb_progress_abort();
		PG_RE_THROW();
	}
	PG_END_TRY();

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
