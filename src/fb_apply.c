/*
 * fb_apply.c
 *    Streaming apply coordinator for flashback query results.
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/stratnum.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

#include "fb_apply.h"
#include "fb_catalog.h"
#include "fb_guc.h"
#include "fb_progress.h"
#include "fb_spool.h"

#define FB_APPLY_PARALLEL_MAX_WORKERS 16
#define FB_APPLY_PARALLEL_ERRMSG_MAX 512
#define FB_APPLY_PARALLEL_GUC_MAX 64
#define FB_APPLY_PARALLEL_MIN_REL_PAGES 64
#define FB_APPLY_PARALLEL_MAX_REL_PAGES 32768
#define FB_APPLY_PARALLEL_TUPLE_QUEUE_SIZE (1024 * 1024)

typedef enum FbApplyParallelTaskStatus
{
	FB_APPLY_PARALLEL_TASK_INIT = 0,
	FB_APPLY_PARALLEL_TASK_RUNNING,
	FB_APPLY_PARALLEL_TASK_DONE,
	FB_APPLY_PARALLEL_TASK_ERROR
} FbApplyParallelTaskStatus;

typedef struct FbApplyParallelTask
{
	int status;
	Oid dboid;
	Oid useroid;
	Oid relid;
	int shard_id;
	int shard_count;
	uint64 row_count;
	uint32 tuple_count;
	uint32 seen_count;
	char snapshot_name[MAXPGPATH];
	char spool_path[MAXPGPATH];
	char seen_path[MAXPGPATH];
	char memory_limit[FB_APPLY_PARALLEL_GUC_MAX];
	char errmsg[FB_APPLY_PARALLEL_ERRMSG_MAX];
} FbApplyParallelTask;

typedef struct FbApplyParallelShared
{
	int task_count;
	int worker_count;
	Size tasks_offset;
	Size reverse_shared_offset;
	Size reverse_shared_size;
	Size pscan_offset;
	Size pscan_size;
	Size tuple_queue_offset;
	Size tuple_queue_stride;
	char data[FLEXIBLE_ARRAY_MEMBER];
} FbApplyParallelShared;

typedef struct FbApplySerializedTupleHeader
{
	uint32 tuple_len;
} FbApplySerializedTupleHeader;

typedef enum FbApplyPhase
{
	FB_APPLY_PHASE_SCAN = 1,
	FB_APPLY_PHASE_RESIDUAL,
	FB_APPLY_PHASE_DONE
} FbApplyPhase;

typedef struct FbApplyBufferedEmit
{
	bool ready;
	FbApplyEmit emit;
	Datum key_value;
	bool key_isnull;
	FbKeyedResidualItem *residual_item;
} FbApplyBufferedEmit;

typedef struct FbApplyFastPathState
{
	FbFastPathSpec spec;
	Relation index_rel;
	IndexScanDesc index_scan;
	ScanKeyData scankeys[2];
	int scankey_count;
	bool scankeys_ready;
	int key_cursor;
	uint64 emitted_count;
	FbKeyedResidualItem *residual_items;
	uint64 residual_count;
	uint64 residual_cursor;
	FmgrInfo cmp_fmgr;
	bool has_cmp;
	int16 key_typlen;
	bool key_typbyval;
	FbApplyBufferedEmit current_candidate;
	FbApplyBufferedEmit residual_candidate;
} FbApplyFastPathState;

struct FbApplyContext
{
	const FbRelationInfo *info;
	const FbFastPathSpec *fast_path;
	TupleDesc tupdesc;
	Relation rel;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool slot_is_bound_output;
	FbApplyFastPathState *fast_state;
	bool pushed_snapshot;
	FbApplyPhase phase;
	uint64 estimated_rows;
	uint64 scanned_rows;
	uint64 progress_stride;
	uint64 residual_total;
	uint64 residual_emitted;
	bool parallel_materialized;
	FbSpoolSession *parallel_spool;
	dsm_segment *parallel_seg;
	FbApplyParallelShared *parallel_shared;
	FbApplyParallelTask *parallel_tasks;
	BackgroundWorkerHandle **parallel_handles;
	shm_mq_handle **parallel_tuple_handles;
	bool *parallel_worker_done;
	bool parallel_seen_merged;
	int parallel_active_workers;
	int parallel_log_count;
	int parallel_log_index;
	MemoryContext parallel_rowctx;
	void *mode_state;
	bool count_only_mode;
	bool count_only_slot_ready;
	uint64 count_only_total;
	uint64 count_only_emitted;
};

PGDLLEXPORT void fb_apply_parallel_worker_main(Datum main_arg);

static FbApplyParallelTask *fb_apply_parallel_tasks(FbApplyParallelShared *shared);
static const void *fb_apply_parallel_reverse_shared_const(const FbApplyParallelShared *shared);
static ParallelTableScanDesc fb_apply_parallel_pscan(FbApplyParallelShared *shared);
static shm_mq *fb_apply_parallel_tuple_queue(FbApplyParallelShared *shared, int task_index);
static void fb_apply_parallel_copy_setting(char *dst, Size dstlen, const char *name);
static void fb_apply_parallel_fill_task(FbApplyParallelTask *task,
										Oid relid,
										const char *snapshot_name,
										const char *spool_path,
										const char *seen_path,
										int shard_id,
										int shard_count);
static void fb_apply_parallel_apply_gucs(const FbApplyParallelTask *task);
static int fb_apply_parallel_target_workers(void);
static void fb_apply_serialize_tuple(StringInfo buf, HeapTuple tuple);
static bool fb_apply_read_parallel_emit(FbApplyContext *ctx, FbApplyEmit *emit);
static bool fb_apply_parallel_supported(const FbRelationInfo *info,
										 const FbFastPathSpec *fast_path,
										 Relation rel);
static bool fb_apply_parallel_launch_worker(dsm_handle handle,
											 int task_index,
											 const char *worker_name,
											 BackgroundWorkerHandle **handle_out);
static void fb_apply_parallel_wait_worker(BackgroundWorkerHandle *handle,
										   const FbApplyParallelTask *task);
static void fb_apply_parallel_merge_seen(FbApplyContext *ctx,
										 const char *path,
										 uint32 item_count);
static bool fb_apply_parallel_materialize(FbApplyContext *ctx,
										   const FbReverseOpSource *source);
static void fb_apply_reset_buffered_emit(FbApplyBufferedEmit *buffer);
static void fb_apply_take_buffered_emit(FbApplyBufferedEmit *buffer,
										FbApplyEmit *emit);
static bool fb_apply_next_count_only_emit(FbApplyContext *ctx,
										  FbApplyEmit *emit);

static FbApplyParallelTask *
fb_apply_parallel_tasks(FbApplyParallelShared *shared)
{
	return (FbApplyParallelTask *) ((char *) shared + shared->tasks_offset);
}

static const void *
fb_apply_parallel_reverse_shared_const(const FbApplyParallelShared *shared)
{
	return (const char *) shared + shared->reverse_shared_offset;
}

static ParallelTableScanDesc
fb_apply_parallel_pscan(FbApplyParallelShared *shared)
{
	return (ParallelTableScanDesc) ((char *) shared + shared->pscan_offset);
}

static shm_mq *
fb_apply_parallel_tuple_queue(FbApplyParallelShared *shared, int task_index)
{
	return (shm_mq *) ((char *) shared +
					   shared->tuple_queue_offset +
					   (shared->tuple_queue_stride * task_index));
}

static void
fb_apply_reset_buffered_emit(FbApplyBufferedEmit *buffer)
{
	if (buffer == NULL)
		return;

	if (buffer->emit.owned_tuple && buffer->emit.tuple != NULL)
		heap_freetuple(buffer->emit.tuple);
	MemSet(buffer, 0, sizeof(*buffer));
}

static void
fb_apply_take_buffered_emit(FbApplyBufferedEmit *buffer, FbApplyEmit *emit)
{
	if (buffer == NULL || emit == NULL)
		return;

	*emit = buffer->emit;
	buffer->ready = false;
	buffer->emit.kind = FB_APPLY_EMIT_NONE;
	buffer->emit.slot = NULL;
	buffer->emit.tuple = NULL;
	buffer->emit.owned_tuple = false;
	buffer->residual_item = NULL;
}

static bool
fb_apply_next_count_only_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	if (ctx == NULL || emit == NULL || ctx->slot == NULL)
		return false;

	if (ctx->count_only_emitted >= ctx->count_only_total)
	{
		ctx->phase = FB_APPLY_PHASE_DONE;
		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 100, NULL);
		fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
		return false;
	}

	if (!ctx->count_only_slot_ready)
	{
		int natts = ctx->slot->tts_tupleDescriptor->natts;

		ExecClearTuple(ctx->slot);
		memset(ctx->slot->tts_values, 0, sizeof(Datum) * natts);
		memset(ctx->slot->tts_isnull, true, sizeof(bool) * natts);
		ExecStoreVirtualTuple(ctx->slot);
		ctx->slot->tts_tableOid = ctx->info->relid;
		ctx->count_only_slot_ready = true;
	}

	memset(emit, 0, sizeof(*emit));
	emit->kind = FB_APPLY_EMIT_SLOT;
	emit->slot = ctx->slot;
	ctx->count_only_emitted++;
	if (ctx->count_only_total > 0)
		fb_progress_update_fraction(FB_PROGRESS_STAGE_APPLY,
									ctx->count_only_emitted,
									ctx->count_only_total,
									NULL);
	return true;
}

static void
fb_apply_parallel_copy_setting(char *dst, Size dstlen, const char *name)
{
	char *value;

	value = GetConfigOptionByName(name, NULL, false);
	if (value == NULL)
		elog(ERROR, "could not read GUC \"%s\" for parallel apply worker", name);
	if (strlen(value) >= dstlen)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("GUC \"%s\" is too long for parallel apply worker state", name)));

	strlcpy(dst, value, dstlen);
}

static void
fb_apply_parallel_fill_task(FbApplyParallelTask *task,
							Oid relid,
							const char *snapshot_name,
							const char *spool_path,
							const char *seen_path,
							int shard_id,
							int shard_count)
{
	MemSet(task, 0, sizeof(*task));
	task->status = FB_APPLY_PARALLEL_TASK_INIT;
	task->dboid = MyDatabaseId;
	task->useroid = GetUserId();
	task->relid = relid;
	task->shard_id = shard_id;
	task->shard_count = shard_count;
	if (snapshot_name != NULL)
		strlcpy(task->snapshot_name, snapshot_name, sizeof(task->snapshot_name));
	if (spool_path != NULL)
		strlcpy(task->spool_path, spool_path, sizeof(task->spool_path));
	if (seen_path != NULL)
		strlcpy(task->seen_path, seen_path, sizeof(task->seen_path));
	fb_apply_parallel_copy_setting(task->memory_limit,
								   sizeof(task->memory_limit),
								   "pg_flashback.memory_limit");
}

static void
fb_apply_parallel_apply_gucs(const FbApplyParallelTask *task)
{
	SetConfigOption("pg_flashback.memory_limit",
					task->memory_limit,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("default_transaction_isolation",
					"repeatable read",
					PGC_USERSET,
					PGC_S_SESSION);
}

static int
fb_apply_parallel_target_workers(void)
{
	int worker_count;
	int max_parallel = max_parallel_workers_per_gather;
	int max_bgworkers = max_worker_processes - 1;

	worker_count = Min(fb_parallel_workers(), FB_APPLY_PARALLEL_MAX_WORKERS);
	if (max_parallel > 0)
		worker_count = Min(worker_count, max_parallel);
	if (max_bgworkers > 0)
		worker_count = Min(worker_count, max_bgworkers);
	return worker_count;
}

static void
fb_apply_serialize_tuple(StringInfo buf, HeapTuple tuple)
{
	FbApplySerializedTupleHeader hdr;

	if (buf == NULL || tuple == NULL)
		return;

	resetStringInfo(buf);
	MemSet(&hdr, 0, sizeof(hdr));
	hdr.tuple_len = tuple->t_len;
	appendBinaryStringInfo(buf, (const char *) &hdr, sizeof(hdr));
	appendBinaryStringInfo(buf, (const char *) tuple->t_data, tuple->t_len);
}

static bool
fb_apply_parallel_supported(const FbRelationInfo *info,
							const FbFastPathSpec *fast_path,
							Relation rel)
{
	if (fb_parallel_workers() <= 1)
		return false;
	if (info == NULL || info->mode != FB_APPLY_KEYED)
		return false;
	if (fast_path != NULL && fast_path->mode != FB_FAST_PATH_NONE)
		return false;
	if (rel == NULL || rel->rd_rel == NULL)
		return false;
	if (rel->rd_rel->relpages < FB_APPLY_PARALLEL_MIN_REL_PAGES)
		return false;
	if (rel->rd_rel->relpages > FB_APPLY_PARALLEL_MAX_REL_PAGES)
		return false;

	return true;
}

bool
fb_apply_parallel_candidate(const FbRelationInfo *info,
							  const FbFastPathSpec *fast_path,
							  Oid relid)
{
	Relation rel;
	bool supported;

	if (!OidIsValid(relid))
		return false;

	rel = relation_open(relid, AccessShareLock);
	supported = fb_apply_parallel_supported(info, fast_path, rel);
	relation_close(rel, AccessShareLock);
	return supported;
}

static void
fb_apply_slot_get_attrs(TupleTableSlot *slot,
						  const AttrNumber *attrs,
						  int attr_count)
{
	int max_attr = 0;
	int i;

	if (slot == NULL)
		return;

	if (attrs == NULL || attr_count <= 0)
	{
		slot_getallattrs(slot);
		return;
	}

	for (i = 0; i < attr_count; i++)
	{
		if (attrs[i] > max_attr)
			max_attr = attrs[i];
	}

	if (max_attr > 0)
		slot_getsomeattrs(slot, max_attr);
}

static char *
fb_apply_build_identity_values(TupleDesc tupdesc,
								 const Datum *values,
								 const bool *nulls,
								 const AttrNumber *attrs,
								 int attr_count)
{
	StringInfoData buf;
	int i;

	initStringInfo(&buf);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		bool include = (attrs == NULL);

		if (attr->attisdropped)
			continue;

		if (!include)
		{
			int key_index;

			for (key_index = 0; key_index < attr_count; key_index++)
			{
				if (attrs[key_index] == attr->attnum)
				{
					include = true;
					break;
				}
			}
		}

		if (!include)
			continue;

		if (buf.len > 0)
			appendStringInfoChar(&buf, '|');

		appendStringInfoString(&buf, NameStr(attr->attname));
		appendStringInfoChar(&buf, '=');

		if (nulls[i])
			appendStringInfoString(&buf, "NULL");
		else
		{
			Oid output_func;
			bool is_varlena;
			char *value_text;
			char *quoted;

			getTypeOutputInfo(attr->atttypid, &output_func, &is_varlena);
			value_text = OidOutputFunctionCall(output_func, values[i]);
			quoted = quote_literal_cstr(value_text);
			appendStringInfoString(&buf, quoted);
		}
	}

	return buf.data;
}

char *
fb_apply_build_key_identity_slot(const FbRelationInfo *info,
								   TupleTableSlot *slot,
								   TupleDesc tupdesc)
{
	if (info == NULL || slot == NULL || tupdesc == NULL)
		return NULL;

	fb_apply_slot_get_attrs(slot, info->key_attnums, info->key_natts);
	return fb_apply_build_identity_values(tupdesc,
										  slot->tts_values,
										  slot->tts_isnull,
										  info->key_attnums,
										  info->key_natts);
}

char *
fb_apply_build_row_identity_slot(TupleTableSlot *slot,
								   TupleDesc tupdesc)
{
	if (slot == NULL || tupdesc == NULL)
		return NULL;

	fb_apply_slot_get_attrs(slot, NULL, 0);
	return fb_apply_build_identity_values(tupdesc,
										  slot->tts_values,
										  slot->tts_isnull,
										  NULL,
										  0);
}

static void
fb_apply_copy_fast_path_spec(FbFastPathSpec *dst, const FbFastPathSpec *src)
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

static int
fb_apply_compare_key_values(FbApplyFastPathState *fast,
							  Datum left,
							  bool left_isnull,
							  Datum right,
							  bool right_isnull)
{
	if (left_isnull && right_isnull)
		return 0;
	if (left_isnull)
		return -1;
	if (right_isnull)
		return 1;

	return DatumGetInt32(FunctionCall2Coll(&fast->cmp_fmgr,
										   fast->spec.key_collation,
										   left,
										   right));
}

typedef struct FbApplyKeyItem
{
	Datum value;
	bool isnull;
} FbApplyKeyItem;

static int
fb_apply_compare_key_items(const void *left, const void *right, void *arg)
{
	const FbApplyKeyItem *left_item = (const FbApplyKeyItem *) left;
	const FbApplyKeyItem *right_item = (const FbApplyKeyItem *) right;
	FbApplyFastPathState *fast = (FbApplyFastPathState *) arg;
	int cmp;

	cmp = fb_apply_compare_key_values(fast,
									  left_item->value,
									  left_item->isnull,
									  right_item->value,
									  right_item->isnull);
	return fast->spec.order_asc ? cmp : -cmp;
}

static int
fb_apply_compare_residual_items(const void *left, const void *right, void *arg)
{
	const FbKeyedResidualItem *left_item = (const FbKeyedResidualItem *) left;
	const FbKeyedResidualItem *right_item = (const FbKeyedResidualItem *) right;
	FbApplyFastPathState *fast = (FbApplyFastPathState *) arg;
	int cmp;

	cmp = fb_apply_compare_key_values(fast,
									  left_item->key_value,
									  left_item->key_isnull,
									  right_item->key_value,
									  right_item->key_isnull);
	return fast->spec.order_asc ? cmp : -cmp;
}

static void
fb_apply_sort_fast_keys(FbApplyFastPathState *fast)
{
	FbApplyKeyItem *items;
	int i;

	if (fast == NULL || fast->spec.key_count <= 1 || !fast->has_cmp)
		return;

	items = palloc0(sizeof(*items) * fast->spec.key_count);
	for (i = 0; i < fast->spec.key_count; i++)
	{
		items[i].value = fast->spec.key_values[i];
		items[i].isnull = fast->spec.key_nulls[i];
	}

	qsort_arg(items,
			  fast->spec.key_count,
			  sizeof(*items),
			  fb_apply_compare_key_items,
			  fast);

	for (i = 0; i < fast->spec.key_count; i++)
	{
		fast->spec.key_values[i] = items[i].value;
		fast->spec.key_nulls[i] = items[i].isnull;
	}
}

static void
fb_apply_sort_residual_items(FbApplyFastPathState *fast)
{
	if (fast == NULL || fast->residual_count <= 1 || !fast->has_cmp)
		return;

	qsort_arg(fast->residual_items,
			  fast->residual_count,
			  sizeof(*fast->residual_items),
			  fb_apply_compare_residual_items,
			  fast);
}

static bool
fb_apply_fast_key_in_range(FbApplyFastPathState *fast,
						   Datum value,
						   bool isnull)
{
	int cmp;

	if (fast == NULL)
		return false;
	if (isnull)
		return false;

	if (fast->spec.has_lower_bound)
	{
		cmp = fb_apply_compare_key_values(fast,
										  value,
										  isnull,
										  fast->spec.lower_value,
										  fast->spec.lower_isnull);
		if (cmp < 0)
			return false;
		if (cmp == 0 && !fast->spec.lower_inclusive)
			return false;
	}

	if (fast->spec.has_upper_bound)
	{
		cmp = fb_apply_compare_key_values(fast,
										  value,
										  isnull,
										  fast->spec.upper_value,
										  fast->spec.upper_isnull);
		if (cmp > 0)
			return false;
		if (cmp == 0 && !fast->spec.upper_inclusive)
			return false;
	}

	return true;
}

static void
fb_apply_extract_key_from_slot(FbApplyContext *ctx,
								 TupleTableSlot *slot,
								 Datum *value_out,
								 bool *isnull_out)
{
	bool isnull = false;
	Datum value;

	value = slot_getattr(slot, ctx->fast_state->spec.key_attnum, &isnull);
	*value_out = value;
	*isnull_out = isnull;
}

static void
fb_apply_extract_key_from_tuple(FbApplyContext *ctx,
								  HeapTuple tuple,
								  Datum *value_out,
								  bool *isnull_out)
{
	*value_out = heap_getattr(tuple,
							  ctx->fast_state->spec.key_attnum,
							  ctx->tupdesc,
							  isnull_out);
}

static void
fb_apply_extract_key_from_emit(FbApplyContext *ctx,
								 const FbApplyEmit *emit,
								 Datum *value_out,
								 bool *isnull_out)
{
	if (emit->kind == FB_APPLY_EMIT_SLOT)
	{
		fb_apply_extract_key_from_slot(ctx, emit->slot, value_out, isnull_out);
		return;
	}

	fb_apply_extract_key_from_tuple(ctx, emit->tuple, value_out, isnull_out);
}

static void
fb_apply_buffer_emit(FbApplyContext *ctx,
					   const FbApplyEmit *emit,
					   FbKeyedResidualItem *residual_item,
					   FbApplyBufferedEmit *buffer)
{
	fb_apply_reset_buffered_emit(buffer);
	buffer->ready = true;
	buffer->residual_item = residual_item;

	if (emit->kind == FB_APPLY_EMIT_SLOT)
	{
		buffer->emit.kind = FB_APPLY_EMIT_TUPLE;
		buffer->emit.slot = NULL;
		buffer->emit.tuple = ExecCopySlotHeapTuple(emit->slot);
		buffer->emit.owned_tuple = true;
	}
	else
		buffer->emit = *emit;

	fb_apply_extract_key_from_emit(ctx, &buffer->emit, &buffer->key_value, &buffer->key_isnull);
}

static void
fb_apply_parallel_worker_do(dsm_segment *seg,
							FbApplyParallelTask *task,
							FbApplyParallelShared *shared)
{
	FbRelationInfo info;
	Relation rel = NULL;
	FbReverseOpReader *reader = NULL;
	void *apply_state = NULL;
	TableScanDesc scan = NULL;
	TupleTableSlot *slot = NULL;
	FbSpoolLog *seen_log = NULL;
	shm_mq *tuple_queue = NULL;
	shm_mq_handle *tuple_handle = NULL;
	StringInfoData tuple_buf;
	bool tuple_buf_ready = false;

	fb_catalog_load_relation_info(task->relid, &info);
	rel = relation_open(task->relid, AccessShareLock);
	reader = fb_reverse_reader_open_shared(
		fb_apply_parallel_reverse_shared_const(shared));
	apply_state = fb_keyed_apply_begin_reader_ex(&info,
												 RelationGetDescr(rel),
												 reader,
												 (long) Min(fb_reverse_source_shared_total_count(
													 fb_apply_parallel_reverse_shared_const(shared)),
													 (uint64) LONG_MAX),
												 0,
												 fb_get_memory_limit_bytes(),
												 -1,
												 0,
												 false);
	fb_reverse_reader_close(reader);
	reader = NULL;

	tuple_queue = fb_apply_parallel_tuple_queue(shared, task->shard_id);
	shm_mq_set_sender(tuple_queue, MyProc);
	tuple_handle = shm_mq_attach(tuple_queue, seg, NULL);
	if (shm_mq_wait_for_attach(tuple_handle) != SHM_MQ_SUCCESS)
		elog(ERROR, "pg_flashback apply tuple queue could not attach");
	seen_log = fb_spool_log_create_path(task->seen_path);
	scan = table_beginscan_parallel(rel, fb_apply_parallel_pscan(shared));
	slot = table_slot_create(rel, NULL);
	initStringInfo(&tuple_buf);
	tuple_buf_ready = true;

	PG_TRY();
	{
		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			FbApplyEmit emit = {0};
			char *matched_identity = NULL;
			Datum matched_key = (Datum) 0;
			bool matched_key_isnull = true;
			bool matched_key_found = false;

			if (fb_keyed_apply_supports_parallel_single_typed_key(apply_state))
			{
				if (fb_keyed_apply_process_current_emit_typed_key(apply_state,
																  slot,
																  &emit,
																  &matched_key,
																  &matched_key_isnull,
																  &matched_key_found))
				{
					HeapTuple tuple_to_write = NULL;

					if (emit.kind == FB_APPLY_EMIT_SLOT)
						tuple_to_write = ExecCopySlotHeapTuple(emit.slot);
					else if (emit.kind == FB_APPLY_EMIT_TUPLE)
						tuple_to_write = emit.tuple;

					if (tuple_to_write != NULL)
					{
						fb_apply_serialize_tuple(&tuple_buf, tuple_to_write);
						if (shm_mq_send(tuple_handle,
										tuple_buf.len,
										tuple_buf.data,
										false,
										false) == SHM_MQ_DETACHED)
							break;
						task->row_count++;
						if (emit.kind == FB_APPLY_EMIT_SLOT)
							heap_freetuple(tuple_to_write);
					}
				}
			}
			else if (fb_keyed_apply_process_current_emit_identity(apply_state,
																 slot,
																 &emit,
																 &matched_identity))
			{
				HeapTuple tuple_to_write = NULL;

				if (emit.kind == FB_APPLY_EMIT_SLOT)
					tuple_to_write = ExecCopySlotHeapTuple(emit.slot);
				else if (emit.kind == FB_APPLY_EMIT_TUPLE)
					tuple_to_write = emit.tuple;

				if (tuple_to_write != NULL)
				{
					fb_apply_serialize_tuple(&tuple_buf, tuple_to_write);
					if (shm_mq_send(tuple_handle,
									tuple_buf.len,
									tuple_buf.data,
									false,
									false) == SHM_MQ_DETACHED)
						break;
					task->row_count++;
					if (emit.kind == FB_APPLY_EMIT_SLOT)
						heap_freetuple(tuple_to_write);
				}
			}

			if (matched_key_found)
			{
				fb_keyed_apply_serialize_single_typed_key(apply_state,
														 matched_key,
														 matched_key_isnull,
														 &tuple_buf);
				fb_spool_log_append(seen_log, tuple_buf.data, (uint32) tuple_buf.len);
			}
			else if (matched_identity != NULL)
			{
				fb_spool_log_append(seen_log,
									matched_identity,
									(uint32) (strlen(matched_identity) + 1));
				pfree(matched_identity);
			}

			ExecClearTuple(slot);
		}

		task->seen_count = fb_spool_log_count(seen_log);
		fb_spool_log_close(seen_log);
		seen_log = NULL;
		shm_mq_detach(tuple_handle);
		tuple_handle = NULL;
		if (tuple_buf_ready)
		{
			pfree(tuple_buf.data);
			tuple_buf_ready = false;
		}
		ExecDropSingleTupleTableSlot(slot);
		slot = NULL;
		table_endscan(scan);
		scan = NULL;
		relation_close(rel, AccessShareLock);
		rel = NULL;
		fb_keyed_apply_end(apply_state);
		apply_state = NULL;
	}
	PG_CATCH();
	{
		if (seen_log != NULL)
			fb_spool_log_close(seen_log);
		if (tuple_handle != NULL)
			shm_mq_detach(tuple_handle);
		if (tuple_buf_ready)
			pfree(tuple_buf.data);
		if (slot != NULL)
			ExecDropSingleTupleTableSlot(slot);
		if (scan != NULL)
			table_endscan(scan);
		if (rel != NULL)
			relation_close(rel, AccessShareLock);
		if (apply_state != NULL)
			fb_keyed_apply_end(apply_state);
		if (reader != NULL)
			fb_reverse_reader_close(reader);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
fb_apply_parallel_worker_main(Datum main_arg)
{
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg;
	FbApplyParallelShared *shared;
	int task_index;
	FbApplyParallelTask *task;

	memcpy(&task_index, MyBgworkerEntry->bgw_extra, sizeof(task_index));
	BackgroundWorkerUnblockSignals();

	seg = dsm_attach(handle);
	if (seg == NULL)
		proc_exit(1);

	shared = (FbApplyParallelShared *) dsm_segment_address(seg);
	task = &fb_apply_parallel_tasks(shared)[task_index];
	task->status = FB_APPLY_PARALLEL_TASK_RUNNING;

	BackgroundWorkerInitializeConnectionByOid(task->dboid, task->useroid, 0);
	fb_apply_parallel_apply_gucs(task);

	PG_TRY();
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		if (task->snapshot_name[0] != '\0')
			ImportSnapshot(task->snapshot_name);
		PushActiveSnapshot(GetTransactionSnapshot());
		fb_apply_parallel_worker_do(seg, task, shared);
		PopActiveSnapshot();
		CommitTransactionCommand();
		task->status = FB_APPLY_PARALLEL_TASK_DONE;
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();

		FlushErrorState();
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		if (IsTransactionState())
			AbortCurrentTransaction();
		strlcpy(task->errmsg, edata->message, sizeof(task->errmsg));
		task->status = FB_APPLY_PARALLEL_TASK_ERROR;
	}
	PG_END_TRY();

	dsm_detach(seg);
	proc_exit(0);
}

static bool
fb_apply_parallel_launch_worker(dsm_handle handle,
								int task_index,
								const char *worker_name,
								BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker worker;
	pid_t pid;
	BgwHandleStatus status;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", worker_name);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback apply");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_apply_parallel_worker_main");
	worker.bgw_main_arg = UInt32GetDatum(handle);
	memcpy(worker.bgw_extra, &task_index, sizeof(task_index));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, handle_out))
		return false;

	status = WaitForBackgroundWorkerStartup(*handle_out, &pid);
	if (status != BGWH_STARTED)
		return false;

	return true;
}

static void
fb_apply_parallel_wait_worker(BackgroundWorkerHandle *handle,
							  const FbApplyParallelTask *task)
{
	BgwHandleStatus status;

	status = WaitForBackgroundWorkerShutdown(handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("postmaster died while waiting for pg_flashback apply worker")));
	if (task->status != FB_APPLY_PARALLEL_TASK_DONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_flashback apply worker failed"),
				 errdetail_internal("%s",
									task->errmsg[0] != '\0' ? task->errmsg : "worker exited without success")));
}

static void
fb_apply_parallel_merge_seen(FbApplyContext *ctx,
							 const char *path,
							 uint32 item_count)
{
	FbSpoolLog *log;
	FbSpoolCursor *cursor;
	StringInfoData buf;

	if (ctx == NULL || ctx->mode_state == NULL || path == NULL || item_count == 0)
		return;

	log = fb_spool_log_open_readonly(path, item_count);
	cursor = fb_spool_cursor_open(log, FB_SPOOL_FORWARD);
	initStringInfo(&buf);
	while (fb_spool_cursor_read(cursor, &buf, NULL))
	{
		if (fb_keyed_apply_supports_parallel_single_typed_key(ctx->mode_state))
			fb_keyed_apply_mark_serialized_single_typed_seen(ctx->mode_state,
															 buf.data,
															 buf.len);
		else
			fb_keyed_apply_mark_identity_seen(ctx->mode_state, buf.data);
	}
	pfree(buf.data);
	fb_spool_cursor_close(cursor);
	fb_spool_log_close(log);
}

static bool
fb_apply_read_parallel_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	while (ctx->parallel_active_workers > 0)
	{
		int offset;
		FbApplySerializedTupleHeader hdr;
		MemoryContext oldctx;
		const char *ptr;
		bool found_message = false;

		for (offset = 0; offset < ctx->parallel_log_count; offset++)
		{
			int idx = (ctx->parallel_log_index + offset) % ctx->parallel_log_count;
			Size nbytes = 0;
			void *data = NULL;
			shm_mq_result res;

			if (ctx->parallel_worker_done[idx])
				continue;

			res = shm_mq_receive(ctx->parallel_tuple_handles[idx],
								 &nbytes,
								 &data,
								 true);
			if (res == SHM_MQ_SUCCESS)
			{
				memcpy(&hdr, data, sizeof(hdr));
				ptr = (const char *) data + sizeof(hdr);
				MemoryContextReset(ctx->parallel_rowctx);
				oldctx = MemoryContextSwitchTo(ctx->parallel_rowctx);
				emit->kind = FB_APPLY_EMIT_TUPLE;
				emit->slot = NULL;
				emit->tuple = palloc0(HEAPTUPLESIZE + hdr.tuple_len);
				emit->tuple->t_len = hdr.tuple_len;
				emit->tuple->t_data = (HeapTupleHeader) ((char *) emit->tuple + HEAPTUPLESIZE);
				ItemPointerSetInvalid(&emit->tuple->t_self);
				emit->tuple->t_tableOid = InvalidOid;
				memcpy(emit->tuple->t_data, ptr, hdr.tuple_len);
				MemoryContextSwitchTo(oldctx);
				ctx->parallel_log_index = (idx + 1) % ctx->parallel_log_count;
				return true;
			}
			if (res == SHM_MQ_DETACHED)
			{
				fb_apply_parallel_wait_worker(ctx->parallel_handles[idx],
											  &ctx->parallel_tasks[idx]);
				ctx->parallel_worker_done[idx] = true;
				ctx->parallel_active_workers--;
				continue;
			}
			found_message = true;
		}

		if (found_message)
		{
			int idx;
			Size nbytes = 0;
			void *data = NULL;
			shm_mq_result res = SHM_MQ_WOULD_BLOCK;

			for (idx = 0; idx < ctx->parallel_log_count; idx++)
			{
				int probe = (ctx->parallel_log_index + idx) % ctx->parallel_log_count;

				if (ctx->parallel_worker_done[probe])
					continue;
				res = shm_mq_receive(ctx->parallel_tuple_handles[probe],
									 &nbytes,
									 &data,
									 false);
				if (res == SHM_MQ_SUCCESS)
				{
					memcpy(&hdr, data, sizeof(hdr));
					ptr = (const char *) data + sizeof(hdr);
					MemoryContextReset(ctx->parallel_rowctx);
					oldctx = MemoryContextSwitchTo(ctx->parallel_rowctx);
					emit->kind = FB_APPLY_EMIT_TUPLE;
					emit->slot = NULL;
					emit->tuple = palloc0(HEAPTUPLESIZE + hdr.tuple_len);
					emit->tuple->t_len = hdr.tuple_len;
					emit->tuple->t_data = (HeapTupleHeader) ((char *) emit->tuple + HEAPTUPLESIZE);
					ItemPointerSetInvalid(&emit->tuple->t_self);
					emit->tuple->t_tableOid = InvalidOid;
					memcpy(emit->tuple->t_data, ptr, hdr.tuple_len);
					MemoryContextSwitchTo(oldctx);
					ctx->parallel_log_index = (probe + 1) % ctx->parallel_log_count;
					return true;
				}
				if (res == SHM_MQ_DETACHED)
				{
					fb_apply_parallel_wait_worker(ctx->parallel_handles[probe],
												  &ctx->parallel_tasks[probe]);
					ctx->parallel_worker_done[probe] = true;
					ctx->parallel_active_workers--;
				}
				break;
			}
		}
	}

	if (!ctx->parallel_seen_merged)
	{
		int i;

		for (i = 0; i < ctx->parallel_log_count; i++)
			fb_apply_parallel_merge_seen(ctx,
										 ctx->parallel_tasks[i].seen_path,
										 ctx->parallel_tasks[i].seen_count);
		ctx->parallel_seen_merged = true;
	}

	return false;
}

static bool
fb_apply_parallel_materialize(FbApplyContext *ctx,
							  const FbReverseOpSource *source)
{
	int worker_count;
	char *snapshot_name;
	Size tasks_size;
	Size reverse_shared_size;
	Size pscan_size;
	Size tuple_queue_stride;
	Size tuple_queue_bytes;
	Size shared_size;
	dsm_segment *seg = NULL;
	dsm_handle seg_handle;
	FbApplyParallelShared *shared;
	FbApplyParallelTask *tasks;
	const char *session_dir;
	BackgroundWorkerHandle *handles[FB_APPLY_PARALLEL_MAX_WORKERS];
	int launched = 0;
	int i;

	if (ctx == NULL || source == NULL || ctx->rel == NULL)
		return false;
	if (!fb_apply_parallel_supported(ctx->info, ctx->fast_path, ctx->rel))
		return false;

	worker_count = fb_apply_parallel_target_workers();
	if (worker_count <= 1)
		return false;

	fb_reverse_source_materialize((FbReverseOpSource *) source);
	if (ctx->parallel_spool == NULL)
		ctx->parallel_spool = fb_spool_session_create();
	session_dir = fb_spool_session_dir(ctx->parallel_spool);
	snapshot_name = ExportSnapshot(GetActiveSnapshot());

	tasks_size = MAXALIGN(sizeof(FbApplyParallelTask) * worker_count);
	reverse_shared_size = MAXALIGN(fb_reverse_source_shared_size(source));
	pscan_size = MAXALIGN(table_parallelscan_estimate(ctx->rel, GetActiveSnapshot()));
	tuple_queue_stride = MAXALIGN(Max((Size) FB_APPLY_PARALLEL_TUPLE_QUEUE_SIZE,
									  shm_mq_minimum_size));
	tuple_queue_bytes = tuple_queue_stride * worker_count;
	shared_size = MAXALIGN(sizeof(FbApplyParallelShared)) +
		tasks_size + reverse_shared_size + pscan_size + tuple_queue_bytes;

	seg = dsm_create(shared_size, 0);
	shared = (FbApplyParallelShared *) dsm_segment_address(seg);
	MemSet(shared, 0, shared_size);
	shared->task_count = worker_count;
	shared->worker_count = worker_count;
	shared->tasks_offset = MAXALIGN(sizeof(FbApplyParallelShared));
	shared->reverse_shared_offset = shared->tasks_offset + tasks_size;
	shared->reverse_shared_size = reverse_shared_size;
	shared->pscan_offset = shared->reverse_shared_offset + reverse_shared_size;
	shared->pscan_size = pscan_size;
	shared->tuple_queue_offset = shared->pscan_offset + pscan_size;
	shared->tuple_queue_stride = tuple_queue_stride;
	tasks = fb_apply_parallel_tasks(shared);
	fb_reverse_source_write_shared(source,
								   (char *) shared + shared->reverse_shared_offset,
								   reverse_shared_size);
	table_parallelscan_initialize(ctx->rel,
								  fb_apply_parallel_pscan(shared),
								  GetActiveSnapshot());

	for (i = 0; i < worker_count; i++)
	{
		char worker_name[BGW_MAXLEN];
		char *seen_path;
		shm_mq *tuple_queue;

		seen_path = psprintf("%s/apply-seen-%d.log", session_dir, i);
		fb_apply_parallel_fill_task(&tasks[i],
									ctx->info->relid,
									snapshot_name,
									NULL,
									seen_path,
									i,
									worker_count);
		pfree(seen_path);
		tuple_queue = fb_apply_parallel_tuple_queue(shared, i);
		shm_mq_create(tuple_queue, tuple_queue_stride);
		shm_mq_set_receiver(tuple_queue, MyProc);
		snprintf(worker_name, sizeof(worker_name), "pg_flashback apply %d", i);
		seg_handle = dsm_segment_handle(seg);
		if (!fb_apply_parallel_launch_worker(seg_handle,
											 i,
											 worker_name,
											 &handles[i]))
			break;
		launched++;
	}

	if (launched != worker_count)
	{
		for (i = 0; i < launched; i++)
			TerminateBackgroundWorker(handles[i]);
		for (i = 0; i < launched; i++)
			fb_apply_parallel_wait_worker(handles[i], &tasks[i]);
		dsm_detach(seg);
		return false;
	}
	ctx->parallel_seg = seg;
	ctx->parallel_shared = shared;
	ctx->parallel_tasks = tasks;
	ctx->parallel_handles = palloc0(sizeof(BackgroundWorkerHandle *) * launched);
	ctx->parallel_tuple_handles = palloc0(sizeof(shm_mq_handle *) * launched);
	ctx->parallel_worker_done = palloc0(sizeof(bool) * launched);
	ctx->parallel_log_count = launched;
	ctx->parallel_log_index = 0;
	ctx->parallel_active_workers = launched;
	ctx->parallel_seen_merged = false;
	ctx->parallel_rowctx = AllocSetContextCreate(CurrentMemoryContext,
												 "fb apply parallel row",
												 ALLOCSET_SMALL_SIZES);
	ctx->parallel_materialized = true;

	for (i = 0; i < launched; i++)
	{
		shm_mq *tuple_queue = fb_apply_parallel_tuple_queue(shared, i);

		ctx->parallel_handles[i] = handles[i];
		ctx->parallel_tuple_handles[i] = shm_mq_attach(tuple_queue, seg, handles[i]);
		shm_mq_set_handle(ctx->parallel_tuple_handles[i], handles[i]);
	}

	return true;
}

static void
fb_apply_cleanup_scan_resources(FbApplyContext *ctx)
{
	if (ctx == NULL)
		return;

	if (ctx->slot != NULL)
	{
		/*
		 * Keep the apply slot alive until fb_apply_end(). The same slot is used
		 * to return residual tuples after scan resources are torn down.
		 */
		ExecClearTuple(ctx->slot);
	}

	if (ctx->fast_state != NULL)
	{
		fb_apply_reset_buffered_emit(&ctx->fast_state->current_candidate);
		fb_apply_reset_buffered_emit(&ctx->fast_state->residual_candidate);

		if (ctx->fast_state->index_scan != NULL)
		{
			index_endscan(ctx->fast_state->index_scan);
			ctx->fast_state->index_scan = NULL;
		}

		if (ctx->fast_state->index_rel != NULL)
		{
			index_close(ctx->fast_state->index_rel, AccessShareLock);
			ctx->fast_state->index_rel = NULL;
		}
	}

	if (ctx->scan != NULL)
	{
		table_endscan(ctx->scan);
		ctx->scan = NULL;
	}

	if (ctx->rel != NULL)
	{
		relation_close(ctx->rel, AccessShareLock);
		ctx->rel = NULL;
	}

	if (ctx->pushed_snapshot)
	{
		PopActiveSnapshot();
		ctx->pushed_snapshot = false;
	}
}

static void
fb_apply_cleanup_resources(FbApplyContext *ctx)
{
	if (ctx == NULL)
		return;

	fb_apply_cleanup_scan_resources(ctx);

	if (ctx->parallel_handles != NULL)
	{
		int i;

		for (i = 0; i < ctx->parallel_log_count; i++)
		{
			if (ctx->parallel_tuple_handles != NULL &&
				ctx->parallel_tuple_handles[i] != NULL)
			{
				shm_mq_detach(ctx->parallel_tuple_handles[i]);
				ctx->parallel_tuple_handles[i] = NULL;
			}
		}
		for (i = 0; i < ctx->parallel_log_count; i++)
		{
			if (ctx->parallel_handles[i] != NULL &&
				!ctx->parallel_worker_done[i])
				WaitForBackgroundWorkerShutdown(ctx->parallel_handles[i]);
		}
		pfree(ctx->parallel_handles);
		ctx->parallel_handles = NULL;
	}
	if (ctx->parallel_tuple_handles != NULL)
	{
		pfree(ctx->parallel_tuple_handles);
		ctx->parallel_tuple_handles = NULL;
	}
	if (ctx->parallel_worker_done != NULL)
	{
		pfree(ctx->parallel_worker_done);
		ctx->parallel_worker_done = NULL;
	}
	if (ctx->parallel_seg != NULL)
	{
		dsm_detach(ctx->parallel_seg);
		ctx->parallel_seg = NULL;
	}
	ctx->parallel_shared = NULL;
	ctx->parallel_tasks = NULL;
	if (ctx->parallel_rowctx != NULL)
	{
		MemoryContextDelete(ctx->parallel_rowctx);
		ctx->parallel_rowctx = NULL;
	}
	if (ctx->parallel_spool != NULL)
	{
		fb_spool_session_destroy(ctx->parallel_spool);
		ctx->parallel_spool = NULL;
	}
	ctx->parallel_materialized = false;
	ctx->parallel_seen_merged = false;
	ctx->parallel_active_workers = 0;
	ctx->parallel_log_count = 0;
	ctx->parallel_log_index = 0;
}

static bool
fb_apply_process_current_emit(FbApplyContext *ctx,
							  TupleTableSlot *slot,
							  FbApplyEmit *emit)
{
	if (ctx->info->mode == FB_APPLY_KEYED)
		return fb_keyed_apply_process_current_emit(ctx->mode_state, slot, emit);

	return fb_bag_apply_process_current_emit(ctx->mode_state, slot, emit);
}

static bool
fb_apply_next_residual_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	if (ctx->info->mode == FB_APPLY_KEYED)
		return fb_keyed_apply_next_residual_emit(ctx->mode_state, emit);

	return fb_bag_apply_next_residual_emit(ctx->mode_state, emit);
}

static bool
fb_apply_fast_path_enabled(FbApplyContext *ctx)
{
	return ctx != NULL &&
		ctx->fast_state != NULL &&
		ctx->fast_state->spec.mode != FB_FAST_PATH_NONE;
}

static bool
fb_apply_fast_path_supported(const FbRelationInfo *info,
							   const FbFastPathSpec *fast_path)
{
	return info != NULL &&
		fast_path != NULL &&
		info->mode == FB_APPLY_KEYED &&
		fast_path->mode != FB_FAST_PATH_NONE &&
		fast_path->key_attnum == info->key_attnums[0] &&
		info->key_natts == 1 &&
		OidIsValid(info->key_index_oid);
}

static void
fb_apply_fast_path_init(FbApplyContext *ctx)
{
	FbApplyFastPathState *fast;
	TypeCacheEntry *typentry;
	uint64 residual_count = 0;

	if (!fb_apply_fast_path_supported(ctx->info, ctx->fast_path))
		return;

	fast = palloc0(sizeof(*fast));
	fb_apply_copy_fast_path_spec(&fast->spec, ctx->fast_path);
	fast->index_rel = index_open(ctx->info->key_index_oid, AccessShareLock);
	fast->key_cursor = 0;

	get_typlenbyval(fast->spec.key_type_oid, &fast->key_typlen, &fast->key_typbyval);
	typentry = lookup_type_cache(fast->spec.key_type_oid, TYPECACHE_CMP_PROC_FINFO);
	if (typentry != NULL && typentry->cmp_proc != InvalidOid)
	{
		fast->cmp_fmgr = typentry->cmp_proc_finfo;
		fast->has_cmp = true;
	}

	if (fast->spec.ordered_output && fast->spec.mode != FB_FAST_PATH_KEY_EQ)
		fb_apply_sort_fast_keys(fast);
	if (fast->spec.mode == FB_FAST_PATH_KEY_TOPN ||
		fast->spec.mode == FB_FAST_PATH_KEY_RANGE)
	{
		int key_count = 0;

		if (!fast->has_cmp)
		{
			index_close(fast->index_rel, AccessShareLock);
			pfree(fast);
			return;
		}

		fast->residual_items = fb_keyed_apply_collect_residual_items(ctx->mode_state,
																	 &residual_count);
		fast->residual_count = residual_count;
		if (fast->spec.ordered_output)
			fb_apply_sort_residual_items(fast);
		fast->index_scan = index_beginscan(ctx->rel,
										   fast->index_rel,
										   GetActiveSnapshot(),
										   NULL,
										   (fast->spec.mode == FB_FAST_PATH_KEY_RANGE) ? 2 : 0,
										   0);
		if (fast->spec.mode == FB_FAST_PATH_KEY_RANGE)
		{
			Oid strategy_op;
			RegProcedure strategy_proc;

			if (fast->spec.has_lower_bound)
			{
				strategy_op = get_opfamily_member(
					fast->index_rel->rd_opfamily[0],
					fast->index_rel->rd_opcintype[0],
					fast->index_rel->rd_opcintype[0],
					fast->spec.lower_inclusive ?
					BTGreaterEqualStrategyNumber :
					BTGreaterStrategyNumber);
				if (!OidIsValid(strategy_op))
					elog(ERROR, "fb apply missing lower bound operator for range fast path");
				strategy_proc = get_opcode(strategy_op);
				if (!OidIsValid(strategy_proc))
					elog(ERROR, "fb apply missing lower bound procedure for range fast path");

				ScanKeyInit(&fast->scankeys[key_count],
							1,
							fast->spec.lower_inclusive ?
							BTGreaterEqualStrategyNumber :
							BTGreaterStrategyNumber,
							strategy_proc,
							fast->spec.lower_value);
				key_count++;
			}

			if (fast->spec.has_upper_bound)
			{
				strategy_op = get_opfamily_member(
					fast->index_rel->rd_opfamily[0],
					fast->index_rel->rd_opcintype[0],
					fast->index_rel->rd_opcintype[0],
					fast->spec.upper_inclusive ?
					BTLessEqualStrategyNumber :
					BTLessStrategyNumber);
				if (!OidIsValid(strategy_op))
					elog(ERROR, "fb apply missing upper bound operator for range fast path");
				strategy_proc = get_opcode(strategy_op);
				if (!OidIsValid(strategy_proc))
					elog(ERROR, "fb apply missing upper bound procedure for range fast path");

				ScanKeyInit(&fast->scankeys[key_count],
							1,
							fast->spec.upper_inclusive ?
							BTLessEqualStrategyNumber :
							BTLessStrategyNumber,
							strategy_proc,
							fast->spec.upper_value);
				key_count++;
			}

			fast->scankey_count = key_count;
			fast->scankeys_ready = (key_count > 0);
		}
		/*
		 * Even a no-key ordered btree scan must be explicitly rescanned once
		 * after index_beginscan(); otherwise the AM can reach index_getnext*
		 * with uninitialized scan state and crash in _bt_start_array_keys().
		 */
		index_rescan(fast->index_scan,
					 fast->scankeys_ready ? fast->scankeys : NULL,
					 fast->scankeys_ready ? fast->scankey_count : 0,
					 NULL,
					 0);
	}
	else
	{
		Oid eqop;
		RegProcedure eqproc;

		eqop = get_opfamily_member(fast->index_rel->rd_opfamily[0],
								   fast->index_rel->rd_opcintype[0],
								   fast->index_rel->rd_opcintype[0],
								   BTEqualStrategyNumber);
		if (!OidIsValid(eqop))
		{
			index_close(fast->index_rel, AccessShareLock);
			pfree(fast);
			return;
		}
		eqproc = get_opcode(eqop);
		if (!OidIsValid(eqproc))
		{
			index_close(fast->index_rel, AccessShareLock);
			pfree(fast);
			return;
		}

		ScanKeyInit(&fast->scankeys[0],
					1,
					BTEqualStrategyNumber,
					eqproc,
					(Datum) 0);
		fast->index_scan = index_beginscan(ctx->rel,
										   fast->index_rel,
										   GetActiveSnapshot(),
										   NULL,
										   1,
										   0);
		fast->scankey_count = 1;
		fast->scankeys_ready = true;
	}

	ctx->fast_state = fast;
}

static Datum
fb_apply_emit_as_datum(FbApplyContext *ctx, const FbApplyEmit *emit)
{
	switch (emit->kind)
	{
		case FB_APPLY_EMIT_SLOT:
			return ExecFetchSlotHeapTupleDatum(emit->slot);
		case FB_APPLY_EMIT_TUPLE:
			return heap_copy_tuple_as_datum(emit->tuple, ctx->tupdesc);
		case FB_APPLY_EMIT_NONE:
			break;
	}

	elog(ERROR, "fb apply emitted invalid row kind");
	return (Datum) 0;
}

static void
fb_apply_emit_to_slot(FbApplyContext *ctx,
					  FbApplyEmit *emit,
					  TupleTableSlot *slot)
{
	if (ctx == NULL || emit == NULL || slot == NULL)
		elog(ERROR, "fb apply could not emit into NULL slot");

	ExecClearTuple(slot);

	switch (emit->kind)
	{
		case FB_APPLY_EMIT_SLOT:
			if (emit->slot == slot)
				return;
			ExecCopySlot(slot, emit->slot);
			return;
		case FB_APPLY_EMIT_TUPLE:
			ExecForceStoreHeapTuple(emit->tuple, slot, emit->owned_tuple);
			emit->owned_tuple = false;
			slot->tts_tableOid = ctx->info->relid;
			return;
		case FB_APPLY_EMIT_NONE:
			break;
	}

	elog(ERROR, "fb apply emitted invalid row kind");
}

static void
fb_apply_release_emit(FbApplyContext *ctx, const FbApplyEmit *emit)
{
	if (ctx == NULL || emit == NULL)
		return;

	if (emit->owned_tuple && emit->tuple != NULL)
		heap_freetuple(emit->tuple);

	if (emit->kind == FB_APPLY_EMIT_SLOT &&
		emit->slot != NULL &&
		emit->slot == ctx->slot &&
		!ctx->slot_is_bound_output)
		ExecClearTuple(ctx->slot);
}

static bool
fb_apply_fast_lookup_next_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	FbApplyFastPathState *fast = ctx->fast_state;

	while (fast->key_cursor < fast->spec.key_count)
	{
		Datum key_value = fast->spec.key_values[fast->key_cursor];
		bool key_isnull = fast->spec.key_nulls[fast->key_cursor];
		bool found_current = false;

		fast->key_cursor++;
		fb_progress_update_fraction(FB_PROGRESS_STAGE_APPLY,
									fast->key_cursor,
									fast->spec.key_count,
									NULL);
		if (key_isnull)
			continue;

		fast->scankeys[0].sk_argument = key_value;
		index_rescan(fast->index_scan, &fast->scankeys[0], 1, NULL, 0);
		found_current = index_getnext_slot(fast->index_scan,
										   ForwardScanDirection,
										   ctx->slot);
		if (found_current)
		{
			if (fb_apply_process_current_emit(ctx, ctx->slot, emit))
				return true;
			ExecClearTuple(ctx->slot);
		}

		if (fb_keyed_apply_emit_missing_key(ctx->mode_state, key_value, key_isnull, emit))
			return true;
	}

	return false;
}

static bool
fb_apply_fast_fill_current_candidate(FbApplyContext *ctx)
{
	FbApplyFastPathState *fast = ctx->fast_state;
	ScanDirection dir = fast->spec.order_asc ? ForwardScanDirection : BackwardScanDirection;

	while (!fast->current_candidate.ready &&
		   index_getnext_slot(fast->index_scan, dir, ctx->slot))
	{
		FbApplyEmit emit = {0};

		if (fb_apply_process_current_emit(ctx, ctx->slot, &emit))
		{
			fb_apply_buffer_emit(ctx, &emit, NULL, &fast->current_candidate);
			return true;
		}
		ExecClearTuple(ctx->slot);
	}

	return fast->current_candidate.ready;
}

static bool
fb_apply_fast_fill_residual_candidate(FbApplyContext *ctx)
{
	FbApplyFastPathState *fast = ctx->fast_state;

	while (!fast->residual_candidate.ready &&
		   fast->residual_cursor < fast->residual_count)
	{
		FbKeyedResidualItem *item = &fast->residual_items[fast->residual_cursor++];

		if (!fb_keyed_apply_residual_item_ready(item))
			continue;
		if (fast->spec.mode == FB_FAST_PATH_KEY_RANGE &&
			!fb_apply_fast_key_in_range(fast, item->key_value, item->key_isnull))
			continue;

		fast->residual_candidate.ready = true;
		fast->residual_candidate.emit.kind = FB_APPLY_EMIT_TUPLE;
		fast->residual_candidate.emit.slot = NULL;
		fast->residual_candidate.emit.tuple = item->tuple;
		fast->residual_candidate.key_value = item->key_value;
		fast->residual_candidate.key_isnull = item->key_isnull;
		fast->residual_candidate.residual_item = item;
		return true;
	}

	return fast->residual_candidate.ready;
}

static bool
fb_apply_fast_topn_next_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	FbApplyFastPathState *fast = ctx->fast_state;
	int cmp = 0;
	bool choose_current = false;

	if (fast->emitted_count >= fast->spec.limit_count)
		return false;

	if (fast->residual_candidate.ready &&
		!fb_keyed_apply_residual_item_ready(fast->residual_candidate.residual_item))
		fast->residual_candidate.ready = false;

	fb_apply_fast_fill_current_candidate(ctx);
	fb_apply_fast_fill_residual_candidate(ctx);

	if (!fast->current_candidate.ready && !fast->residual_candidate.ready)
		return false;
	if (fast->current_candidate.ready && !fast->residual_candidate.ready)
		choose_current = true;
	else if (!fast->current_candidate.ready && fast->residual_candidate.ready)
		choose_current = false;
	else
	{
		cmp = fb_apply_compare_key_values(fast,
										  fast->current_candidate.key_value,
										  fast->current_candidate.key_isnull,
										  fast->residual_candidate.key_value,
										  fast->residual_candidate.key_isnull);
		if (!fast->spec.order_asc)
			cmp = -cmp;
		choose_current = (cmp <= 0);
	}

	if (choose_current)
	{
		fb_apply_take_buffered_emit(&fast->current_candidate, emit);
	}
	else
	{
		FbKeyedResidualItem *item = fast->residual_candidate.residual_item;

		fb_apply_take_buffered_emit(&fast->residual_candidate, emit);
		fb_keyed_apply_residual_item_mark_emitted(item);
	}

	fast->emitted_count++;
	fb_progress_update_fraction(FB_PROGRESS_STAGE_APPLY,
								fast->emitted_count,
								fast->spec.limit_count,
								NULL);
	return true;
}

static bool
fb_apply_fast_range_next_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	FbApplyFastPathState *fast = ctx->fast_state;
	ScanDirection dir = (!fast->spec.ordered_output || fast->spec.order_asc) ?
		ForwardScanDirection : BackwardScanDirection;

	if (fast->spec.ordered_output)
	{
		int cmp = 0;
		bool choose_current = false;

		if (fast->spec.limit_count > 0 &&
			fast->emitted_count >= fast->spec.limit_count)
			return false;

		if (fast->residual_candidate.ready &&
			!fb_keyed_apply_residual_item_ready(fast->residual_candidate.residual_item))
			fast->residual_candidate.ready = false;

		fb_apply_fast_fill_current_candidate(ctx);
		fb_apply_fast_fill_residual_candidate(ctx);

		if (!fast->current_candidate.ready && !fast->residual_candidate.ready)
			return false;
		if (fast->current_candidate.ready && !fast->residual_candidate.ready)
			choose_current = true;
		else if (!fast->current_candidate.ready && fast->residual_candidate.ready)
			choose_current = false;
		else
		{
			cmp = fb_apply_compare_key_values(fast,
											  fast->current_candidate.key_value,
											  fast->current_candidate.key_isnull,
											  fast->residual_candidate.key_value,
											  fast->residual_candidate.key_isnull);
			if (!fast->spec.order_asc)
				cmp = -cmp;
			choose_current = (cmp <= 0);
		}

		if (choose_current)
		{
			fb_apply_take_buffered_emit(&fast->current_candidate, emit);
		}
		else
		{
			FbKeyedResidualItem *item = fast->residual_candidate.residual_item;

			fb_apply_take_buffered_emit(&fast->residual_candidate, emit);
			fb_keyed_apply_residual_item_mark_emitted(item);
		}

		fast->emitted_count++;
		if (fast->spec.limit_count > 0)
			fb_progress_update_fraction(FB_PROGRESS_STAGE_APPLY,
										fast->emitted_count,
										fast->spec.limit_count,
										NULL);
		return true;
	}

	while (index_getnext_slot(fast->index_scan, dir, ctx->slot))
	{
		if (fb_apply_process_current_emit(ctx, ctx->slot, emit))
			return true;
		ExecClearTuple(ctx->slot);
	}

	while (fast->residual_cursor < fast->residual_count)
	{
		FbKeyedResidualItem *item = &fast->residual_items[fast->residual_cursor++];

		if (!fb_keyed_apply_residual_item_ready(item))
			continue;
		if (!fb_apply_fast_key_in_range(fast, item->key_value, item->key_isnull))
			continue;

		emit->kind = FB_APPLY_EMIT_TUPLE;
		emit->slot = NULL;
		emit->tuple = item->tuple;
		fb_keyed_apply_residual_item_mark_emitted(item);
		return true;
	}

	return false;
}

static bool
fb_apply_next_fast_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	bool emitted = false;

	if (!fb_apply_fast_path_enabled(ctx))
		return false;

	memset(emit, 0, sizeof(*emit));
	switch (ctx->fast_state->spec.mode)
	{
		case FB_FAST_PATH_KEY_EQ:
		case FB_FAST_PATH_KEY_IN:
			emitted = fb_apply_fast_lookup_next_emit(ctx, emit);
			break;
		case FB_FAST_PATH_KEY_RANGE:
			emitted = fb_apply_fast_range_next_emit(ctx, emit);
			break;
		case FB_FAST_PATH_KEY_TOPN:
			emitted = fb_apply_fast_topn_next_emit(ctx, emit);
			break;
		case FB_FAST_PATH_NONE:
			break;
	}

	if (emitted)
		return true;

	ctx->phase = FB_APPLY_PHASE_DONE;
	fb_apply_cleanup_resources(ctx);
	fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 100, NULL);
	fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
	fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
	return false;
}

/*
 * fb_apply_finish_scan_phase
 *    Apply helper.
 */

static void
fb_apply_finish_scan_phase(FbApplyContext *ctx)
{
	if (ctx->phase != FB_APPLY_PHASE_SCAN)
		return;

	if (ctx->info->mode == FB_APPLY_KEYED)
	{
		fb_keyed_apply_finish_scan(ctx->mode_state);
		ctx->residual_total = fb_keyed_apply_residual_total(ctx->mode_state);
	}
	else
	{
		fb_bag_apply_finish_scan(ctx->mode_state);
		ctx->residual_total = fb_bag_apply_residual_total(ctx->mode_state);
	}

	ctx->phase = FB_APPLY_PHASE_RESIDUAL;
	fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 100, NULL);
	fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
	if (ctx->residual_total == 0)
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
}

static bool
fb_apply_next_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	if (ctx == NULL || emit == NULL)
		return false;

	if (ctx->count_only_mode)
		return fb_apply_next_count_only_emit(ctx, emit);

	if (fb_apply_fast_path_enabled(ctx))
		return fb_apply_next_fast_emit(ctx, emit);

	if (ctx->phase == FB_APPLY_PHASE_SCAN)
	{
		if (ctx->parallel_materialized)
		{
			memset(emit, 0, sizeof(*emit));
			if (fb_apply_read_parallel_emit(ctx, emit))
				return true;

			fb_apply_cleanup_resources(ctx);
			fb_apply_finish_scan_phase(ctx);
		}

		while (ctx->phase == FB_APPLY_PHASE_SCAN &&
			   table_scan_getnextslot(ctx->scan, ForwardScanDirection, ctx->slot))
		{
			bool emitted;

			memset(emit, 0, sizeof(*emit));
			ctx->scanned_rows++;
			if (ctx->estimated_rows > 0 &&
				(ctx->scanned_rows == 1 ||
				 ctx->scanned_rows == ctx->estimated_rows ||
				 (ctx->scanned_rows % ctx->progress_stride) == 0))
				fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
										   fb_progress_map_subrange(0, 100,
																ctx->scanned_rows,
																ctx->estimated_rows),
										   NULL);

			emitted = fb_apply_process_current_emit(ctx, ctx->slot, emit);
			if (!emitted)
				ExecClearTuple(ctx->slot);
			if (emitted)
				return true;
		}

		fb_apply_cleanup_resources(ctx);
		fb_apply_finish_scan_phase(ctx);
	}

	if (ctx->phase == FB_APPLY_PHASE_RESIDUAL)
	{
		bool emitted;

		memset(emit, 0, sizeof(*emit));
		emitted = fb_apply_next_residual_emit(ctx, emit);

		if (!emitted)
		{
			ctx->phase = FB_APPLY_PHASE_DONE;
			fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
			return false;
		}

		ctx->residual_emitted++;
		fb_progress_update_fraction(FB_PROGRESS_STAGE_MATERIALIZE,
									ctx->residual_emitted,
									ctx->residual_total,
									NULL);
		return true;
	}

	return false;
}

/*
 * fb_apply_begin
 *    Apply entry point.
 */

FbApplyContext *
fb_apply_begin(const FbRelationInfo *info,
			   TupleDesc tupdesc,
			   const FbReverseOpSource *source,
			   const FbFastPathSpec *fast_path)
{
	FbApplyContext *ctx;
	Snapshot snapshot;
	bool parallel_candidate = false;

	ctx = palloc0(sizeof(*ctx));
	ctx->info = info;
	ctx->fast_path = fast_path;
	ctx->tupdesc = tupdesc;
	ctx->phase = FB_APPLY_PHASE_SCAN;
	ctx->rel = relation_open(info->relid, AccessShareLock);
	ctx->estimated_rows = (ctx->rel->rd_rel->reltuples > 0) ?
		(uint64) ctx->rel->rd_rel->reltuples : 0;
	ctx->progress_stride = (ctx->estimated_rows > 0) ?
		Max((uint64) 1, ctx->estimated_rows / 1024) : 1;

	snapshot = GetActiveSnapshot();
	if (snapshot == NULL)
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		snapshot = GetActiveSnapshot();
		ctx->pushed_snapshot = true;
	}

	fb_progress_enter_stage(FB_PROGRESS_STAGE_APPLY, NULL);
	parallel_candidate = fb_apply_parallel_supported(info, fast_path, ctx->rel);

	if (info->mode == FB_APPLY_KEYED && parallel_candidate)
	{
		FbReverseOpReader *reader;

		reader = fb_reverse_reader_open(source);
		ctx->mode_state = fb_keyed_apply_begin_reader_ex(info,
														 tupdesc,
														 reader,
														 (long) Min(fb_reverse_source_total_count(source),
																 (uint64) LONG_MAX),
														 fb_reverse_source_tracked_bytes(source),
														 fb_reverse_source_memory_limit_bytes(source),
														 -1,
														 0,
														 false);
		fb_reverse_reader_close(reader);
	}
	else if (info->mode == FB_APPLY_KEYED)
		ctx->mode_state = fb_keyed_apply_begin(info, tupdesc, source);
	else
		ctx->mode_state = fb_bag_apply_begin(info, tupdesc, source);

	if (parallel_candidate &&
		fb_keyed_apply_supports_parallel_single_typed_key(ctx->mode_state) &&
		fb_apply_parallel_materialize(ctx, source))
	{
		fb_apply_cleanup_scan_resources(ctx);
		return ctx;
	}

	ctx->scan = table_beginscan_strat(ctx->rel, snapshot, 0, NULL, true, false);
	ctx->slot = table_slot_create(ctx->rel, NULL);
	fb_apply_fast_path_init(ctx);
	return ctx;
}

FbApplyContext *
fb_apply_begin_count_only(const FbRelationInfo *info,
						  TupleDesc tupdesc,
						  uint64 row_count)
{
	FbApplyContext *ctx;

	ctx = palloc0(sizeof(*ctx));
	ctx->info = info;
	ctx->tupdesc = tupdesc;
	ctx->phase = FB_APPLY_PHASE_SCAN;
	ctx->count_only_mode = true;
	ctx->count_only_total = row_count;
	fb_progress_enter_stage(FB_PROGRESS_STAGE_APPLY, NULL);
	if (row_count == 0)
	{
		fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 100, NULL);
		fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
		ctx->phase = FB_APPLY_PHASE_DONE;
	}
	return ctx;
}

/*
 * fb_apply_next
 *    Apply entry point.
 */

bool
fb_apply_next(FbApplyContext *ctx, Datum *result)
{
	FbApplyEmit emit = {0};

	if (ctx == NULL)
		return false;

	if (!fb_apply_next_emit(ctx, &emit))
		return false;

	*result = fb_apply_emit_as_datum(ctx, &emit);
	fb_apply_release_emit(ctx, &emit);
	return true;
}

bool
fb_apply_next_slot(FbApplyContext *ctx, TupleTableSlot *slot)
{
	FbApplyEmit emit = {0};

	if (ctx == NULL || slot == NULL)
		return false;

	if (!fb_apply_next_emit(ctx, &emit))
	{
		ExecClearTuple(slot);
		return false;
	}

	fb_apply_emit_to_slot(ctx, &emit, slot);
	fb_apply_release_emit(ctx, &emit);
	return true;
}

TupleTableSlot *
fb_apply_next_output_slot(FbApplyContext *ctx)
{
	FbApplyEmit emit = {0};
	TupleTableSlot *output_slot;

	if (ctx == NULL || ctx->slot == NULL)
		return NULL;
	output_slot = ctx->slot;

	if (!fb_apply_next_emit(ctx, &emit))
	{
		if (ctx->slot_is_bound_output && output_slot != NULL)
			return ExecClearTuple(output_slot);
		return NULL;
	}

	if (emit.kind == FB_APPLY_EMIT_SLOT &&
		(!ctx->slot_is_bound_output || emit.slot == output_slot))
		return emit.slot;

	fb_apply_emit_to_slot(ctx, &emit, output_slot);
	fb_apply_release_emit(ctx, &emit);
	return output_slot;
}

void
fb_apply_bind_output_slot(FbApplyContext *ctx, TupleTableSlot *slot)
{
	if (ctx == NULL || slot == NULL)
		return;
	if (ctx->slot == slot)
	{
		ctx->slot_is_bound_output = true;
		return;
	}
	if (ctx->slot != NULL && !ctx->slot_is_bound_output)
		ExecDropSingleTupleTableSlot(ctx->slot);
	ctx->slot = slot;
	ctx->slot_is_bound_output = true;
}

bool
fb_apply_parallel_materialized(const FbApplyContext *ctx)
{
	return (ctx != NULL && ctx->parallel_materialized);
}

int
fb_apply_parallel_log_count(const FbApplyContext *ctx)
{
	return (ctx == NULL) ? 0 : ctx->parallel_log_count;
}

/*
 * fb_apply_end
 *    Apply entry point.
 */

void
fb_apply_end(FbApplyContext *ctx)
{
	if (ctx == NULL)
		return;

	fb_apply_cleanup_resources(ctx);

	if (ctx->slot != NULL)
	{
		if (!ctx->slot_is_bound_output)
			ExecDropSingleTupleTableSlot(ctx->slot);
		else
			ExecClearTuple(ctx->slot);
		ctx->slot = NULL;
		ctx->slot_is_bound_output = false;
	}

	if (ctx->mode_state != NULL)
	{
		if (ctx->info != NULL && ctx->info->mode == FB_APPLY_KEYED)
			fb_keyed_apply_end(ctx->mode_state);
		else
			fb_bag_apply_end(ctx->mode_state);
		ctx->mode_state = NULL;
	}

	ctx->phase = FB_APPLY_PHASE_DONE;
}
