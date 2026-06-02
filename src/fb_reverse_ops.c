/*
 * fb_reverse_ops.c
 *    Reverse-op spill source, external run sorting, and readers.
 */

#include "postgres.h"

#include "utils/memutils.h"

#include "fb_memory.h"
#include "fb_progress.h"
#include "fb_reverse_ops.h"

typedef struct FbReverseRun
{
	FbSpoolLog *log;
	uint32 count;
	struct FbReverseRun *next;
} FbReverseRun;

typedef struct FbSharedReverseSourceHeader
{
	uint64 total_count;
	uint32 run_count;
} FbSharedReverseSourceHeader;

typedef struct FbSharedReverseRunDesc
{
	uint32 count;
	char path[MAXPGPATH];
} FbSharedReverseRunDesc;

typedef struct FbSerializedRowImageHeader
{
	uint32 tuple_len;
} FbSerializedRowImageHeader;

typedef struct FbSerializedReverseOpHeader
{
	FbReverseOpType type;
	TransactionId xid;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
	XLogRecPtr record_lsn;
	FbSerializedRowImageHeader old_row;
	FbSerializedRowImageHeader new_row;
} FbSerializedReverseOpHeader;

struct FbReverseOpSource
{
	FbSpoolSession *session;
	uint64 *tracked_bytes;
	uint64 tracked_bytes_local;
	uint64 memory_limit_bytes;
	uint64 run_limit_bytes;
	FbReverseOp *ops;
	uint32 count;
	uint32 capacity;
	uint64 array_bytes;
	uint64 row_bytes;
	uint64 total_count;
	uint32 run_count;
	FbReverseRun *runs_head;
	FbReverseRun *runs_tail;
};

typedef struct FbReverseRunReader
{
	FbReverseRun *run;
	FbSpoolLog *owned_log;
	FbSpoolCursor *cursor;
	MemoryContext rowctx;
	FbReverseOp current;
	bool has_current;
} FbReverseRunReader;

struct FbReverseOpReader
{
	const FbReverseOpSource *source;
	bool in_memory;
	uint32 index;
	uint32 run_count;
	FbReverseRunReader *runs;
	MemoryContext emitctx;
};

static int fb_reverse_op_cmp(const void *lhs, const void *rhs);
static void fb_reverse_source_detach_tracking(FbReverseOpSource *source);
static void fb_reverse_reader_init_run(FbReverseRunReader *reader,
										 FbReverseRun *run,
										 FbSpoolLog *owned_log);

static Size
fb_row_image_owned_bytes(const FbRowImage *row)
{
	if (row == NULL)
		return 0;

	return fb_memory_heaptuple_bytes(row->tuple);
}

static Size
fb_reverse_op_row_bytes(const FbReverseOp *op)
{
	if (op == NULL)
		return 0;

	return fb_row_image_owned_bytes(&op->old_row) +
		fb_row_image_owned_bytes(&op->new_row);
}

static void
fb_row_image_release(FbRowImage *row)
{
	if (row == NULL)
		return;

	if (row->tuple != NULL)
	{
		heap_freetuple(row->tuple);
		row->tuple = NULL;
	}
	row->finalized = false;
}

static void
fb_reverse_op_release(FbReverseOp *op)
{
	if (op == NULL)
		return;

	fb_row_image_release(&op->old_row);
	fb_row_image_release(&op->new_row);
}

static int
fb_reverse_op_cmp(const void *lhs, const void *rhs)
{
	const FbReverseOp *left = (const FbReverseOp *) lhs;
	const FbReverseOp *right = (const FbReverseOp *) rhs;

	if (left->commit_lsn < right->commit_lsn)
		return 1;
	if (left->commit_lsn > right->commit_lsn)
		return -1;
	if (left->record_lsn < right->record_lsn)
		return 1;
	if (left->record_lsn > right->record_lsn)
		return -1;
	return 0;
}

static void
fb_reverse_source_grow(FbReverseOpSource *source)
{
	uint32 old_capacity;
	Size bytes;

	if (source == NULL)
		return;

	if (source->count < source->capacity)
		return;

	old_capacity = source->capacity;
	source->capacity = (source->capacity == 0) ? 32 : source->capacity * 2;
	bytes = sizeof(FbReverseOp) * (source->capacity - old_capacity);
	fb_memory_charge_bytes(source->tracked_bytes,
						   source->memory_limit_bytes,
						   bytes,
						   "ReverseOp array");
	source->array_bytes += bytes;
	if (source->ops == NULL)
		source->ops = palloc0(sizeof(FbReverseOp) * source->capacity);
	else
	{
		source->ops = repalloc(source->ops, sizeof(FbReverseOp) * source->capacity);
		MemSet(source->ops + old_capacity, 0,
			   sizeof(FbReverseOp) * (source->capacity - old_capacity));
	}
}

static void
fb_reverse_serialize_row(StringInfo buf, const FbRowImage *row,
						   FbSerializedRowImageHeader *hdr)
{
	const char *tuple_data = NULL;

	MemSet(hdr, 0, sizeof(*hdr));
	if (row == NULL)
		return;

	if (row->tuple != NULL)
	{
		hdr->tuple_len = row->tuple->t_len;
		tuple_data = (const char *) row->tuple->t_data;
	}

	if (hdr->tuple_len > 0)
		appendBinaryStringInfo(buf, tuple_data, hdr->tuple_len);
}

static void
fb_reverse_deserialize_row(const char **ptr,
							 const FbSerializedRowImageHeader *hdr,
							 FbRowImage *row)
{
	const char *cursor = *ptr;

	MemSet(row, 0, sizeof(*row));
	if (hdr->tuple_len > 0)
	{
		row->tuple = palloc0(HEAPTUPLESIZE + hdr->tuple_len);
		row->tuple->t_len = hdr->tuple_len;
		row->tuple->t_data = (HeapTupleHeader) ((char *) row->tuple + HEAPTUPLESIZE);
		ItemPointerSetInvalid(&row->tuple->t_self);
		row->tuple->t_tableOid = InvalidOid;
		memcpy(row->tuple->t_data, cursor, hdr->tuple_len);
		cursor += hdr->tuple_len;
	}

	*ptr = cursor;
}

static void
fb_reverse_copy_row_image(const FbRowImage *src, FbRowImage *dst)
{
	MemSet(dst, 0, sizeof(*dst));
	if (src == NULL)
		return;

	if (src->tuple != NULL)
		dst->tuple = heap_copytuple(src->tuple);
	dst->finalized = src->finalized;
}

static void
fb_reverse_source_detach_tracking(FbReverseOpSource *source)
{
	if (source == NULL || source->tracked_bytes == &source->tracked_bytes_local)
		return;

	source->tracked_bytes_local = (source->tracked_bytes != NULL) ?
		*source->tracked_bytes : 0;
	source->tracked_bytes = &source->tracked_bytes_local;
}

static void
fb_reverse_run_append(FbReverseOpSource *source)
{
	FbReverseRun *run;
	StringInfoData buf;
	uint32 i;

	if (source == NULL || source->count == 0)
		return;

	qsort(source->ops, source->count, sizeof(FbReverseOp), fb_reverse_op_cmp);
	run = palloc0(sizeof(*run));
	run->log = fb_spool_log_create(source->session, "reverse-run");
	run->count = source->count;

	initStringInfo(&buf);
	for (i = 0; i < source->count; i++)
	{
		FbSerializedReverseOpHeader hdr;
		FbReverseOp *op = &source->ops[i];
		Size row_bytes = fb_reverse_op_row_bytes(op);

		MemSet(&hdr, 0, sizeof(hdr));
		hdr.type = op->type;
		hdr.xid = op->xid;
		hdr.commit_ts = op->commit_ts;
		hdr.commit_lsn = op->commit_lsn;
		hdr.record_lsn = op->record_lsn;
		resetStringInfo(&buf);
		buf.len = 0;
		appendBinaryStringInfo(&buf, (const char *) &hdr, sizeof(hdr));
		fb_reverse_serialize_row(&buf, &op->old_row, &hdr.old_row);
		fb_reverse_serialize_row(&buf, &op->new_row, &hdr.new_row);
		memcpy(buf.data, &hdr, sizeof(hdr));
		fb_spool_log_append(run->log, buf.data, buf.len);

		fb_reverse_op_release(op);
		fb_memory_release_bytes(source->tracked_bytes, row_bytes);
		if (source->row_bytes >= row_bytes)
			source->row_bytes -= row_bytes;
		else
			source->row_bytes = 0;
	}
	pfree(buf.data);

	source->count = 0;
	if (source->runs_tail == NULL)
		source->runs_head = run;
	else
		source->runs_tail->next = run;
	source->runs_tail = run;
	source->run_count++;
	if (source->array_bytes > 0)
	{
		fb_memory_release_bytes(source->tracked_bytes, source->array_bytes);
		source->array_bytes = 0;
	}
	if (source->ops != NULL)
	{
		pfree(source->ops);
		source->ops = NULL;
	}
	source->capacity = 0;
}

FbReverseOpSource *
fb_reverse_source_create(FbSpoolSession *session,
						   uint64 *tracked_bytes,
						   uint64 memory_limit_bytes)
{
	FbReverseOpSource *source;

	source = palloc0(sizeof(*source));
	source->session = session;
	source->tracked_bytes_local = (tracked_bytes != NULL) ? *tracked_bytes : 0;
	source->tracked_bytes = (tracked_bytes != NULL) ?
		tracked_bytes : &source->tracked_bytes_local;
	source->memory_limit_bytes = memory_limit_bytes;
	if (memory_limit_bytes > 0)
	{
		uint64 run_limit = memory_limit_bytes / 2;

		if (memory_limit_bytes >= ((uint64) 4 * 1024 * 1024 * 1024))
			run_limit = (memory_limit_bytes * 5) / 8;
		source->run_limit_bytes = Max((uint64) (256 * 1024), run_limit);
	}
	else
		source->run_limit_bytes = (uint64) (8 * 1024 * 1024);
	return source;
}

void
fb_reverse_source_append(FbReverseOpSource *source, const FbReverseOp *op)
{
	Size row_bytes;

	if (source == NULL || op == NULL)
		return;

	fb_reverse_source_grow(source);
	source->ops[source->count++] = *op;
	source->total_count++;
	row_bytes = fb_reverse_op_row_bytes(op);
	source->row_bytes += row_bytes;

	if (source->row_bytes + source->array_bytes > source->run_limit_bytes &&
		source->count > 0 && source->session != NULL)
		fb_reverse_run_append(source);
}

void
fb_reverse_source_finish(FbReverseOpSource *source)
{
	if (source == NULL)
		return;

	fb_progress_enter_stage(FB_PROGRESS_STAGE_BUILD_REVERSE, NULL);
	if (source->run_count == 0)
	{
		if (source->count > 1)
			qsort(source->ops, source->count, sizeof(FbReverseOp), fb_reverse_op_cmp);
		fb_reverse_source_detach_tracking(source);
		fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_REVERSE, 100, NULL);
		return;
	}

	if (source->count > 0)
		fb_reverse_run_append(source);
	fb_reverse_source_detach_tracking(source);
	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_REVERSE, 100, NULL);
}

void
fb_reverse_source_materialize(FbReverseOpSource *source)
{
	if (source == NULL || source->count == 0)
		return;
	if (source->session == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("reverse source cannot be shared without a spool session")));

	fb_reverse_run_append(source);
}

void
fb_reverse_source_destroy(FbReverseOpSource *source)
{
	uint32 i;
	FbReverseRun *run;

	if (source == NULL)
		return;

	for (i = 0; i < source->count; i++)
	{
		Size row_bytes = fb_reverse_op_row_bytes(&source->ops[i]);

		fb_reverse_op_release(&source->ops[i]);
		fb_memory_release_bytes(source->tracked_bytes, row_bytes);
	}
	if (source->array_bytes > 0)
		fb_memory_release_bytes(source->tracked_bytes, source->array_bytes);
	if (source->ops != NULL)
		pfree(source->ops);
	run = source->runs_head;
	while (run != NULL)
	{
		FbReverseRun *next = run->next;

		pfree(run);
		run = next;
	}
	pfree(source);
}

uint64
fb_reverse_source_tracked_bytes(const FbReverseOpSource *source)
{
	if (source == NULL || source->tracked_bytes == NULL)
		return 0;

	return *source->tracked_bytes;
}

uint64
fb_reverse_source_memory_limit_bytes(const FbReverseOpSource *source)
{
	return (source == NULL) ? 0 : source->memory_limit_bytes;
}

uint64
fb_reverse_source_total_count(const FbReverseOpSource *source)
{
	return (source == NULL) ? 0 : source->total_count;
}

Size
fb_reverse_source_shared_size(const FbReverseOpSource *source)
{
	uint32 run_count = (source == NULL) ? 0 : source->run_count;

	return MAXALIGN(sizeof(FbSharedReverseSourceHeader)) +
		MAXALIGN(sizeof(FbSharedReverseRunDesc) * run_count);
}

void
fb_reverse_source_write_shared(const FbReverseOpSource *source,
								 void *dest,
								 Size dest_size)
{
	FbSharedReverseSourceHeader *hdr;
	FbSharedReverseRunDesc *runs;
	FbReverseRun *run;
	uint32 i = 0;
	Size needed;

	if (source == NULL || dest == NULL)
		return;
	if (source->count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("reverse source must be materialized before sharing")));

	needed = fb_reverse_source_shared_size(source);
	if (dest_size < needed)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("shared reverse source buffer is too small")));

	MemSet(dest, 0, needed);
	hdr = (FbSharedReverseSourceHeader *) dest;
	hdr->total_count = source->total_count;
	hdr->run_count = source->run_count;
	runs = (FbSharedReverseRunDesc *) ((char *) dest +
										 MAXALIGN(sizeof(FbSharedReverseSourceHeader)));

	for (run = source->runs_head; run != NULL; run = run->next)
	{
		const char *path = fb_spool_log_path(run->log);

		if (i >= hdr->run_count)
			elog(ERROR, "reverse source run count mismatch");
		runs[i].count = run->count;
		if (path == NULL || strlen(path) >= sizeof(runs[i].path))
			ereport(ERROR,
					(errcode(ERRCODE_NAME_TOO_LONG),
					 errmsg("reverse source spill path is too long to share")));
		strlcpy(runs[i].path, path, sizeof(runs[i].path));
		i++;
	}
}

uint64
fb_reverse_source_shared_total_count(const void *shared_data)
{
	const FbSharedReverseSourceHeader *hdr;

	if (shared_data == NULL)
		return 0;

	hdr = (const FbSharedReverseSourceHeader *) shared_data;
	return hdr->total_count;
}

static bool
fb_reverse_run_reader_advance(FbReverseRunReader *reader)
{
	StringInfoData buf;
	FbSerializedReverseOpHeader hdr;
	const char *ptr;
	MemoryContext oldctx;

	if (reader == NULL)
		return false;

	MemoryContextReset(reader->rowctx);
	oldctx = MemoryContextSwitchTo(reader->rowctx);
	initStringInfo(&buf);
	if (!fb_spool_cursor_read(reader->cursor, &buf, NULL))
	{
		MemoryContextSwitchTo(oldctx);
		reader->has_current = false;
		return false;
	}

	memcpy(&hdr, buf.data, sizeof(hdr));
	ptr = buf.data + sizeof(hdr);
	MemSet(&reader->current, 0, sizeof(reader->current));
	reader->current.type = hdr.type;
	reader->current.xid = hdr.xid;
	reader->current.commit_ts = hdr.commit_ts;
	reader->current.commit_lsn = hdr.commit_lsn;
	reader->current.record_lsn = hdr.record_lsn;
	fb_reverse_deserialize_row(&ptr, &hdr.old_row, &reader->current.old_row);
	fb_reverse_deserialize_row(&ptr, &hdr.new_row, &reader->current.new_row);
	reader->has_current = true;
	MemoryContextSwitchTo(oldctx);
	return true;
}

static void
fb_reverse_reader_init_run(FbReverseRunReader *reader,
							 FbReverseRun *run,
							 FbSpoolLog *owned_log)
{
	FbSpoolLog *log;

	reader->run = run;
	reader->owned_log = owned_log;
	log = (run != NULL) ? run->log : owned_log;
	reader->cursor = fb_spool_cursor_open(log, FB_SPOOL_FORWARD);
	reader->rowctx = AllocSetContextCreate(CurrentMemoryContext,
										   "fb reverse run row",
										   ALLOCSET_SMALL_SIZES);
	fb_reverse_run_reader_advance(reader);
}

FbReverseOpReader *
fb_reverse_reader_open(const FbReverseOpSource *source)
{
	FbReverseOpReader *reader;
	FbReverseRun *run;
	uint32 i = 0;

	if (source == NULL)
		return NULL;

	reader = palloc0(sizeof(*reader));
	reader->source = source;
	reader->in_memory = (source->run_count == 0);
	reader->emitctx = AllocSetContextCreate(CurrentMemoryContext,
											 "fb reverse emit row",
											 ALLOCSET_SMALL_SIZES);
	if (reader->in_memory)
		return reader;

	reader->run_count = source->run_count;
	reader->runs = palloc0(sizeof(FbReverseRunReader) * reader->run_count);
	for (run = source->runs_head; run != NULL; run = run->next)
	{
		fb_reverse_reader_init_run(&reader->runs[i], run, NULL);
		i++;
	}

	return reader;
}

FbReverseOpReader *
fb_reverse_reader_open_shared(const void *shared_data)
{
	const FbSharedReverseSourceHeader *hdr;
	const FbSharedReverseRunDesc *runs;
	FbReverseOpReader *reader;
	uint32 i;

	if (shared_data == NULL)
		return NULL;

	hdr = (const FbSharedReverseSourceHeader *) shared_data;
	runs = (const FbSharedReverseRunDesc *) ((const char *) shared_data +
											 MAXALIGN(sizeof(FbSharedReverseSourceHeader)));

	reader = palloc0(sizeof(*reader));
	reader->emitctx = AllocSetContextCreate(CurrentMemoryContext,
											 "fb reverse emit row",
											 ALLOCSET_SMALL_SIZES);
	reader->in_memory = false;
	reader->run_count = hdr->run_count;
	if (reader->run_count == 0)
		return reader;

	reader->runs = palloc0(sizeof(FbReverseRunReader) * reader->run_count);
	for (i = 0; i < reader->run_count; i++)
	{
		FbSpoolLog *log = fb_spool_log_open_readonly(runs[i].path, runs[i].count);

		fb_reverse_reader_init_run(&reader->runs[i], NULL, log);
	}

	return reader;
}

bool
fb_reverse_reader_next(FbReverseOpReader *reader, FbReverseOp *op)
{
	uint32 i;
	int best = -1;

	if (reader == NULL || op == NULL)
		return false;

	if (reader->in_memory)
	{
		if (reader->index >= reader->source->count)
			return false;
		*op = reader->source->ops[reader->index++];
		return true;
	}

	for (i = 0; i < reader->run_count; i++)
	{
		if (!reader->runs[i].has_current)
			continue;
		if (best < 0 ||
			fb_reverse_op_cmp(&reader->runs[i].current,
							  &reader->runs[best].current) < 0)
			best = (int) i;
	}

	if (best < 0)
		return false;

	MemoryContextReset(reader->emitctx);
	{
		MemoryContext oldctx = MemoryContextSwitchTo(reader->emitctx);

		MemSet(op, 0, sizeof(*op));
		op->type = reader->runs[best].current.type;
		op->xid = reader->runs[best].current.xid;
		op->commit_ts = reader->runs[best].current.commit_ts;
		op->commit_lsn = reader->runs[best].current.commit_lsn;
		op->record_lsn = reader->runs[best].current.record_lsn;
		fb_reverse_copy_row_image(&reader->runs[best].current.old_row, &op->old_row);
		fb_reverse_copy_row_image(&reader->runs[best].current.new_row, &op->new_row);
		MemoryContextSwitchTo(oldctx);
	}
	fb_reverse_run_reader_advance(&reader->runs[best]);
	return true;
}

void
fb_reverse_reader_close(FbReverseOpReader *reader)
{
	uint32 i;

	if (reader == NULL)
		return;

	for (i = 0; i < reader->run_count; i++)
	{
		if (reader->runs[i].cursor != NULL)
			fb_spool_cursor_close(reader->runs[i].cursor);
		if (reader->runs[i].owned_log != NULL)
			fb_spool_log_close(reader->runs[i].owned_log);
		if (reader->runs[i].rowctx != NULL)
			MemoryContextDelete(reader->runs[i].rowctx);
	}
	if (reader->emitctx != NULL)
		MemoryContextDelete(reader->emitctx);
	if (reader->runs != NULL)
		pfree(reader->runs);
	pfree(reader);
}

char *
fb_reverse_ops_debug_summary(const FbReverseOpSource *source)
{
	if (source == NULL)
		return psprintf("reverse_ops=0");

	return psprintf("reverse_ops=%llu spill_runs=%u",
					(unsigned long long) source->total_count,
					source->run_count);
}
