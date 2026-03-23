#include "postgres.h"

#include <sys/stat.h>

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "storage/fd.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "fb_guc.h"
#include "fb_wal.h"

typedef struct FbWalSegmentEntry
{
	char name[25];
	TimeLineID timeline_id;
	XLogSegNo segno;
} FbWalSegmentEntry;

typedef struct FbWalReaderPrivate
{
	TimeLineID timeline_id;
	XLogRecPtr endptr;
	bool endptr_reached;
} FbWalReaderPrivate;

typedef struct FbTouchedXidEntry
{
	TransactionId xid;
} FbTouchedXidEntry;

typedef struct FbUnsafeXidEntry
{
	TransactionId xid;
	FbWalUnsafeReason reason;
} FbUnsafeXidEntry;

typedef struct FbXidStatusEntry
{
	TransactionId xid;
	FbWalXidStatus status;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
} FbXidStatusEntry;

typedef struct FbWalScanVisitorState
{
	const FbRelationInfo *info;
	FbWalScanContext *ctx;
	HTAB *touched_xids;
	HTAB *unsafe_xids;
} FbWalScanVisitorState;

typedef struct FbWalIndexBuildState
{
	const FbRelationInfo *info;
	FbWalScanContext *ctx;
	FbWalRecordIndex *index;
	HTAB *touched_xids;
	HTAB *unsafe_xids;
} FbWalIndexBuildState;

static void fb_mark_unsafe(FbWalScanContext *ctx, FbWalUnsafeReason reason);
static TransactionId fb_record_xid(XLogReaderState *reader);
static bool fb_locator_matches_relation(const RelFileLocator *locator,
										const FbRelationInfo *info);
static bool fb_record_touches_relation(XLogReaderState *reader,
									   const FbRelationInfo *info);
static void fb_index_charge_bytes(FbWalRecordIndex *index, Size bytes,
								  const char *what);

static bool
fb_parse_timeline_id(const char *name, TimeLineID *timeline_id)
{
	unsigned int parsed = 0;

	if (timeline_id == NULL)
		return false;

	if (sscanf(name, "%8X", &parsed) != 1)
		return false;

	*timeline_id = (TimeLineID) parsed;
	return true;
}

static int
fb_segment_name_cmp(const void *lhs, const void *rhs)
{
	const FbWalSegmentEntry *left = (const FbWalSegmentEntry *) lhs;
	const FbWalSegmentEntry *right = (const FbWalSegmentEntry *) rhs;

	if (left->segno < right->segno)
		return -1;
	if (left->segno > right->segno)
		return 1;

	return strcmp(left->name, right->name);
}

static void
fb_index_charge_bytes(FbWalRecordIndex *index, Size bytes, const char *what)
{
	if (index == NULL || bytes == 0)
		return;

	if (index->memory_limit_bytes > 0 &&
		index->tracked_bytes + (uint64) bytes > index->memory_limit_bytes)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("pg_flashback memory limit exceeded while tracking %s", what),
				 errdetail("tracked=%llu bytes limit=%llu bytes requested=%zu bytes",
						   (unsigned long long) index->tracked_bytes,
						   (unsigned long long) index->memory_limit_bytes,
						   bytes)));

	index->tracked_bytes += (uint64) bytes;
}

static int
fb_open_file_in_directory(const char *directory, const char *fname)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, fname);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	return fd;
}

static int
fb_read_wal_segment_size(const char *archive_dir, const char *segment_name)
{
	PGAlignedXLogBlock buf;
	char path[MAXPGPATH];
	FILE *fp;
	size_t bytes_read;
	XLogLongPageHeader longhdr;
	int wal_seg_size;

	snprintf(path, sizeof(path), "%s/%s", archive_dir, segment_name);
	fp = AllocateFile(path, PG_BINARY_R);
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	bytes_read = fread(buf.data, 1, XLOG_BLCKSZ, fp);
	FreeFile(fp);

	if (bytes_read != XLOG_BLCKSZ)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not read WAL header from \"%s\"", path)));

	longhdr = (XLogLongPageHeader) buf.data;
	wal_seg_size = longhdr->xlp_seg_size;

	if (!IsValidWalSegSize(wal_seg_size))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid WAL segment size in \"%s\": %d", path, wal_seg_size)));

	return wal_seg_size;
}

static void
fb_collect_archive_segments(FbWalScanContext *ctx)
{
	const char *archive_dir = fb_get_archive_dir();
	DIR *dir;
	struct dirent *de;
	TimeLineID highest_tli = 0;
	bool found = false;
	FbWalSegmentEntry *segments = NULL;
	int segment_count = 0;
	int segment_capacity = 0;
	XLogRecPtr last_segment_start;
	char last_segment_path[MAXPGPATH];
	struct stat st;
	off_t last_segment_bytes;
	int i;

	dir = AllocateDir(archive_dir);
	if (dir == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not open pg_flashback.archive_dir: %s", archive_dir)));

	while ((de = ReadDir(dir, archive_dir)) != NULL)
	{
		TimeLineID timeline_id;

		if (!IsXLogFileName(de->d_name))
			continue;
		if (!fb_parse_timeline_id(de->d_name, &timeline_id))
			continue;

		if (!found || timeline_id > highest_tli)
		{
			highest_tli = timeline_id;
			found = true;
		}
	}

	FreeDir(dir);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_flashback.archive_dir contains no WAL segments: %s",
						archive_dir)));

	dir = AllocateDir(archive_dir);
	if (dir == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not open pg_flashback.archive_dir: %s", archive_dir)));

	while ((de = ReadDir(dir, archive_dir)) != NULL)
	{
		TimeLineID timeline_id;

		if (!IsXLogFileName(de->d_name))
			continue;
		if (!fb_parse_timeline_id(de->d_name, &timeline_id))
			continue;
		if (timeline_id != highest_tli)
			continue;

		if (segment_count == segment_capacity)
		{
			segment_capacity = (segment_capacity == 0) ? 16 : segment_capacity * 2;
			if (segments == NULL)
				segments = palloc(sizeof(FbWalSegmentEntry) * segment_capacity);
			else
				segments = repalloc(segments,
									sizeof(FbWalSegmentEntry) * segment_capacity);
		}

		MemSet(&segments[segment_count], 0, sizeof(FbWalSegmentEntry));
		strlcpy(segments[segment_count].name, de->d_name,
				sizeof(segments[segment_count].name));
		segments[segment_count].timeline_id = timeline_id;
		segment_count++;
	}

	FreeDir(dir);

	if (segment_count == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_flashback.archive_dir contains no WAL segments on timeline %u",
						highest_tli)));

	ctx->timeline_id = highest_tli;
	ctx->wal_seg_size = fb_read_wal_segment_size(archive_dir, segments[0].name);

	for (i = 0; i < segment_count; i++)
	{
		TimeLineID timeline_id = 0;

		XLogFromFileName(segments[i].name, &timeline_id, &segments[i].segno,
						 ctx->wal_seg_size);
	}

	/*
	 * Sorting by segno gives the actual archive order inside the selected
	 * timeline and lets us detect gaps cheaply before we spend time decoding
	 * records.
	 */
	qsort(segments, segment_count, sizeof(FbWalSegmentEntry),
		  fb_segment_name_cmp);

	for (i = 1; i < segment_count; i++)
	{
		if (segments[i].segno != segments[i - 1].segno + 1)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("WAL not complete: missing segment between %s and %s",
							segments[i - 1].name, segments[i].name)));
	}

	strlcpy(ctx->first_segment, segments[0].name, sizeof(ctx->first_segment));
	strlcpy(ctx->last_segment, segments[segment_count - 1].name,
			sizeof(ctx->last_segment));
	ctx->total_segments = segment_count;
	ctx->segments_complete = true;

	XLogSegNoOffsetToRecPtr(segments[0].segno, 0, ctx->wal_seg_size,
							ctx->start_lsn);
	XLogSegNoOffsetToRecPtr(segments[segment_count - 1].segno, 0,
							ctx->wal_seg_size, last_segment_start);

	snprintf(last_segment_path, sizeof(last_segment_path), "%s/%s",
			 archive_dir, segments[segment_count - 1].name);
	if (stat(last_segment_path, &st) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat WAL segment \"%s\": %m",
						last_segment_path)));

	last_segment_bytes = Min((off_t) ctx->wal_seg_size, st.st_size);
	if (last_segment_bytes <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL segment \"%s\" is empty", last_segment_path)));

	ctx->end_lsn = last_segment_start + (XLogRecPtr) last_segment_bytes;
	ctx->end_is_partial = (last_segment_bytes < ctx->wal_seg_size);

	if (segments != NULL)
		pfree(segments);
}

static void
fb_wal_open_segment(XLogReaderState *state, XLogSegNo next_segno,
					TimeLineID *timeline_id)
{
	char segment_name[MAXPGPATH];

	XLogFileName(segment_name, *timeline_id, next_segno, state->segcxt.ws_segsize);
	state->seg.ws_file = fb_open_file_in_directory(state->segcxt.ws_dir,
												   segment_name);
	if (state->seg.ws_file < 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: missing segment %s in %s",
						segment_name, state->segcxt.ws_dir)));
}

static void
fb_wal_close_segment(XLogReaderState *state)
{
	CloseTransientFile(state->seg.ws_file);
	state->seg.ws_file = -1;
}

static int
fb_wal_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr, int req_len,
				 XLogRecPtr target_rec_ptr, char *read_buf)
{
	FbWalReaderPrivate *private = (FbWalReaderPrivate *) state->private_data;
	WALReadError errinfo;
	int count = XLOG_BLCKSZ;

	(void) target_rec_ptr;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (target_page_ptr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (target_page_ptr + req_len <= private->endptr)
			count = private->endptr - target_page_ptr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	if (!WALRead(state, read_buf, target_page_ptr, count, private->timeline_id,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char segment_name[MAXPGPATH];

		XLogFileName(segment_name, seg->ws_tli, seg->ws_segno,
					 state->segcxt.ws_segsize);

		if (errinfo.wre_errno != 0)
		{
			errno = errinfo.wre_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL segment \"%s\", offset %d: %m",
							segment_name, errinfo.wre_off)));
		}

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read WAL segment \"%s\", offset %d: read %d of %d",
						segment_name, errinfo.wre_off, errinfo.wre_read,
						errinfo.wre_req)));
	}

	return count;
}

static HTAB *
fb_create_touched_xid_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(FbTouchedXidEntry);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb touched xids", 128, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_create_unsafe_xid_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(FbUnsafeXidEntry);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb unsafe xids", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_create_xid_status_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(FbXidStatusEntry);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb xid statuses", 128, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static bool
fb_hash_has_xid(HTAB *hash, TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return false;

	return hash_search(hash, &xid, HASH_FIND, NULL) != NULL;
}

static FbXidStatusEntry *
fb_get_xid_status_entry(HTAB *hash, TransactionId xid, bool *found)
{
	if (!TransactionIdIsValid(xid))
		return NULL;

	return (FbXidStatusEntry *) hash_search(hash, &xid, HASH_ENTER, found);
}

static char *
fb_copy_bytes(const char *data, Size len)
{
	char *copy;

	if (data == NULL || len == 0)
		return NULL;

	copy = palloc(len);
	memcpy(copy, data, len);
	return copy;
}

static void
fb_note_checkpoint_record(XLogReaderState *reader, FbWalScanContext *ctx)
{
	uint8 rmid = XLogRecGetRmid(reader);
	uint8 info_code;
	CheckPoint *checkpoint;
	TimestampTz checkpoint_ts;

	if (rmid != RM_XLOG_ID)
		return;

	info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (info_code != XLOG_CHECKPOINT_SHUTDOWN &&
		info_code != XLOG_CHECKPOINT_ONLINE)
		return;

	checkpoint = (CheckPoint *) XLogRecGetData(reader);
	checkpoint_ts = time_t_to_timestamptz(checkpoint->time);

	if (checkpoint_ts > ctx->target_ts)
		return;

	if (!ctx->anchor_found ||
		checkpoint_ts > ctx->anchor_time ||
		(checkpoint_ts == ctx->anchor_time &&
		 checkpoint->redo >= ctx->anchor_redo_lsn))
	{
		ctx->anchor_found = true;
		ctx->anchor_checkpoint_lsn = reader->ReadRecPtr;
		ctx->anchor_redo_lsn = checkpoint->redo;
		ctx->anchor_time = checkpoint_ts;
	}
}

static void
fb_index_append_record(FbWalRecordIndex *index, const FbRecordRef *record)
{
	uint32 old_capacity = index->record_capacity;
	Size new_bytes = 0;

	if (index->record_count == index->record_capacity)
	{
		index->record_capacity = (index->record_capacity == 0) ? 32 :
			index->record_capacity * 2;
		new_bytes = sizeof(FbRecordRef) *
			(index->record_capacity - old_capacity);
		fb_index_charge_bytes(index, new_bytes, "RecordRef array");
		if (index->records == NULL)
			index->records = palloc0(sizeof(FbRecordRef) * index->record_capacity);
		else
		{
			index->records = repalloc(index->records,
									  sizeof(FbRecordRef) * index->record_capacity);
			MemSet(index->records + old_capacity, 0,
				   sizeof(FbRecordRef) * (index->record_capacity - old_capacity));
		}
	}

	index->records[index->record_count++] = *record;
	index->total_record_count++;
}

static void
fb_record_block_copy_image(FbRecordBlockRef *block_ref, XLogReaderState *reader,
						   FbWalRecordIndex *index)
{
	char page[BLCKSZ];

	if (!XLogRecHasBlockImage(reader, block_ref->block_id))
		return;
	if (!RestoreBlockImage(reader, block_ref->block_id, page))
		return;

	block_ref->has_image = true;
	block_ref->apply_image = XLogRecBlockImageApply(reader, block_ref->block_id);
	fb_index_charge_bytes(index, BLCKSZ, "FPI image");
	block_ref->image = fb_copy_bytes(page, BLCKSZ);
}

static void
fb_fill_record_block_ref(FbRecordBlockRef *block_ref, XLogReaderState *reader,
						 uint8 block_id, const FbRelationInfo *info,
						 FbWalRecordIndex *index)
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
	Size datalen = 0;
	char *data;

	MemSet(block_ref, 0, sizeof(*block_ref));
	if (!XLogRecGetBlockTagExtended(reader, block_id, &locator, &forknum, &blkno, NULL))
		return;

	block_ref->in_use = true;
	block_ref->block_id = block_id;
	block_ref->locator = locator;
	block_ref->forknum = forknum;
	block_ref->blkno = blkno;
	block_ref->is_main_relation = RelFileLocatorEquals(locator, info->locator);

	if (XLogRecHasBlockData(reader, block_id))
	{
		data = XLogRecGetBlockData(reader, block_id, &datalen);
		block_ref->has_data = (data != NULL && datalen > 0);
		if (block_ref->has_data)
		{
			fb_index_charge_bytes(index, datalen, "block data");
			block_ref->data = fb_copy_bytes(data, datalen);
			block_ref->data_len = datalen;
		}
	}

	fb_record_block_copy_image(block_ref, reader, index);
}

static void
fb_note_xid_status(HTAB *xid_statuses, TransactionId xid,
				   FbWalXidStatus status, TimestampTz commit_ts,
				   XLogRecPtr commit_lsn)
{
	FbXidStatusEntry *entry;
	bool found;

	entry = fb_get_xid_status_entry(xid_statuses, xid, &found);
	if (entry == NULL)
		return;

	entry->xid = xid;
	entry->status = status;
	entry->commit_ts = commit_ts;
	entry->commit_lsn = commit_lsn;
}

static void
fb_copy_heap_record_ref(XLogReaderState *reader, const FbRelationInfo *info,
						FbWalRecordKind kind, FbWalRecordIndex *index)
{
	FbRecordRef record;
	int block_count = 0;
	int block_id;

	MemSet(&record, 0, sizeof(record));
	record.kind = kind;
	record.lsn = reader->ReadRecPtr;
	record.end_lsn = reader->EndRecPtr;
	record.xid = fb_record_xid(reader);
	record.info = XLogRecGetInfo(reader);
	record.init_page = ((XLogRecGetInfo(reader) & XLOG_HEAP_INIT_PAGE) != 0);
	fb_index_charge_bytes(index, XLogRecGetDataLen(reader), "main data");
	record.main_data = fb_copy_bytes(XLogRecGetData(reader), XLogRecGetDataLen(reader));
	record.main_data_len = XLogRecGetDataLen(reader);

	for (block_id = 0;
		 block_id <= XLogRecMaxBlockId(reader) && block_count < FB_WAL_MAX_BLOCK_REFS;
		 block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!XLogRecGetBlockTagExtended(reader, block_id, &locator, &forknum,
										&blkno, NULL))
			continue;
		if (forknum != MAIN_FORKNUM)
			continue;
		if (!fb_locator_matches_relation(&locator, info))
			continue;

		fb_fill_record_block_ref(&record.blocks[block_count], reader, block_id, info, index);
		block_count++;
	}

	record.block_count = block_count;
	fb_index_append_record(index, &record);
}

static bool
fb_heap_record_matches_target(XLogReaderState *reader, const FbRelationInfo *info)
{
	return fb_record_touches_relation(reader, info);
}

static void
fb_mark_xid_touched(HTAB *touched_xids, TransactionId xid,
					FbWalScanContext *ctx)
{
	FbTouchedXidEntry *entry;
	bool found;

	if (!TransactionIdIsValid(xid))
		return;

	entry = (FbTouchedXidEntry *) hash_search(touched_xids, &xid, HASH_ENTER,
											  &found);
	if (!found)
	{
		entry->xid = xid;
		ctx->touched_xids++;
	}
}

static void
fb_mark_xid_unsafe(HTAB *unsafe_xids, TransactionId xid,
				   FbWalUnsafeReason reason, FbWalScanContext *ctx)
{
	FbUnsafeXidEntry *entry;
	bool found;

	if (!TransactionIdIsValid(xid))
	{
		fb_mark_unsafe(ctx, reason);
		return;
	}

	entry = (FbUnsafeXidEntry *) hash_search(unsafe_xids, &xid, HASH_ENTER,
											 &found);
	if (!found)
	{
		entry->xid = xid;
		entry->reason = reason;
	}
	else if (entry->reason == FB_WAL_UNSAFE_STORAGE_CHANGE &&
			 reason != FB_WAL_UNSAFE_STORAGE_CHANGE)
		entry->reason = reason;
}

static FbUnsafeXidEntry *
fb_find_unsafe_xid(HTAB *unsafe_xids, TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return NULL;

	return (FbUnsafeXidEntry *) hash_search(unsafe_xids, &xid, HASH_FIND, NULL);
}

static TransactionId
fb_record_xid(XLogReaderState *reader)
{
	TransactionId xid = XLogRecGetTopXid(reader);

	if (TransactionIdIsValid(xid))
		return xid;

	return XLogRecGetXid(reader);
}

static void
fb_mark_record_xids_touched(XLogReaderState *reader, HTAB *touched_xids,
							FbWalScanContext *ctx)
{
	TransactionId xid = XLogRecGetXid(reader);
	TransactionId top_xid = XLogRecGetTopXid(reader);

	fb_mark_xid_touched(touched_xids, xid, ctx);
	if (top_xid != xid)
		fb_mark_xid_touched(touched_xids, top_xid, ctx);
}

static bool
fb_locator_matches_relation(const RelFileLocator *locator,
							const FbRelationInfo *info)
{
	if (RelFileLocatorEquals(*locator, info->locator))
		return true;

	if (info->has_toast_locator &&
		RelFileLocatorEquals(*locator, info->toast_locator))
		return true;

	return false;
}

static bool
fb_record_touches_relation(XLogReaderState *reader, const FbRelationInfo *info)
{
	int block_id;

	for (block_id = 0; block_id <= XLogRecMaxBlockId(reader); block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!XLogRecGetBlockTagExtended(reader, block_id, &locator, &forknum,
										&blkno, NULL))
			continue;

		(void) blkno;

		if (forknum != MAIN_FORKNUM)
			continue;

		if (fb_locator_matches_relation(&locator, info))
			return true;
	}

	return false;
}

static void
fb_mark_unsafe(FbWalScanContext *ctx, FbWalUnsafeReason reason)
{
	if (ctx->unsafe)
		return;

	ctx->unsafe = true;
	ctx->unsafe_reason = reason;
}

static bool
fb_heap_truncate_matches_relation(XLogReaderState *reader,
								  const FbRelationInfo *info)
{
	xl_heap_truncate *xlrec = (xl_heap_truncate *) XLogRecGetData(reader);
	uint32 i;

	if (xlrec->dbId != info->locator.dbOid)
		return false;

	for (i = 0; i < xlrec->nrelids; i++)
	{
		if (xlrec->relids[i] == info->relid)
			return true;
		if (OidIsValid(info->toast_relid) && xlrec->relids[i] == info->toast_relid)
			return true;
	}

	return false;
}

static bool
fb_heap_rewrite_matches_relation(XLogReaderState *reader,
								 const FbRelationInfo *info)
{
	xl_heap_rewrite_mapping *xlrec;

	xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);

	if (xlrec->mapped_db != info->locator.dbOid)
		return false;

	if (xlrec->mapped_rel == info->relid)
		return true;

	if (OidIsValid(info->toast_relid) && xlrec->mapped_rel == info->toast_relid)
		return true;

	return false;
}

static bool
fb_smgr_record_matches_relation(XLogReaderState *reader, const FbRelationInfo *info)
{
	uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	if (info_code == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(reader);

		if (xlrec->forkNum != MAIN_FORKNUM)
			return false;

		return fb_locator_matches_relation(&xlrec->rlocator, info);
	}
	else if (info_code == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(reader);

		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) == 0)
			return false;

		return fb_locator_matches_relation(&xlrec->rlocator, info);
	}

	return false;
}

static void
fb_note_xact_record(XLogReaderState *reader, HTAB *touched_xids,
					HTAB *unsafe_xids,
					FbWalScanContext *ctx)
{
	uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	TransactionId xid = XLogRecGetXid(reader);
	FbUnsafeXidEntry *unsafe_entry = fb_find_unsafe_xid(unsafe_xids, xid);

	(void) touched_xids;

	switch (info_code)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_COMMIT_PREPARED:
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				xl_xact_parsed_commit parsed;

				ParseCommitRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				if (parsed.xact_time > ctx->target_ts &&
					parsed.xact_time <= ctx->query_now_ts)
				{
					ctx->commit_count++;
					if (unsafe_entry != NULL)
						fb_mark_unsafe(ctx, unsafe_entry->reason);
				}
				break;
			}
		case XLOG_XACT_ABORT:
		case XLOG_XACT_ABORT_PREPARED:
			{
				xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(reader);
				xl_xact_parsed_abort parsed;

				ParseAbortRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				if (parsed.xact_time > ctx->target_ts &&
					parsed.xact_time <= ctx->query_now_ts)
					ctx->abort_count++;
				break;
			}
		default:
			break;
	}
}

static void
fb_note_xact_status_for_touched(XLogReaderState *reader,
								HTAB *touched_xids,
								HTAB *unsafe_xids,
								FbWalRecordIndex *index,
								FbWalScanContext *ctx)
{
	uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	TransactionId xid = XLogRecGetXid(reader);
	FbUnsafeXidEntry *unsafe_entry = fb_find_unsafe_xid(unsafe_xids, xid);

	switch (info_code)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_COMMIT_PREPARED:
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				xl_xact_parsed_commit parsed;
				TimestampTz commit_ts;
				int i;

				ParseCommitRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				commit_ts = parsed.xact_time;
				if (fb_hash_has_xid(touched_xids, xid))
				{
					fb_note_xid_status(index->xid_statuses, xid,
									   FB_WAL_XID_COMMITTED, commit_ts,
									   reader->EndRecPtr);
					if (commit_ts > ctx->target_ts &&
						commit_ts <= ctx->query_now_ts)
						index->target_commit_count++;
				}

				for (i = 0; i < parsed.nsubxacts; i++)
				{
					TransactionId subxid = parsed.subxacts[i];

					if (!fb_hash_has_xid(touched_xids, subxid))
						continue;

					fb_note_xid_status(index->xid_statuses, subxid,
									   FB_WAL_XID_COMMITTED, commit_ts,
									   reader->EndRecPtr);
					if (commit_ts > ctx->target_ts &&
						commit_ts <= ctx->query_now_ts)
						index->target_commit_count++;
				}

				if (commit_ts > ctx->target_ts &&
					commit_ts <= ctx->query_now_ts &&
					unsafe_entry != NULL)
				{
					fb_mark_unsafe(ctx, unsafe_entry->reason);
				}
				break;
			}
		case XLOG_XACT_ABORT:
		case XLOG_XACT_ABORT_PREPARED:
			{
				xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(reader);
				xl_xact_parsed_abort parsed;
				TimestampTz abort_ts;
				int i;

				ParseAbortRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				abort_ts = parsed.xact_time;

				if (fb_hash_has_xid(touched_xids, xid))
				{
					fb_note_xid_status(index->xid_statuses, xid,
									   FB_WAL_XID_ABORTED, abort_ts,
									   reader->EndRecPtr);
					if (abort_ts > ctx->target_ts &&
						abort_ts <= ctx->query_now_ts)
						index->target_abort_count++;
				}

				for (i = 0; i < parsed.nsubxacts; i++)
				{
					TransactionId subxid = parsed.subxacts[i];

					if (!fb_hash_has_xid(touched_xids, subxid))
						continue;

					fb_note_xid_status(index->xid_statuses, subxid,
									   FB_WAL_XID_ABORTED, abort_ts,
									   reader->EndRecPtr);
					if (abort_ts > ctx->target_ts &&
						abort_ts <= ctx->query_now_ts)
						index->target_abort_count++;
				}
				break;
			}
		default:
			break;
	}
}

static bool
fb_scan_record_visitor(XLogReaderState *reader, void *arg)
{
	FbWalScanVisitorState *state = (FbWalScanVisitorState *) arg;
	const FbRelationInfo *info = state->info;
	FbWalScanContext *ctx = state->ctx;
	HTAB *touched_xids = state->touched_xids;
	HTAB *unsafe_xids = state->unsafe_xids;
	uint8 rmid = XLogRecGetRmid(reader);

	fb_note_checkpoint_record(reader, ctx);

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_HEAP_TRUNCATE &&
			fb_heap_truncate_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_TRUNCATE, ctx);
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (info_code == XLOG_HEAP2_REWRITE &&
			fb_heap_rewrite_matches_relation(reader, info))
		{
			xl_heap_rewrite_mapping *xlrec;

			xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);
			fb_mark_xid_unsafe(unsafe_xids, xlrec->mapped_xid,
							   FB_WAL_UNSAFE_REWRITE, ctx);
		}
	}
	else if (rmid == RM_SMGR_ID)
	{
		if (fb_smgr_record_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_STORAGE_CHANGE, ctx);
	}

	if (fb_record_touches_relation(reader, info))
		fb_mark_record_xids_touched(reader, touched_xids, ctx);

	if (rmid == RM_XACT_ID)
		fb_note_xact_record(reader, touched_xids, unsafe_xids, ctx);

	return !ctx->unsafe;
}

static bool
fb_index_record_visitor(XLogReaderState *reader, void *arg)
{
	FbWalIndexBuildState *state = (FbWalIndexBuildState *) arg;
	const FbRelationInfo *info = state->info;
	FbWalScanContext *ctx = state->ctx;
	FbWalRecordIndex *index = state->index;
	HTAB *touched_xids = state->touched_xids;
	HTAB *unsafe_xids = state->unsafe_xids;
	uint8 rmid = XLogRecGetRmid(reader);

	fb_note_checkpoint_record(reader, ctx);

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_HEAP_TRUNCATE &&
			fb_heap_truncate_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_TRUNCATE, ctx);

		if (fb_heap_record_matches_target(reader, info))
		{
			uint8 heap_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

			fb_mark_record_xids_touched(reader, touched_xids, ctx);

			switch (heap_code)
			{
				case XLOG_HEAP_INSERT:
					fb_copy_heap_record_ref(reader, info,
										 FB_WAL_RECORD_HEAP_INSERT, index);
					break;
				case XLOG_HEAP_DELETE:
					if ((((xl_heap_delete *) XLogRecGetData(reader))->flags &
						 XLH_DELETE_IS_SUPER) == 0)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_DELETE, index);
					break;
				case XLOG_HEAP_UPDATE:
					fb_copy_heap_record_ref(reader, info,
										 FB_WAL_RECORD_HEAP_UPDATE, index);
					break;
				case XLOG_HEAP_HOT_UPDATE:
					fb_copy_heap_record_ref(reader, info,
										 FB_WAL_RECORD_HEAP_HOT_UPDATE, index);
					break;
				default:
					break;
			}
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (info_code == XLOG_HEAP2_REWRITE &&
			fb_heap_rewrite_matches_relation(reader, info))
		{
			xl_heap_rewrite_mapping *xlrec;

			xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);
			fb_mark_xid_unsafe(unsafe_xids, xlrec->mapped_xid,
							   FB_WAL_UNSAFE_REWRITE, ctx);
		}
	}
	else if (rmid == RM_SMGR_ID)
	{
		if (fb_smgr_record_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_STORAGE_CHANGE, ctx);
	}

	if (rmid == RM_XACT_ID)
		fb_note_xact_status_for_touched(reader, touched_xids, unsafe_xids,
										index, ctx);

	return !ctx->unsafe;
}

void
fb_wal_visit_records(FbWalScanContext *ctx, FbWalRecordVisitor visitor, void *arg)
{
	FbWalReaderPrivate private;
	XLogReaderState *reader;
	XLogRecPtr first_record;

	MemSet(&private, 0, sizeof(private));
	private.timeline_id = ctx->timeline_id;
	private.endptr = ctx->end_lsn;

	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_get_archive_dir(),
								XL_ROUTINE(.page_read = fb_wal_read_page,
										   .segment_open = fb_wal_open_segment,
										   .segment_close = fb_wal_close_segment),
								&private);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	first_record = XLogFindNextRecord(reader, ctx->start_lsn);
	if (XLogRecPtrIsInvalid(first_record))
	{
		XLogReaderFree(reader);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find a valid WAL record in %s",
						fb_get_archive_dir())));
	}

	ctx->first_record_lsn = first_record;

	while (true)
	{
		XLogRecord *record;
		char *errormsg = NULL;

		record = XLogReadRecord(reader, &errormsg);
		if (record == NULL)
			break;

		ctx->records_scanned++;
		ctx->last_record_lsn = reader->EndRecPtr;
		if (visitor != NULL && !visitor(reader, arg))
			break;
	}

	XLogReaderFree(reader);
}

void
fb_require_archive_has_wal_segments(void)
{
	FbWalScanContext ctx;

	MemSet(&ctx, 0, sizeof(ctx));
	fb_collect_archive_segments(&ctx);
}

void
fb_wal_prepare_scan_context(TimestampTz target_ts, FbWalScanContext *ctx)
{
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	MemSet(ctx, 0, sizeof(*ctx));
	ctx->target_ts = target_ts;
	ctx->query_now_ts = GetCurrentTimestamp();

	fb_collect_archive_segments(ctx);
}

void
fb_wal_scan_relation_window(const FbRelationInfo *info, FbWalScanContext *ctx)
{
	FbWalScanVisitorState state;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.ctx = ctx;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();

	fb_wal_visit_records(ctx, fb_scan_record_visitor, &state);

	hash_destroy(state.touched_xids);
	hash_destroy(state.unsafe_xids);
}

void
fb_wal_build_record_index(const FbRelationInfo *info,
						  FbWalScanContext *ctx,
						  FbWalRecordIndex *index)
{
	FbWalIndexBuildState state;
	uint32 i;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (index == NULL)
		elog(ERROR, "FbWalRecordIndex must not be NULL");

	MemSet(index, 0, sizeof(*index));
	index->memory_limit_bytes = fb_get_memory_limit_bytes();
	index->xid_statuses = fb_create_xid_status_hash();

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.ctx = ctx;
	state.index = index;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();

	fb_wal_visit_records(ctx, fb_index_record_visitor, &state);

	if (!ctx->anchor_found)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: no checkpoint before target timestamp")));

	index->anchor_found = ctx->anchor_found;
	index->anchor_checkpoint_lsn = ctx->anchor_checkpoint_lsn;
	index->anchor_redo_lsn = ctx->anchor_redo_lsn;
	index->anchor_time = ctx->anchor_time;
	index->unsafe = ctx->unsafe;
	index->unsafe_reason = ctx->unsafe_reason;

	for (i = 0; i < index->record_count; i++)
	{
		FbRecordRef *record = &index->records[i];
		FbXidStatusEntry *status_entry;

		if (record->lsn < index->anchor_redo_lsn)
			continue;

		status_entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
														&record->xid,
														HASH_FIND, NULL);
		if (status_entry != NULL)
		{
			record->commit_ts = status_entry->commit_ts;
			record->commit_lsn = status_entry->commit_lsn;
			record->aborted = (status_entry->status == FB_WAL_XID_ABORTED);
			record->committed_after_target =
				(status_entry->status == FB_WAL_XID_COMMITTED &&
				 status_entry->commit_ts > ctx->target_ts &&
				 status_entry->commit_ts <= ctx->query_now_ts);
			record->committed_before_target =
				(status_entry->status == FB_WAL_XID_COMMITTED &&
				 status_entry->commit_ts <= ctx->target_ts);
		}

		index->kept_record_count++;
		if (!record->committed_after_target)
			continue;

		index->target_record_count++;
		switch (record->kind)
		{
			case FB_WAL_RECORD_HEAP_INSERT:
				index->target_insert_count++;
				break;
			case FB_WAL_RECORD_HEAP_DELETE:
				index->target_delete_count++;
				break;
			case FB_WAL_RECORD_HEAP_UPDATE:
			case FB_WAL_RECORD_HEAP_HOT_UPDATE:
				index->target_update_count++;
				break;
		}
	}

	hash_destroy(state.touched_xids);
	hash_destroy(state.unsafe_xids);
}

bool
fb_wal_lookup_xid_status(const FbWalRecordIndex *index,
						 TransactionId xid,
						 FbWalXidStatus *status,
						 TimestampTz *commit_ts)
{
	FbXidStatusEntry *entry;

	if (index == NULL || index->xid_statuses == NULL || !TransactionIdIsValid(xid))
		return false;

	entry = (FbXidStatusEntry *) hash_search(index->xid_statuses, &xid,
											 HASH_FIND, NULL);
	if (entry == NULL)
		return false;

	if (status != NULL)
		*status = entry->status;
	if (commit_ts != NULL)
		*commit_ts = entry->commit_ts;
	return true;
}

const char *
fb_wal_unsafe_reason_name(FbWalUnsafeReason reason)
{
	switch (reason)
	{
		case FB_WAL_UNSAFE_NONE:
			return "none";
		case FB_WAL_UNSAFE_TRUNCATE:
			return "truncate";
		case FB_WAL_UNSAFE_REWRITE:
			return "rewrite";
		case FB_WAL_UNSAFE_STORAGE_CHANGE:
			return "storage_change";
	}

	return "unknown";
}

char *
fb_wal_debug_summary(const FbWalScanContext *ctx)
{
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	return psprintf("complete=%s anchor=%s unsafe=%s reason=%s",
					ctx->segments_complete ? "true" : "false",
					ctx->anchor_found ? "true" : "false",
					ctx->unsafe ? "true" : "false",
					fb_wal_unsafe_reason_name(ctx->unsafe_reason));
}

char *
fb_wal_index_debug_summary(const FbWalScanContext *ctx,
						   const FbWalRecordIndex *index)
{
	if (ctx == NULL || index == NULL)
		elog(ERROR, "summary inputs must not be NULL");

	return psprintf("anchor=%s unsafe=%s reason=%s refs=%llu target_dml=%llu commits=%llu aborts=%llu",
					index->anchor_found ? "true" : "false",
					index->unsafe ? "true" : "false",
					fb_wal_unsafe_reason_name(index->unsafe_reason),
					(unsigned long long) index->kept_record_count,
					(unsigned long long) index->target_record_count,
					(unsigned long long) index->target_commit_count,
					(unsigned long long) index->target_abort_count);
}
