/*
 * fb_wal.h
 *    WAL scan and record index interfaces.
 */

#ifndef FB_WAL_H
#define FB_WAL_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "utils/hsearch.h"

#include "fb_common.h"
#include "fb_spool.h"

typedef struct FbSummaryQueryCache FbSummaryQueryCache;

typedef struct FbWalBlockKey
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
} FbWalBlockKey;

typedef struct FbWalPrecomputedMissingBlock
{
	FbWalBlockKey key;
	uint32 first_record_index;
	XLogRecPtr first_record_lsn;
} FbWalPrecomputedMissingBlock;

typedef struct FbWalResolvedSegment
{
	char name[25];
	TimeLineID timeline_id;
	XLogSegNo segno;
	char path[MAXPGPATH];
	off_t bytes;
	bool partial;
	bool valid;
	bool mismatch;
	bool ignored;
	int source_kind;
} FbWalResolvedSegment;

typedef enum FbWalUnsafeReason
{
	FB_WAL_UNSAFE_NONE = 0,
	FB_WAL_UNSAFE_TRUNCATE,
	FB_WAL_UNSAFE_REWRITE,
	FB_WAL_UNSAFE_STORAGE_CHANGE
} FbWalUnsafeReason;

typedef enum FbWalUnsafeScope
{
	FB_WAL_UNSAFE_SCOPE_NONE = 0,
	FB_WAL_UNSAFE_SCOPE_MAIN,
	FB_WAL_UNSAFE_SCOPE_TOAST
} FbWalUnsafeScope;

typedef enum FbWalStorageChangeOp
{
	FB_WAL_STORAGE_CHANGE_UNKNOWN = 0,
	FB_WAL_STORAGE_CHANGE_STANDBY_LOCK,
	FB_WAL_STORAGE_CHANGE_SMGR_CREATE,
	FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE
} FbWalStorageChangeOp;

/*
 * FbWalScanContext
 *    Tracks WAL scan context.
 */

typedef struct FbWalScanContext
{
	TimestampTz target_ts;
	TimestampTz query_now_ts;
	TimeLineID timeline_id;
	int wal_seg_size;
	char first_segment[25];
	char last_segment[25];
	char retained_gap_left_segment[25];
	char retained_gap_right_segment[25];
	uint32 total_segments;
	bool segments_complete;
	bool retained_suffix_only;
	bool end_is_partial;
	XLogRecPtr start_lsn;
	XLogRecPtr original_start_lsn;
	XLogRecPtr end_lsn;
	XLogRecPtr first_record_lsn;
	XLogRecPtr last_record_lsn;
	uint64 records_scanned;
	uint64 touched_xids;
	uint64 commit_count;
	uint64 abort_count;
	bool anchor_found;
	bool anchor_hint_found;
	XLogRecPtr anchor_checkpoint_lsn;
	XLogRecPtr anchor_redo_lsn;
	TimestampTz anchor_time;
	bool start_lsn_pruned;
	uint32 checkpoint_sidecar_entries;
	uint32 anchor_hint_segment_index;
	bool unsafe;
	FbWalUnsafeReason unsafe_reason;
	TransactionId unsafe_xid;
	TimestampTz unsafe_commit_ts;
	XLogRecPtr unsafe_record_lsn;
	FbWalUnsafeScope unsafe_scope;
	FbWalStorageChangeOp unsafe_storage_op;
	void *resolved_segments;
	uint32 resolved_segment_count;
	uint32 pg_wal_segment_count;
	uint32 archive_segment_count;
	uint32 ckwal_segment_count;
	bool using_archive_dest;
	bool using_legacy_archive_dir;
	bool ckwal_invoked;
	int parallel_workers;
	bool segment_prefilter_ready;
	bool segment_prefilter_used;
	uint32 prefilter_hit_segments;
	uint32 prefilter_total_segments;
	bool *segment_hit_map;
	uint32 summary_span_windows;
	uint32 summary_span_covered_segments;
	uint32 summary_span_fallback_segments;
	uint32 summary_xid_hits;
	uint32 summary_xid_fallback;
	uint32 summary_xid_segments_read;
	uint32 summary_unsafe_hits;
	uint32 metadata_fallback_windows;
	uint64 payload_sparse_reader_resets;
	uint64 payload_sparse_reader_reuses;
	bool current_segment_may_hit;
	uint32 progress_segment_total;
	uint32 visited_segment_count;
	FbSpoolSession *spool_session;
	FbSummaryQueryCache *summary_cache;
} FbWalScanContext;

typedef enum FbWalRecordKind
{
	FB_WAL_RECORD_HEAP_INSERT = 1,
	FB_WAL_RECORD_HEAP_DELETE,
	FB_WAL_RECORD_HEAP_UPDATE,
	FB_WAL_RECORD_HEAP_HOT_UPDATE,
	FB_WAL_RECORD_HEAP_CONFIRM,
	FB_WAL_RECORD_HEAP_LOCK,
	FB_WAL_RECORD_HEAP_INPLACE,
	FB_WAL_RECORD_HEAP2_PRUNE,
	FB_WAL_RECORD_HEAP2_VISIBLE,
	FB_WAL_RECORD_HEAP2_MULTI_INSERT,
	FB_WAL_RECORD_HEAP2_LOCK_UPDATED,
	FB_WAL_RECORD_XLOG_FPI,
	FB_WAL_RECORD_XLOG_FPI_FOR_HINT
} FbWalRecordKind;

typedef enum FbWalXidStatus
{
	FB_WAL_XID_UNKNOWN = 0,
	FB_WAL_XID_COMMITTED,
	FB_WAL_XID_ABORTED
} FbWalXidStatus;

typedef enum FbWalPayloadScanMode
{
	FB_WAL_PAYLOAD_SCAN_WINDOWED = 0,
	FB_WAL_PAYLOAD_SCAN_SPARSE
} FbWalPayloadScanMode;

/*
 * FbRecordBlockRef
 *    WAL structure.
 */

typedef struct FbRecordBlockRef
{
	bool in_use;
	uint8 block_id;
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
	bool is_main_relation;
	bool is_toast_relation;
	bool has_image;
	bool apply_image;
	char *image;
	bool has_data;
	char *data;
	Size data_len;
} FbRecordBlockRef;

#define FB_WAL_MAX_BLOCK_REFS 2

/*
 * FbRecordRef
 *    WAL structure.
 */

typedef struct FbRecordRef
{
	FbWalRecordKind kind;
	XLogRecPtr lsn;
	XLogRecPtr end_lsn;
	TransactionId xid;
	uint8 info;
	bool init_page;
	bool committed_after_target;
	bool committed_before_target;
	bool aborted;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
	int block_count;
	FbRecordBlockRef blocks[FB_WAL_MAX_BLOCK_REFS];
	char *main_data;
	Size main_data_len;
} FbRecordRef;

/*
 * FbWalRecordIndex
 *    WAL structure.
 */

typedef struct FbWalRecordIndex
{
	TimestampTz target_ts;
	TimestampTz query_now_ts;
	XLogRecPtr anchor_checkpoint_lsn;
	XLogRecPtr anchor_redo_lsn;
	TimestampTz anchor_time;
	bool anchor_found;
	bool unsafe;
	FbWalUnsafeReason unsafe_reason;
	TransactionId unsafe_xid;
	TimestampTz unsafe_commit_ts;
	XLogRecPtr unsafe_record_lsn;
	FbWalUnsafeScope unsafe_scope;
	FbWalStorageChangeOp unsafe_storage_op;
	uint64 total_record_count;
	uint64 kept_record_count;
	uint64 target_record_count;
	uint64 target_commit_count;
	uint64 target_abort_count;
	uint64 target_insert_count;
	uint64 target_delete_count;
	uint64 target_update_count;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
	uint32 record_count;
	uint32 payload_window_count;
	uint32 payload_parallel_workers;
	uint32 payload_covered_segment_count;
	FbWalPayloadScanMode payload_scan_mode;
	uint64 payload_scanned_record_count;
	uint64 payload_kept_record_count;
	uint64 payload_sparse_reader_resets;
	uint64 payload_sparse_reader_reuses;
	bool tail_inline_payload;
	XLogRecPtr tail_cutover_lsn;
	HTAB *xid_statuses;
	FbSpoolSession *spool_session;
	FbSpoolLog *record_log;
	FbSpoolLog *record_tail_log;
	HTAB *precomputed_missing_blocks;
	uint32 precomputed_missing_block_count;
	const FbWalResolvedSegment *resolved_segments;
	uint32 resolved_segment_count;
	int wal_seg_size;
	FbSummaryQueryCache *summary_cache;
} FbWalRecordIndex;

typedef struct FbWalRecordCursor FbWalRecordCursor;

typedef bool (*FbWalRecordVisitor) (XLogReaderState *reader, void *arg);

/*
 * fb_require_archive_has_wal_segments
 *    WAL API.
 */

void fb_require_archive_has_wal_segments(void);
/*
 * fb_wal_prepare_scan_context
 *    WAL API.
 */

void fb_wal_prepare_scan_context(TimestampTz target_ts,
								 FbSpoolSession *spool_session,
								 FbWalScanContext *ctx);
/*
 * fb_wal_scan_relation_window
 *    WAL API.
 */

void fb_wal_scan_relation_window(const FbRelationInfo *info, FbWalScanContext *ctx);
/*
 * fb_wal_build_record_index
 *    WAL API.
 */

void fb_wal_build_record_index(const FbRelationInfo *info,
							   FbWalScanContext *ctx,
							   FbWalRecordIndex *index);
/*
 * fb_wal_lookup_xid_status
 *    WAL API.
 */

bool fb_wal_lookup_xid_status(const FbWalRecordIndex *index,
							  TransactionId xid,
							  FbWalXidStatus *status,
							  TimestampTz *commit_ts);
FbWalRecordCursor *fb_wal_record_cursor_open(const FbWalRecordIndex *index,
											 FbSpoolDirection direction);
bool fb_wal_record_cursor_seek(FbWalRecordCursor *cursor, uint32 record_index);
bool fb_wal_record_cursor_read(FbWalRecordCursor *cursor,
							   FbRecordRef *record,
							   uint32 *record_index);
void fb_wal_record_cursor_close(FbWalRecordCursor *cursor);
bool fb_wal_record_load(const FbWalRecordIndex *index,
						 uint32 record_index,
						 FbRecordRef *record);
/*
 * fb_wal_visit_records
 *    WAL API.
 */

void fb_wal_visit_records(FbWalScanContext *ctx, FbWalRecordVisitor visitor,
						  void *arg);
void fb_wal_visit_resolved_records(const FbWalRecordIndex *index,
								   XLogRecPtr end_lsn,
								   FbWalRecordVisitor visitor,
								   void *arg);
/*
 * fb_wal_unsafe_reason_name
 *    WAL API.
 */

const char *fb_wal_unsafe_reason_name(FbWalUnsafeReason reason);
const char *fb_wal_unsafe_scope_name(FbWalUnsafeScope scope);
const char *fb_wal_storage_change_op_name(FbWalStorageChangeOp op);
const char *fb_wal_payload_scan_mode_name(FbWalPayloadScanMode mode);

#endif
