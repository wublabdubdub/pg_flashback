#ifndef FB_WAL_H
#define FB_WAL_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "utils/hsearch.h"

#include "fb_common.h"

typedef enum FbWalUnsafeReason
{
	FB_WAL_UNSAFE_NONE = 0,
	FB_WAL_UNSAFE_TRUNCATE,
	FB_WAL_UNSAFE_REWRITE,
	FB_WAL_UNSAFE_STORAGE_CHANGE
} FbWalUnsafeReason;

typedef struct FbWalScanContext
{
	TimestampTz target_ts;
	TimestampTz query_now_ts;
	TimeLineID timeline_id;
	int wal_seg_size;
	char first_segment[25];
	char last_segment[25];
	uint32 total_segments;
	bool segments_complete;
	bool end_is_partial;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	XLogRecPtr first_record_lsn;
	XLogRecPtr last_record_lsn;
	uint64 records_scanned;
	uint64 touched_xids;
	uint64 commit_count;
	uint64 abort_count;
	bool anchor_found;
	XLogRecPtr anchor_checkpoint_lsn;
	XLogRecPtr anchor_redo_lsn;
	TimestampTz anchor_time;
	bool unsafe;
	FbWalUnsafeReason unsafe_reason;
} FbWalScanContext;

typedef enum FbWalRecordKind
{
	FB_WAL_RECORD_HEAP_INSERT = 1,
	FB_WAL_RECORD_HEAP_DELETE,
	FB_WAL_RECORD_HEAP_UPDATE,
	FB_WAL_RECORD_HEAP_HOT_UPDATE
} FbWalRecordKind;

typedef enum FbWalXidStatus
{
	FB_WAL_XID_UNKNOWN = 0,
	FB_WAL_XID_COMMITTED,
	FB_WAL_XID_ABORTED
} FbWalXidStatus;

typedef struct FbRecordBlockRef
{
	bool in_use;
	uint8 block_id;
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
	bool is_main_relation;
	bool has_image;
	bool apply_image;
	char *image;
	bool has_data;
	char *data;
	Size data_len;
} FbRecordBlockRef;

#define FB_WAL_MAX_BLOCK_REFS 2

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

typedef struct FbWalRecordIndex
{
	XLogRecPtr anchor_checkpoint_lsn;
	XLogRecPtr anchor_redo_lsn;
	TimestampTz anchor_time;
	bool anchor_found;
	bool unsafe;
	FbWalUnsafeReason unsafe_reason;
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
	FbRecordRef *records;
	uint32 record_count;
	uint32 record_capacity;
	HTAB *xid_statuses;
} FbWalRecordIndex;

typedef bool (*FbWalRecordVisitor) (XLogReaderState *reader, void *arg);

void fb_require_archive_has_wal_segments(void);
void fb_wal_prepare_scan_context(TimestampTz target_ts, FbWalScanContext *ctx);
void fb_wal_scan_relation_window(const FbRelationInfo *info, FbWalScanContext *ctx);
void fb_wal_build_record_index(const FbRelationInfo *info,
							   FbWalScanContext *ctx,
							   FbWalRecordIndex *index);
bool fb_wal_lookup_xid_status(const FbWalRecordIndex *index,
							  TransactionId xid,
							  FbWalXidStatus *status,
							  TimestampTz *commit_ts);
char *fb_wal_index_debug_summary(const FbWalScanContext *ctx,
								 const FbWalRecordIndex *index);
void fb_wal_visit_records(FbWalScanContext *ctx, FbWalRecordVisitor visitor,
						  void *arg);
const char *fb_wal_unsafe_reason_name(FbWalUnsafeReason reason);
char *fb_wal_debug_summary(const FbWalScanContext *ctx);

#endif
