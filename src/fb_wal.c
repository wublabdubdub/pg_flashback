/*
 * fb_wal.c
 *    WAL scan, source resolution, and record indexing.
 */

#include "postgres.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/standbydefs.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "fb_catalog.h"
#include "fb_guc.h"
#include "fb_ckwal.h"
#include "fb_progress.h"
#include "fb_runtime.h"
#include "fb_summary.h"
#include "fb_summary_service.h"
#include "fb_wal.h"

PG_FUNCTION_INFO_V1(fb_wal_payload_window_contract_debug);
PG_FUNCTION_INFO_V1(fb_wal_nonapply_image_spool_contract_debug);
PG_FUNCTION_INFO_V1(fb_wal_hint_fpi_payload_contract_debug);
PG_FUNCTION_INFO_V1(fb_wal_heap2_visible_payload_contract_debug);
PG_FUNCTION_INFO_V1(fb_summary_xid_resolution_debug);
PG_FUNCTION_INFO_V1(fb_summary_payload_locator_plan_debug);

static bool fb_wal_payload_kind_enabled(uint8 kind);
static bool fb_record_touches_main_relation(XLogReaderState *reader,
											const FbRelationInfo *info);
static void fb_append_touched_xid_hash_samples(StringInfo buf,
												 HTAB *xids,
												 uint32 limit);

#if !defined(HAVE_MEMMEM)
/*
 * memmem
 *    External declaration for platforms without a native prototype.
 */

extern void *memmem(const void *haystack, size_t haystacklen,
					const void *needle, size_t needlelen);
#endif

/*
 * FbWalSegmentEntry
 *    Stores one WAL segment entry.
 */

typedef struct FbWalSegmentEntry
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
} FbWalSegmentEntry;

typedef enum FbWalSourceKind
{
	FB_WAL_SOURCE_PG_WAL = 1,
	FB_WAL_SOURCE_ARCHIVE_DEST,
	FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY,
	FB_WAL_SOURCE_CKWAL
} FbWalSourceKind;

typedef enum FbWalOpenPattern
{
	FB_WAL_OPEN_SEQUENTIAL = 0,
	FB_WAL_OPEN_SPARSE
} FbWalOpenPattern;

/*
 * FbWalReaderPrivate
 *    Private reader state for the WAL scan callback.
 */

typedef struct FbWalReaderPrivate
{
	TimeLineID timeline_id;
	XLogRecPtr endptr;
	bool endptr_reached;
	XLogSegNo last_open_segno;
	bool last_open_segno_valid;
	FbWalSegmentEntry *last_open_entry;
	File current_file;
	bool current_file_open;
	XLogSegNo current_file_segno;
	bool current_file_segno_valid;
	FbWalOpenPattern open_pattern;
	FbWalScanContext *ctx;
} FbWalReaderPrivate;

/*
 * FbTouchedXidEntry
 *    Stores one touched XID entry.
 */

typedef struct FbTouchedXidEntry
{
	TransactionId xid;
} FbTouchedXidEntry;

/*
 * FbUnsafeXidEntry
 *    Stores one unsafe XID entry.
 */

typedef struct FbUnsafeXidEntry
{
	TransactionId xid;
	FbWalUnsafeReason reason;
	FbWalUnsafeScope scope;
	FbWalStorageChangeOp storage_op;
	XLogRecPtr lsn;
} FbUnsafeXidEntry;

/*
 * FbXidStatusEntry
 *    Stores one XID status entry.
 */

typedef struct FbXidStatusEntry
{
	TransactionId xid;
	FbWalXidStatus status;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
} FbXidStatusEntry;

/*
 * FbWalScanVisitorState
 *    Tracks WAL scan visitor state.
 */

typedef struct FbWalScanVisitorState
{
	const FbRelationInfo *info;
	FbWalScanContext *ctx;
	HTAB *touched_xids;
	HTAB *unsafe_xids;
} FbWalScanVisitorState;

typedef struct FbWalAnchorProbeState
{
	FbWalScanContext *ctx;
	bool saw_checkpoint;
	TimestampTz first_checkpoint_ts;
} FbWalAnchorProbeState;

typedef struct FbWalSerialXactVisitorState
{
	FbWalScanContext *ctx;
	FbWalRecordIndex *index;
	HTAB *touched_xids;
	HTAB *unsafe_xids;
	uint32 remaining_xids;
} FbWalSerialXactVisitorState;

typedef struct FbCountOnlyXidEntry
{
	TransactionId xid;
	uint64 target_record_count;
	uint64 insert_row_count;
	uint64 delete_row_count;
	uint64 update_record_count;
} FbCountOnlyXidEntry;

/*
 * FbWalIndexBuildState
 *    Tracks WAL index build state.
 */

typedef struct FbWalIndexBuildState
{
	const FbRelationInfo *info;
	FbWalScanContext *ctx;
	FbWalRecordIndex *index;
	HTAB *touched_xids;
	HTAB *unsafe_xids;
	HTAB *count_only_xids;
	bool collect_metadata;
	bool collect_xact_statuses;
	bool capture_payload;
	bool count_only_capture;
	bool tail_capture_allowed;
	bool defer_payload_body;
	bool locator_only_payload_capture;
	FbSpoolLog **payload_log;
	const char *payload_label;
	XLogRecPtr payload_emit_start_lsn;
	XLogRecPtr payload_emit_end_lsn;
	const FbSummaryPayloadLocator *payload_locators;
	uint32 payload_locator_count;
	uint32 payload_locator_index;
	HTAB *payload_block_states;
	FbSpoolLog **xact_summary_log;
	const char *xact_summary_label;
} FbWalIndexBuildState;

/*
 * FbSerializedRecordBlockRef
 *    On-disk record block metadata.
 */

typedef struct FbSerializedRecordBlockRef
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
	bool has_data;
	uint32 data_len;
} FbSerializedRecordBlockRef;

/*
 * FbSerializedRecordHeader
 *    On-disk record metadata.
 */

typedef struct FbSerializedRecordHeader
{
	FbWalRecordKind kind;
	XLogRecPtr lsn;
	XLogRecPtr end_lsn;
	TransactionId xid;
	uint8 info;
	bool locator_only;
	bool payload_deferred;
	bool init_page;
	int32 block_count;
	uint32 main_data_len;
	FbSerializedRecordBlockRef blocks[FB_WAL_MAX_BLOCK_REFS];
} FbSerializedRecordHeader;

typedef struct FbWalXactSummaryHeader
{
	FbWalXidStatus status;
	TransactionId xid;
	TimestampTz timestamp;
	XLogRecPtr record_lsn;
	XLogRecPtr end_lsn;
	uint32 subxact_count;
} FbWalXactSummaryHeader;

struct FbWalRecordCursor
{
	const FbWalRecordIndex *index;
	FbSpoolCursor *head_cursor;
	FbSpoolCursor *tail_cursor;
	FbSpoolCursor *active_cursor;
	FbSpoolDirection direction;
	uint32 head_count;
	bool reading_tail;
	StringInfoData raw;
	FbRecordRef current;
	bool current_materialized;
	struct FbWalRecordMaterializer *materializer;
};

typedef enum FbWalCursorReadMode
{
	FB_WAL_CURSOR_READ_FULL = 0,
	FB_WAL_CURSOR_READ_SKELETON
} FbWalCursorReadMode;

typedef struct FbWalRecordMaterializer
{
	const FbWalRecordIndex *index;
	FbRelationInfo info;
	FbWalScanContext ctx;
	FbWalReaderPrivate private;
	XLogReaderState *reader;
	char *archive_dir;
	bool initialized;
	uint64 reset_count;
	uint64 reuse_count;
	uint64 locator_stub_materializations;
	uint64 deferred_payload_materializations;
} FbWalRecordMaterializer;

typedef struct FbWalBlockInitState
{
	FbWalBlockKey key;
	bool initialized;
	bool missing_noted;
} FbWalBlockInitState;

/*
 * FbWalPrefilterPattern
 *    Stores one WAL prefilter pattern.
 */

typedef struct FbWalPrefilterPattern
{
	char bytes[sizeof(RelFileLocator)];
	Size len;
	unsigned char skip[256];
} FbWalPrefilterPattern;

/*
 * FbWalSegmentPrefilterTask
 *    Describes one WAL segment prefilter task.
 */

typedef struct FbWalSegmentPrefilterTask
{
	FbWalSegmentEntry *segments;
	int *indexes;
	int index_count;
	const FbRelationInfo *info;
	int wal_seg_size;
	FbWalPrefilterPattern patterns[4];
	int pattern_count;
	bool *hit_map;
} FbWalSegmentPrefilterTask;

typedef struct FbWalSegmentValidateTask
{
	FbWalSegmentEntry *segments;
	int *indexes;
	int index_count;
	int wal_seg_size;
} FbWalSegmentValidateTask;

/*
 * FbWalVisitWindow
 *    Describes one WAL visit window.
 */

typedef struct FbWalVisitWindow
{
	FbWalSegmentEntry *segments;
	uint32 segment_count;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	XLogRecPtr read_end_lsn;
} FbWalVisitWindow;

typedef struct FbWalPayloadLocatorSegmentPlan
{
	FbWalSegmentEntry *segment;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
} FbWalPayloadLocatorSegmentPlan;

typedef struct FbPayloadLocatorPlanDebug
{
	uint32 base_segments;
	uint32 success_segments;
	uint32 empty_success_segments;
	uint32 segments_with_locators;
	uint32 fallback_segments;
	uint64 locator_records;
	uint32 base_by_source[FB_WAL_SOURCE_CKWAL + 1];
	uint32 success_by_source[FB_WAL_SOURCE_CKWAL + 1];
	uint32 fallback_by_source[FB_WAL_SOURCE_CKWAL + 1];
	uint32 locator_by_source[FB_WAL_SOURCE_CKWAL + 1];
	uint32 failed_sample_count;
	struct
	{
		char name[25];
		int source_kind;
	} failed_samples[8];
} FbPayloadLocatorPlanDebug;

/*
 * FbPrefilterCacheEntry
 *    Stores one prefilter cache entry.
 */

typedef struct FbPrefilterCacheEntry
{
	uint64 key;
	bool hit;
} FbPrefilterCacheEntry;

#define FB_PREFILTER_SIDECAR_MAGIC ((uint32) 0x46425046)
#define FB_PREFILTER_SIDECAR_VERSION 1
#define FB_CHECKPOINT_SIDECAR_MAGIC ((uint32) 0x46424350)
#define FB_CHECKPOINT_SIDECAR_VERSION 1
#define FB_WAL_PARALLEL_MAX_WORKERS 16
#define FB_WAL_PARALLEL_ERRMSG_MAX 512
#define FB_WAL_PARALLEL_GUC_MAX 64

typedef struct FbPrefilterSidecarFile
{
	uint32 magic;
	uint16 version;
	uint16 reserved;
	uint64 file_identity_hash;
	uint64 pattern_hash;
	bool hit;
	char padding[7];
} FbPrefilterSidecarFile;

typedef struct FbCheckpointSidecarHeader
{
	uint32 magic;
	uint16 version;
	uint16 reserved;
	uint64 file_identity_hash;
	TimeLineID timeline_id;
	uint32 reserved2;
	XLogSegNo segno;
	uint32 checkpoint_count;
	uint32 reserved3;
} FbCheckpointSidecarHeader;

typedef struct FbCheckpointSidecarEntry
{
	TimestampTz checkpoint_ts;
	XLogRecPtr checkpoint_lsn;
	XLogRecPtr redo_lsn;
} FbCheckpointSidecarEntry;

typedef struct FbCheckpointHintTask
{
	FbWalSegmentEntry *segments;
	int *indexes;
	int index_count;
	TimestampTz target_ts;
	char meta_dir[MAXPGPATH];
	uint32 checkpoint_entry_count;
	bool found;
	FbCheckpointSidecarEntry best_entry;
	uint32 best_segment_index;
} FbCheckpointHintTask;

typedef enum FbWalPayloadTaskStatus
{
	FB_WAL_PAYLOAD_TASK_INIT = 0,
	FB_WAL_PAYLOAD_TASK_RUNNING,
	FB_WAL_PAYLOAD_TASK_DONE,
	FB_WAL_PAYLOAD_TASK_ERROR
} FbWalPayloadTaskStatus;

typedef struct FbWalParallelWindow
{
	uint32 segment_start_index;
	uint32 segment_count;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	XLogRecPtr read_end_lsn;
} FbWalParallelWindow;

typedef struct FbWalPayloadTask
{
	int status;
	Oid dboid;
	Oid useroid;
	Oid relid;
	FbRelationInfo info;
	TimestampTz target_ts;
	XLogRecPtr anchor_redo_lsn;
	int window_start;
	int window_end;
	uint32 locator_start;
	uint32 locator_count;
	uint32 record_count;
	uint64 scanned_record_count;
	uint64 reader_reset_count;
	uint64 reader_reuse_count;
	char spool_path[MAXPGPATH];
	char archive_dest[MAXPGPATH];
	char archive_dir[MAXPGPATH];
	char debug_pg_wal_dir[MAXPGPATH];
	char memory_limit[FB_WAL_PARALLEL_GUC_MAX];
	char spill_mode[FB_WAL_PARALLEL_GUC_MAX];
	char show_progress[FB_WAL_PARALLEL_GUC_MAX];
	char errmsg[FB_WAL_PARALLEL_ERRMSG_MAX];
} FbWalPayloadTask;

typedef struct FbWalPayloadShared
{
	int task_count;
	int window_count;
	uint32 locator_count;
	TimeLineID timeline_id;
	int wal_seg_size;
	uint32 resolved_segment_count;
	Size tasks_offset;
	Size windows_offset;
	Size segments_offset;
	Size locators_offset;
	char data[FLEXIBLE_ARRAY_MEMBER];
} FbWalPayloadShared;

typedef struct FbWalMetadataCollectTask
{
	int status;
	Oid dboid;
	Oid useroid;
	Oid relid;
	TimestampTz target_ts;
	TimestampTz query_now_ts;
	int window_start;
	int window_end;
	uint64 total_record_count;
	uint32 touched_count;
	uint32 unsafe_count;
	uint32 xact_summary_count;
	bool anchor_found;
	XLogRecPtr anchor_checkpoint_lsn;
	XLogRecPtr anchor_redo_lsn;
	TimestampTz anchor_time;
	bool immediate_unsafe_found;
	FbWalUnsafeReason immediate_unsafe_reason;
	FbWalUnsafeScope immediate_unsafe_scope;
	FbWalStorageChangeOp immediate_unsafe_storage_op;
	XLogRecPtr immediate_unsafe_record_lsn;
	XLogRecPtr immediate_unsafe_trigger_lsn;
	char touched_path[MAXPGPATH];
	char unsafe_path[MAXPGPATH];
	char xact_summary_path[MAXPGPATH];
	char archive_dest[MAXPGPATH];
	char archive_dir[MAXPGPATH];
	char debug_pg_wal_dir[MAXPGPATH];
	char memory_limit[FB_WAL_PARALLEL_GUC_MAX];
	char spill_mode[FB_WAL_PARALLEL_GUC_MAX];
	char show_progress[FB_WAL_PARALLEL_GUC_MAX];
	char errmsg[FB_WAL_PARALLEL_ERRMSG_MAX];
} FbWalMetadataCollectTask;

typedef struct FbWalMetadataCollectShared
{
	int task_count;
	int window_count;
	TimeLineID timeline_id;
	int wal_seg_size;
	uint32 resolved_segment_count;
	Size tasks_offset;
	Size windows_offset;
	Size segments_offset;
	char data[FLEXIBLE_ARRAY_MEMBER];
} FbWalMetadataCollectShared;

typedef struct FbWalXactFillTask
{
	int status;
	Oid dboid;
	Oid useroid;
	Oid relid;
	TimestampTz target_ts;
	TimestampTz query_now_ts;
	int window_start;
	int window_end;
	uint32 status_count;
	uint64 target_commit_count;
	uint64 target_abort_count;
	bool unsafe_found;
	TransactionId unsafe_xid;
	TimestampTz unsafe_commit_ts;
	FbWalUnsafeReason unsafe_reason;
	FbWalUnsafeScope unsafe_scope;
	FbWalStorageChangeOp unsafe_storage_op;
	XLogRecPtr unsafe_record_lsn;
	XLogRecPtr unsafe_trigger_lsn;
	char status_path[MAXPGPATH];
	char archive_dest[MAXPGPATH];
	char archive_dir[MAXPGPATH];
	char debug_pg_wal_dir[MAXPGPATH];
	char memory_limit[FB_WAL_PARALLEL_GUC_MAX];
	char spill_mode[FB_WAL_PARALLEL_GUC_MAX];
	char show_progress[FB_WAL_PARALLEL_GUC_MAX];
	char errmsg[FB_WAL_PARALLEL_ERRMSG_MAX];
} FbWalXactFillTask;

typedef struct FbWalXactFillShared
{
	int task_count;
	int window_count;
	TimeLineID timeline_id;
	int wal_seg_size;
	uint32 resolved_segment_count;
	uint32 touched_xid_count;
	uint32 unsafe_count;
	Size tasks_offset;
	Size windows_offset;
	Size segments_offset;
	Size touched_xids_offset;
	Size unsafe_entries_offset;
	char data[FLEXIBLE_ARRAY_MEMBER];
} FbWalXactFillShared;

typedef struct FbWalXactScanVisitorState
{
	const TransactionId *touched_xids;
	uint32 touched_count;
	const FbUnsafeXidEntry *unsafe_entries;
	uint32 unsafe_count;
	FbSpoolLog *status_log;
	FbWalXactFillTask *task;
} FbWalXactScanVisitorState;

PGDLLEXPORT void fb_wal_payload_worker_main(Datum main_arg);
PGDLLEXPORT void fb_wal_metadata_collect_worker_main(Datum main_arg);
PGDLLEXPORT void fb_wal_xact_fill_worker_main(Datum main_arg);

/*
 * fb_mark_unsafe
 *    WAL helper.
 */

static void fb_mark_unsafe(FbWalScanContext *ctx, FbWalUnsafeReason reason);
static bool fb_unsafe_entry_requires_reject(const FbUnsafeXidEntry *entry);
static void fb_capture_unsafe_context(FbWalScanContext *ctx,
									  const FbUnsafeXidEntry *entry,
									  TransactionId xid,
									  TimestampTz commit_ts);
static void fb_mark_xid_touched(HTAB *touched_xids, TransactionId xid,
								FbWalScanContext *ctx);
static void fb_mark_xid_unsafe(HTAB *unsafe_xids, TransactionId xid,
							   FbWalUnsafeReason reason,
							   FbWalUnsafeScope scope,
							   FbWalStorageChangeOp storage_op,
							   XLogRecPtr lsn,
							   FbWalScanContext *ctx);
static FbUnsafeXidEntry *fb_find_unsafe_xid(HTAB *unsafe_xids, TransactionId xid);
/*
 * fb_record_xid
 *    WAL helper.
 */

static TransactionId fb_record_xid(XLogReaderState *reader);
/*
 * fb_locator_matches_relation
 *    WAL helper.
 */

static bool fb_locator_matches_relation(const RelFileLocator *locator,
									   const FbRelationInfo *info);
static FbWalUnsafeScope fb_relation_scope_from_locator(const RelFileLocator *locator,
													   const FbRelationInfo *info);
static FbWalUnsafeScope fb_relation_scope_from_relid(Oid relid,
													 const FbRelationInfo *info);
static FbWalSegmentEntry *fb_wal_prepare_segment(FbWalReaderPrivate *private,
												 XLogSegNo next_segno);
static void fb_wal_close_private_file(FbWalReaderPrivate *private);
static void fb_wal_reset_record_stats(FbWalRecordIndex *index);
static void fb_wal_finalize_record_stats(FbWalRecordIndex *index);
static HTAB *fb_wal_create_precomputed_missing_block_hash(void);
static HTAB *fb_wal_create_block_init_state_hash(void);
bool fb_wal_record_block_materializes_page(const FbRecordRef *record,
										   int block_index);
static bool fb_wal_record_block_requires_initialized_page(const FbRecordRef *record,
														  int block_index);
static void fb_wal_note_precomputed_missing_blocks(FbWalRecordIndex *index,
												   HTAB *block_states,
												   const FbRecordRef *record,
												   uint32 record_index);
static uint32 fb_wal_count_window_segments(const FbWalVisitWindow *windows,
										   uint32 window_count);
static void fb_maybe_activate_tail_payload_capture(XLogReaderState *reader,
												   FbWalIndexBuildState *state);
static void fb_index_note_materialized_record(FbWalRecordIndex *index,
											  FbRecordRef *record);
static void fb_append_xact_summary(XLogReaderState *reader,
								   FbWalIndexBuildState *state);
static uint32 fb_apply_xact_summary_entry(FbWalScanContext *ctx,
										  FbWalRecordIndex *index,
										  HTAB *touched_xids,
										  HTAB *unsafe_xids,
										  const char *data,
										  Size len);
static uint32 fb_wal_apply_xact_summary_log(FbWalScanContext *ctx,
											FbWalRecordIndex *index,
											HTAB *touched_xids,
											HTAB *unsafe_xids);
/*
 * fb_standby_record_matches_relation
 *    WAL helper.
 */

static bool fb_standby_record_matches_relation(XLogReaderState *reader,
								   const FbRelationInfo *info,
								   TransactionId *matched_xid,
								   FbWalUnsafeScope *matched_scope);
/*
 * fb_record_touches_relation
 *    WAL helper.
 */

static bool fb_record_touches_relation(XLogReaderState *reader,
									   const FbRelationInfo *info);
/*
 * fb_wal_source_name
 *    WAL helper.
 */

static const char *fb_wal_source_name(FbWalSourceKind source_kind);
/*
 * fb_validate_segment_identity
 *    WAL helper.
 */

static bool fb_validate_segment_identity(FbWalSegmentEntry *entry,
										 int wal_seg_size);
static bool fb_validate_segment_identity_path(const char *path,
											  TimeLineID timeline_id,
											  XLogSegNo segno,
											  int wal_seg_size);
static bool fb_read_segment_identity_path(const char *path,
										  TimeLineID *timeline_id,
										  XLogSegNo *segno,
										  int wal_seg_size);
static bool fb_segment_candidate_exists(FbWalSegmentEntry *segments,
										int segment_count,
										TimeLineID timeline_id,
										XLogSegNo segno,
										const FbWalSegmentEntry *exclude);
/*
 * fb_try_ckwal_segment
 *    WAL helper.
 */

static bool fb_try_ckwal_segment(FbWalScanContext *ctx,
								 TimeLineID timeline_id,
								 XLogSegNo segno,
								 int wal_seg_size,
								 FbWalSegmentEntry *entry);
/*
 * fb_record_is_speculative_insert
 *    WAL helper.
 */

static bool fb_record_is_speculative_insert(const FbRecordRef *record);
/*
 * fb_record_is_super_delete
 *    WAL helper.
 */

static bool fb_record_is_super_delete(const FbRecordRef *record);
static char *fb_copy_bytes(const char *data, Size len);
static void fb_record_release_temp(FbRecordRef *record);
static bool fb_record_ref_needs_payload_materialization(const FbRecordRef *record);
static void fb_wal_record_materializer_init(FbWalRecordMaterializer *materializer,
											 const FbWalRecordIndex *index);
static void fb_wal_record_materializer_destroy(FbWalRecordMaterializer *materializer);
static void fb_wal_record_materializer_note_usage(FbWalRecordMaterializer *materializer);
static void fb_wal_record_materializer_load_record(FbWalRecordMaterializer *materializer,
													XLogRecPtr record_lsn,
													FbRecordRef *record_out,
													bool include_payload);
static void fb_wal_load_record_by_lsn(const FbWalRecordIndex *index,
									  XLogRecPtr record_lsn,
									  FbRecordRef *record_out,
									  bool include_payload,
									  FbWalRecordMaterializer *materializer);
static void fb_fill_record_block_ref(FbRecordBlockRef *block_ref,
									 XLogReaderState *reader,
									 uint8 block_id,
									 const FbRelationInfo *info,
									 bool copy_payload);
static bool fb_heap_record_matches_target(XLogReaderState *reader,
										  const FbRelationInfo *info);
static bool fb_wal_build_heap_record_ref(XLogReaderState *reader,
										 const FbRelationInfo *info,
										 FbWalRecordKind kind,
										 FbRecordRef *record_out,
										 bool include_payload);
static bool fb_wal_build_xlog_fpi_record_ref(XLogReaderState *reader,
											 const FbRelationInfo *info,
											 FbWalRecordKind kind,
											 FbRecordRef *record_out,
											 bool include_payload);
/*
 * fb_segment_start_lsn
 *    WAL helper.
 */

static XLogRecPtr fb_segment_start_lsn(const FbWalSegmentEntry *entry,
									   int wal_seg_size);
/*
 * fb_segment_end_lsn
 *    WAL helper.
 */

static XLogRecPtr fb_segment_end_lsn(const FbWalSegmentEntry *entry,
									 int wal_seg_size);
static uint64 fb_sidecar_file_identity_hash(const char *path,
											 off_t bytes,
											 time_t mtime);
static bool fb_prefilter_sidecar_load(const char *cache_path,
										 uint64 file_identity_hash,
										 uint64 pattern_hash,
										 bool *hit);
static void fb_prefilter_sidecar_store(const char *cache_path,
										 uint64 file_identity_hash,
										 uint64 pattern_hash,
										 bool hit);
static bool fb_checkpoint_sidecar_load(const FbWalSegmentEntry *entry,
										  const char *meta_dir,
										  FbCheckpointSidecarEntry **entries_out,
										  uint32 *entry_count_out);
static void fb_checkpoint_sidecar_append(const FbWalSegmentEntry *entry,
										   TimestampTz checkpoint_ts,
										   XLogRecPtr checkpoint_lsn,
										   XLogRecPtr redo_lsn);
static void fb_maybe_seed_anchor_hint(FbWalScanContext *ctx);
static bool fb_wal_anchor_probe_visitor(XLogReaderState *reader, void *arg);
static void fb_wal_raise_missing_anchor_error(const FbWalScanContext *ctx,
											  bool saw_checkpoint,
											  TimestampTz first_checkpoint_ts);
static void fb_wal_probe_anchor_coverage(FbWalScanContext *ctx);
static FbWalPayloadTask *fb_wal_payload_tasks(FbWalPayloadShared *shared);
static FbWalParallelWindow *fb_wal_payload_windows(FbWalPayloadShared *shared);
static FbWalSegmentEntry *fb_wal_payload_segments(FbWalPayloadShared *shared);
static FbSummaryPayloadLocator *fb_wal_payload_locators_shared(FbWalPayloadShared *shared);
static FbWalMetadataCollectTask *fb_wal_metadata_collect_tasks(FbWalMetadataCollectShared *shared);
static FbWalParallelWindow *fb_wal_metadata_collect_windows(FbWalMetadataCollectShared *shared);
static FbWalSegmentEntry *fb_wal_metadata_collect_segments(FbWalMetadataCollectShared *shared);
static FbWalXactFillTask *fb_wal_xact_fill_tasks(FbWalXactFillShared *shared);
static FbWalParallelWindow *fb_wal_xact_fill_windows(FbWalXactFillShared *shared);
static FbWalSegmentEntry *fb_wal_xact_fill_segments(FbWalXactFillShared *shared);
static TransactionId *fb_wal_xact_fill_touched_xids(FbWalXactFillShared *shared);
static FbUnsafeXidEntry *fb_wal_xact_fill_unsafe_entries(FbWalXactFillShared *shared);
static void fb_wal_payload_copy_setting(char *dst, Size dstlen, const char *name);
static void fb_wal_payload_capture_gucs(FbWalPayloadTask *task);
static void fb_wal_payload_apply_gucs(const FbWalPayloadTask *task);
static void fb_wal_payload_fill_task(FbWalPayloadTask *task,
									 const FbRelationInfo *info,
									 TimestampTz target_ts,
									 XLogRecPtr anchor_redo_lsn,
									 int window_start,
									 int window_end,
									 uint32 locator_start,
									 uint32 locator_count,
									 const char *spool_path);
static bool fb_wal_payload_launch_worker(dsm_handle handle,
										 int task_index,
										 BackgroundWorkerHandle **handle_out);
static void fb_wal_payload_wait_worker(BackgroundWorkerHandle *handle,
									   const FbWalPayloadTask *task);
static void fb_wal_payload_merge_log(FbWalRecordIndex *index,
									 const char *path,
									 uint32 item_count);
static void fb_wal_metadata_collect_capture_gucs(FbWalMetadataCollectTask *task);
static void fb_wal_metadata_collect_apply_gucs(const FbWalMetadataCollectTask *task);
static void fb_wal_metadata_collect_fill_task(FbWalMetadataCollectTask *task,
											  const FbRelationInfo *info,
											  TimestampTz target_ts,
											  TimestampTz query_now_ts,
											  int window_start,
											  int window_end,
											  const char *touched_path,
											  const char *unsafe_path,
											  const char *xact_summary_path);
static void fb_wal_metadata_collect_launch_worker(dsm_handle handle,
												  int task_index,
												  BackgroundWorkerHandle **handle_out);
static void fb_wal_metadata_collect_wait_worker(BackgroundWorkerHandle *handle,
												const FbWalMetadataCollectTask *task);
static void fb_wal_metadata_dump_touched_xids(HTAB *touched_xids,
											  const char *path,
											  uint32 *count_out);
static void fb_wal_metadata_dump_unsafe_xids(HTAB *unsafe_xids,
											 const char *path,
											 uint32 *count_out);
static void fb_wal_metadata_merge_touched_xids(FbWalScanContext *ctx,
											   HTAB *touched_xids,
											   const char *path,
											   uint32 item_count);
static void fb_wal_metadata_merge_unsafe_xids(FbWalScanContext *ctx,
											  HTAB *unsafe_xids,
											  const char *path,
											  uint32 item_count);
static bool fb_wal_collect_metadata_parallel(const FbRelationInfo *info,
											 FbWalScanContext *ctx,
											 FbWalRecordIndex *index,
											 FbWalIndexBuildState *state,
											 const FbWalVisitWindow *windows,
											 uint32 window_count);
static void fb_wal_xact_fill_capture_gucs(FbWalXactFillTask *task);
static void fb_wal_xact_fill_apply_gucs(const FbWalXactFillTask *task);
static void fb_wal_xact_fill_fill_task(FbWalXactFillTask *task,
									   const FbRelationInfo *info,
									   TimestampTz target_ts,
									   TimestampTz query_now_ts,
									   int window_start,
									   int window_end,
									   const char *status_path);
static bool fb_wal_xact_fill_launch_worker(dsm_handle handle,
										   int task_index,
										   BackgroundWorkerHandle **handle_out);
static void fb_wal_xact_fill_wait_worker(BackgroundWorkerHandle *handle,
										 const FbWalXactFillTask *task);
static void fb_wal_xact_merge_statuses(FbWalRecordIndex *index,
									   const char *path,
									   uint32 item_count);
static bool fb_wal_fill_xact_statuses_parallel(const FbRelationInfo *info,
											   FbWalScanContext *ctx,
											   FbWalRecordIndex *index,
											   const FbWalVisitWindow *windows,
											   uint32 window_count,
											   HTAB *touched_xids,
											   HTAB *unsafe_xids);
static bool fb_wal_xact_fill_visitor(XLogReaderState *reader, void *arg);
static bool fb_wal_materialize_payload_parallel(const FbRelationInfo *info,
									FbWalScanContext *ctx,
									FbWalRecordIndex *index,
									const FbWalVisitWindow *windows,
									uint32 window_count);
static bool fb_wal_materialize_payload_locators_parallel(const FbRelationInfo *info,
														 FbWalScanContext *ctx,
														 FbWalRecordIndex *index,
														 const FbWalVisitWindow *windows,
														 uint32 window_count,
														 const FbSummaryPayloadLocator *locators,
														 uint32 locator_count);
static bool fb_wal_serial_xact_fill_visitor(XLogReaderState *reader, void *arg);
static void fb_wal_fill_xact_statuses_serial(const FbRelationInfo *info,
											 FbWalScanContext *ctx,
											 FbWalRecordIndex *index,
											 const FbWalVisitWindow *windows,
											 uint32 window_count,
											 HTAB *touched_xids,
											 HTAB *unsafe_xids);
static int fb_transactionid_cmp(const void *lhs, const void *rhs);
static int fb_unsafe_xid_entry_cmp(const void *lhs, const void *rhs);
/*
 * fb_prefilter_hash_bytes
 *    WAL helper.
 */

static uint64 fb_prefilter_hash_bytes(uint64 seed, const void *data, Size len);
/*
 * fb_prefilter_patterns_hash
 *    WAL helper.
 */

static uint64 fb_prefilter_patterns_hash(FbWalPrefilterPattern *patterns,
										 int pattern_count);
/*
 * fb_prefilter_cache_key
 *    WAL helper.
 */

static uint64 fb_prefilter_cache_key(const FbWalSegmentEntry *entry,
									 uint64 pattern_hash);
/*
 * fb_prefilter_cache_lookup_memory
 *    WAL helper.
 */

static bool fb_prefilter_cache_lookup_memory(uint64 key, bool *hit);
/*
 * fb_prefilter_cache_store_memory
 *    WAL helper.
 */

static void fb_prefilter_cache_store_memory(uint64 key, bool hit);
static int fb_parallel_worker_count(int task_count);
/*
 * fb_prepare_segment_prefilter
 *    WAL helper.
 */

static void fb_prepare_segment_prefilter(const FbRelationInfo *info,
										 FbWalScanContext *ctx);
static uint32 fb_build_summary_span_visit_windows(const FbRelationInfo *info,
												  FbWalScanContext *ctx,
												  const FbWalVisitWindow *base_windows,
												  uint32 base_window_count,
												  FbWalVisitWindow **windows_out);
static int fb_visit_window_cmp(const void *lhs, const void *rhs);
static uint32 fb_merge_visit_windows(FbWalScanContext *ctx,
									 FbWalVisitWindow **windows_io,
									 uint32 window_count);
static uint32 fb_payload_locator_deduplicate_by_lsn(FbSummaryPayloadLocator *locators,
													uint32 count);
static uint32 fb_build_xact_fallback_visit_windows(FbWalScanContext *ctx,
												   const FbWalVisitWindow *base_windows,
												   uint32 base_window_count,
												   FbWalVisitWindow **windows_out);
static uint32 fb_coalesce_payload_visit_windows(FbWalScanContext *ctx,
												FbWalVisitWindow **windows_io,
												uint32 window_count);
static uint32 fb_summary_seed_metadata_from_summary(const FbRelationInfo *info,
													FbWalScanContext *ctx,
													const FbWalVisitWindow *windows,
													uint32 window_count,
													HTAB *touched_xids,
													HTAB *unsafe_xids,
													FbWalVisitWindow **fallback_windows_out);
static bool fb_summary_fill_xact_statuses(const FbRelationInfo *info,
										  FbWalScanContext *ctx,
										  FbWalRecordIndex *index,
										  const FbWalVisitWindow *windows,
										  uint32 window_count,
										  HTAB *touched_xids,
										  HTAB *unsafe_xids);
static uint32 fb_apply_summary_xid_outcome(FbWalScanContext *ctx,
										   FbWalRecordIndex *index,
										   HTAB *touched_xids,
										   HTAB *unsafe_xids,
										   const FbSummaryXidOutcome *outcome);
static bool fb_summary_fill_exact_xact_statuses(FbWalScanContext *ctx,
												FbWalRecordIndex *index,
												const FbWalVisitWindow *windows,
												uint32 window_count,
												HTAB *touched_xids,
												HTAB *unsafe_xids);
static void fb_build_unresolved_xid_fallback_sets(FbWalRecordIndex *index,
												  HTAB *touched_xids,
												  HTAB *unsafe_xids,
												  HTAB **fallback_touched_out,
												  HTAB **fallback_unsafe_out);
static uint32 fb_build_materialize_visit_windows(FbWalScanContext *ctx,
												 const FbWalVisitWindow *base_windows,
												 uint32 base_window_count,
												 XLogRecPtr start_lsn,
												 XLogRecPtr end_lsn,
												 FbWalVisitWindow **windows_out);
static uint32 fb_build_payload_locator_visit_windows(FbWalScanContext *ctx,
													 const FbSummaryPayloadLocator *locators,
													 uint32 locator_count,
													 FbWalVisitWindow **windows_out);
static uint32 fb_build_summary_payload_locator_plan(const FbRelationInfo *info,
													FbWalScanContext *ctx,
													const FbWalVisitWindow *base_windows,
													uint32 base_window_count,
													XLogRecPtr start_lsn,
													XLogRecPtr end_lsn,
													FbSummaryPayloadLocator **locators_out,
													FbWalVisitWindow **fallback_windows_out,
													uint32 *fallback_window_count_out,
													uint32 *fallback_segment_count_out,
													uint32 *segments_read_out,
													FbPayloadLocatorPlanDebug *plan_debug);
static FbWalVisitWindow *fb_copy_visit_windows(const FbWalVisitWindow *windows,
											   uint32 window_count);
static uint32 fb_count_payload_locator_segments(FbWalScanContext *ctx,
												const FbSummaryPayloadLocator *locators,
												uint32 locator_count);
static bool fb_should_use_sparse_payload_scan(uint32 sparse_window_count,
											  uint32 windowed_window_count,
											  uint32 covered_segment_count);
static void fb_wal_prepare_payload_read_window(FbWalScanContext *ctx,
												 const FbWalVisitWindow *window,
												 const FbWalSegmentEntry *all_segments,
												 uint32 all_segment_count,
												 FbWalVisitWindow *read_window);
static uint32 fb_wal_visit_payload_locators(FbWalScanContext *ctx,
											const FbSummaryPayloadLocator *locators,
											uint32 locator_count,
											FbWalIndexBuildState *state);
static bool fb_wal_visit_windows_share_segment_slice(const FbWalVisitWindow *lhs,
													 const FbWalVisitWindow *rhs);
static uint32 fb_split_parallel_materialize_windows(FbWalScanContext *ctx,
													   FbWalVisitWindow **windows_io,
													   uint32 window_count);
static int fb_find_segment_index_for_lsn(FbWalSegmentEntry *segments,
										 int segment_count,
										 XLogRecPtr lsn,
										 int wal_seg_size);
static void fb_wal_enable_incremental_payload_stats(FbWalRecordIndex *index,
													FbWalIndexBuildState *state);
#if PG_VERSION_NUM >= 130000
static void fb_wal_open_segment(XLogReaderState *state, XLogSegNo next_segno,
								TimeLineID *timeline_id);
static void fb_wal_close_segment(XLogReaderState *state);
#endif
#if PG_VERSION_NUM < 130000
static int fb_wal_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr,
							int req_len, XLogRecPtr target_rec_ptr,
							char *read_buf, TimeLineID *page_tli);
#else
static int fb_wal_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr,
							int req_len, XLogRecPtr target_rec_ptr,
							char *read_buf);
#endif
/*
 * fb_wal_visit_window
 *    WAL helper.
 */

static void fb_wal_visit_window(FbWalScanContext *ctx,
								const FbWalVisitWindow *window,
								FbWalRecordVisitor visitor,
								void *arg);
static void fb_wal_visit_sparse_windows(FbWalScanContext *ctx,
										const FbWalVisitWindow *windows,
										uint32 window_count,
										FbWalIndexBuildState *state);
static void fb_wal_visit_window_batch(FbWalScanContext *ctx,
									  const FbWalVisitWindow *windows,
									  uint32 window_count,
									  FbWalIndexBuildState *state);

/*
 * fb_parse_timeline_id
 *    WAL helper.
 */

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

/*
 * fb_segment_name_cmp
 *    WAL helper.
 */

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

/*
 * fb_wal_source_name
 *    WAL helper.
 */

static const char *
fb_wal_source_name(FbWalSourceKind source_kind)
{
	switch (source_kind)
	{
		case FB_WAL_SOURCE_PG_WAL:
			return "pg_wal";
		case FB_WAL_SOURCE_ARCHIVE_DEST:
			return "archive_dest";
		case FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY:
			return "archive_dir";
		case FB_WAL_SOURCE_CKWAL:
			return "ckwal";
	}

	return "unknown";
}

/*
 * fb_index_charge_bytes
 *    WAL helper.
 */

/*
 * fb_prefilter_pattern_init
 *    WAL helper.
 */

static void
fb_prefilter_pattern_init(FbWalPrefilterPattern *pattern)
{
	Size i;

	if (pattern == NULL)
		return;

	for (i = 0; i < lengthof(pattern->skip); i++)
		pattern->skip[i] = (unsigned char) pattern->len;

	if (pattern->len <= 1)
		return;

	for (i = 0; i < pattern->len - 1; i++)
		pattern->skip[(unsigned char) pattern->bytes[i]] =
			(unsigned char) (pattern->len - 1 - i);
}

/*
 * fb_prefilter_hash_bytes
 *    WAL helper.
 */

static uint64
fb_prefilter_hash_bytes(uint64 seed, const void *data, Size len)
{
	const unsigned char *ptr = (const unsigned char *) data;
	Size i;

	for (i = 0; i < len; i++)
	{
		seed ^= (uint64) ptr[i];
		seed *= UINT64CONST(1099511628211);
	}

	return seed;
}

/*
 * fb_prefilter_patterns_hash
 *    WAL helper.
 */

static uint64
fb_prefilter_patterns_hash(FbWalPrefilterPattern *patterns, int pattern_count)
{
	uint64 hash = UINT64CONST(1469598103934665603);
	int i;

	for (i = 0; i < pattern_count; i++)
	{
		hash = fb_prefilter_hash_bytes(hash, patterns[i].bytes, patterns[i].len);
		hash = fb_prefilter_hash_bytes(hash, &patterns[i].len, sizeof(patterns[i].len));
	}

	return hash;
}

/*
 * fb_get_prefilter_cache
 *    WAL helper.
 */

static HTAB *
fb_get_prefilter_cache(void)
{
	static HTAB *cache = NULL;

	if (cache == NULL)
	{
		HASHCTL ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64);
		ctl.entrysize = sizeof(FbPrefilterCacheEntry);
		cache = hash_create("fb segment prefilter cache",
							1024,
							&ctl,
							HASH_ELEM | HASH_BLOBS);
	}

	return cache;
}

/*
 * fb_prefilter_cache_key
 *    WAL helper.
 */

static uint64
fb_prefilter_cache_key(const FbWalSegmentEntry *entry, uint64 pattern_hash)
{
	uint64 hash = UINT64CONST(1469598103934665603);

	if (entry == NULL)
		return hash;

	hash = fb_prefilter_hash_bytes(hash, entry->path, strlen(entry->path));
	hash = fb_prefilter_hash_bytes(hash, &entry->bytes, sizeof(entry->bytes));
	hash = fb_prefilter_hash_bytes(hash, &pattern_hash, sizeof(pattern_hash));
	return hash;
}

/*
 * fb_prefilter_cache_lookup_memory
 *    WAL helper.
 */

static bool
fb_prefilter_cache_lookup_memory(uint64 key, bool *hit)
{
	FbPrefilterCacheEntry *entry;

	entry = (FbPrefilterCacheEntry *) hash_search(fb_get_prefilter_cache(),
												  &key,
												  HASH_FIND,
												  NULL);
	if (entry == NULL)
		return false;

	if (hit != NULL)
		*hit = entry->hit;
	return true;
}

/*
 * fb_prefilter_cache_store_memory
 *    WAL helper.
 */

static void
fb_prefilter_cache_store_memory(uint64 key, bool hit)
{
	FbPrefilterCacheEntry *entry;

	entry = (FbPrefilterCacheEntry *) hash_search(fb_get_prefilter_cache(),
												  &key,
												  HASH_ENTER,
												  NULL);
	entry->hit = hit;
}

static int
fb_config_int_or_default(const char *name, int default_value)
{
	char   *value;

	value = GetConfigOptionByName(name, NULL, false);
	if (value == NULL || value[0] == '\0')
		return default_value;

	return atoi(value);
}

static bool
fb_shared_preload_has_pg_flashback(void)
{
	char   *value;
	char   *token;

	value = GetConfigOptionByName("shared_preload_libraries", NULL, false);
	if (value == NULL || value[0] == '\0')
		return false;

	token = value;
	while (token != NULL && token[0] != '\0')
	{
		char   *comma = strchr(token, ',');
		size_t	len = (comma != NULL) ? (size_t) (comma - token) : strlen(token);

		while (len > 0 && *token == ' ')
		{
			token++;
			len--;
		}
		while (len > 0 && token[len - 1] == ' ')
			len--;
		if (len == strlen("pg_flashback") &&
			strncmp(token, "pg_flashback", len) == 0)
			return true;
		if (comma == NULL)
			break;
		token = comma + 1;
	}

	return false;
}

static int
fb_parallel_query_worker_budget(void)
{
	int max_workers = fb_config_int_or_default("max_worker_processes", 8);
	int io_workers = fb_config_int_or_default("io_workers", 0);
	int reserve = 1; /* logical replication launcher */

	/*
	 * Query-local WAL workers compete with IO workers and the summary
	 * prebuild service for the same postmaster worker pool. Account for the
	 * static occupants up front so we don't repeatedly register, kill, and
	 * retry workers that can never fit in the current instance budget.
	 */
	if (io_workers > 0)
		reserve += io_workers;
	if (fb_summary_service_enabled() &&
		fb_shared_preload_has_pg_flashback())
	{
		int summary_workers = fb_summary_service_workers();
		int summary_allowed = max_workers - 5;

		if (summary_allowed < 1)
			summary_allowed = 1;
		summary_workers = Min(summary_workers, summary_allowed);
		reserve += 1 + summary_workers; /* launcher + registered workers */
	}

	reserve += 1; /* leave one slot for unrelated dynamic workers */
	max_workers -= reserve;
	if (max_workers < 1)
		max_workers = 1;
	return max_workers;
}

static int
fb_parallel_worker_count(int task_count)
{
	int configured_workers;
	int max_workers;

	if (task_count <= 1)
		return 1;

	configured_workers = fb_parallel_workers();
	if (configured_workers <= 0)
		return 1;

	max_workers = Min(configured_workers, FB_WAL_PARALLEL_MAX_WORKERS);
	max_workers = Min(max_workers, fb_parallel_query_worker_budget());
	if (max_workers <= 0)
		max_workers = 1;

	return Min(max_workers, task_count);
}

static int
fb_transactionid_cmp(const void *lhs, const void *rhs)
{
	TransactionId left = *((const TransactionId *) lhs);
	TransactionId right = *((const TransactionId *) rhs);

	if (left < right)
		return -1;
	if (left > right)
		return 1;
	return 0;
}

static int
fb_unsafe_xid_entry_cmp(const void *lhs, const void *rhs)
{
	const FbUnsafeXidEntry *left = (const FbUnsafeXidEntry *) lhs;
	const FbUnsafeXidEntry *right = (const FbUnsafeXidEntry *) rhs;

	return fb_transactionid_cmp(&left->xid, &right->xid);
}

static FbWalPayloadTask *
fb_wal_payload_tasks(FbWalPayloadShared *shared)
{
	return (FbWalPayloadTask *) ((char *) shared + shared->tasks_offset);
}

static FbWalParallelWindow *
fb_wal_payload_windows(FbWalPayloadShared *shared)
{
	return (FbWalParallelWindow *) ((char *) shared + shared->windows_offset);
}

static FbWalSegmentEntry *
fb_wal_payload_segments(FbWalPayloadShared *shared)
{
	return (FbWalSegmentEntry *) ((char *) shared + shared->segments_offset);
}

static FbSummaryPayloadLocator *
fb_wal_payload_locators_shared(FbWalPayloadShared *shared)
{
	return (FbSummaryPayloadLocator *) ((char *) shared + shared->locators_offset);
}

static FbWalMetadataCollectTask *
fb_wal_metadata_collect_tasks(FbWalMetadataCollectShared *shared)
{
	return (FbWalMetadataCollectTask *) ((char *) shared + shared->tasks_offset);
}

static FbWalParallelWindow *
fb_wal_metadata_collect_windows(FbWalMetadataCollectShared *shared)
{
	return (FbWalParallelWindow *) ((char *) shared + shared->windows_offset);
}

static FbWalSegmentEntry *
fb_wal_metadata_collect_segments(FbWalMetadataCollectShared *shared)
{
	return (FbWalSegmentEntry *) ((char *) shared + shared->segments_offset);
}

static FbWalXactFillTask *
fb_wal_xact_fill_tasks(FbWalXactFillShared *shared)
{
	return (FbWalXactFillTask *) ((char *) shared + shared->tasks_offset);
}

static FbWalParallelWindow *
fb_wal_xact_fill_windows(FbWalXactFillShared *shared)
{
	return (FbWalParallelWindow *) ((char *) shared + shared->windows_offset);
}

static FbWalSegmentEntry *
fb_wal_xact_fill_segments(FbWalXactFillShared *shared)
{
	return (FbWalSegmentEntry *) ((char *) shared + shared->segments_offset);
}

static TransactionId *
fb_wal_xact_fill_touched_xids(FbWalXactFillShared *shared)
{
	return (TransactionId *) ((char *) shared + shared->touched_xids_offset);
}

static FbUnsafeXidEntry *
fb_wal_xact_fill_unsafe_entries(FbWalXactFillShared *shared)
{
	return (FbUnsafeXidEntry *) ((char *) shared + shared->unsafe_entries_offset);
}

static void
fb_wal_payload_copy_setting(char *dst, Size dstlen, const char *name)
{
	char *value;

	value = GetConfigOptionByName(name, NULL, false);
	if (value == NULL)
		elog(ERROR, "could not read GUC \"%s\" for WAL payload worker", name);
	if (strlen(value) >= dstlen)
		ereport(ERROR,
				(errcode(ERRCODE_NAME_TOO_LONG),
				 errmsg("GUC \"%s\" is too long for WAL payload worker state", name)));

	strlcpy(dst, value, dstlen);
}

static void
fb_wal_payload_capture_gucs(FbWalPayloadTask *task)
{
	fb_wal_payload_copy_setting(task->archive_dest,
								sizeof(task->archive_dest),
								"pg_flashback.archive_dest");
	fb_wal_payload_copy_setting(task->archive_dir,
								sizeof(task->archive_dir),
								"pg_flashback.archive_dir");
	fb_wal_payload_copy_setting(task->debug_pg_wal_dir,
								sizeof(task->debug_pg_wal_dir),
								"pg_flashback.debug_pg_wal_dir");
	fb_wal_payload_copy_setting(task->memory_limit,
								sizeof(task->memory_limit),
								"pg_flashback.memory_limit");
	fb_wal_payload_copy_setting(task->spill_mode,
								sizeof(task->spill_mode),
								"pg_flashback.spill_mode");
	fb_wal_payload_copy_setting(task->show_progress,
								sizeof(task->show_progress),
								"pg_flashback.show_progress");
}

static void
fb_wal_payload_apply_gucs(const FbWalPayloadTask *task)
{
	SetConfigOption("pg_flashback.archive_dest",
					task->archive_dest,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.archive_dir",
					task->archive_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.debug_pg_wal_dir",
					task->debug_pg_wal_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.memory_limit",
					task->memory_limit,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.spill_mode",
					task->spill_mode,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.show_progress",
					"off",
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.parallel_workers",
					"0",
					PGC_USERSET,
					PGC_S_SESSION);
}

static void
fb_wal_payload_fill_task(FbWalPayloadTask *task,
						 const FbRelationInfo *info,
						 TimestampTz target_ts,
						 XLogRecPtr anchor_redo_lsn,
						 int window_start,
						 int window_end,
						 uint32 locator_start,
						 uint32 locator_count,
						 const char *spool_path)
{
	if (task == NULL || info == NULL || spool_path == NULL)
		return;

	MemSet(task, 0, sizeof(*task));
	task->status = FB_WAL_PAYLOAD_TASK_INIT;
	task->dboid = MyDatabaseId;
	task->useroid = GetUserId();
	task->relid = info->relid;
	task->info = *info;
	task->info.mode_name = NULL;
	task->target_ts = target_ts;
	task->anchor_redo_lsn = anchor_redo_lsn;
	task->window_start = window_start;
	task->window_end = window_end;
	task->locator_start = locator_start;
	task->locator_count = locator_count;
	strlcpy(task->spool_path, spool_path, sizeof(task->spool_path));
	fb_wal_payload_capture_gucs(task);
}

static void
fb_wal_metadata_collect_capture_gucs(FbWalMetadataCollectTask *task)
{
	fb_wal_payload_copy_setting(task->archive_dest,
								sizeof(task->archive_dest),
								"pg_flashback.archive_dest");
	fb_wal_payload_copy_setting(task->archive_dir,
								sizeof(task->archive_dir),
								"pg_flashback.archive_dir");
	fb_wal_payload_copy_setting(task->debug_pg_wal_dir,
								sizeof(task->debug_pg_wal_dir),
								"pg_flashback.debug_pg_wal_dir");
	fb_wal_payload_copy_setting(task->memory_limit,
								sizeof(task->memory_limit),
								"pg_flashback.memory_limit");
	fb_wal_payload_copy_setting(task->spill_mode,
								sizeof(task->spill_mode),
								"pg_flashback.spill_mode");
	fb_wal_payload_copy_setting(task->show_progress,
								sizeof(task->show_progress),
								"pg_flashback.show_progress");
}

static void
fb_wal_metadata_collect_apply_gucs(const FbWalMetadataCollectTask *task)
{
	SetConfigOption("pg_flashback.archive_dest",
					task->archive_dest,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.archive_dir",
					task->archive_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.debug_pg_wal_dir",
					task->debug_pg_wal_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.memory_limit",
					task->memory_limit,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.spill_mode",
					task->spill_mode,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.show_progress",
					"off",
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.parallel_workers",
					"0",
					PGC_USERSET,
					PGC_S_SESSION);
}

static void
fb_wal_metadata_collect_fill_task(FbWalMetadataCollectTask *task,
								  const FbRelationInfo *info,
								  TimestampTz target_ts,
								  TimestampTz query_now_ts,
								  int window_start,
								  int window_end,
								  const char *touched_path,
								  const char *unsafe_path,
								  const char *xact_summary_path)
{
	if (task == NULL || info == NULL || touched_path == NULL ||
		unsafe_path == NULL || xact_summary_path == NULL)
		return;

	MemSet(task, 0, sizeof(*task));
	task->status = FB_WAL_PAYLOAD_TASK_INIT;
	task->dboid = MyDatabaseId;
	task->useroid = GetUserId();
	task->relid = info->relid;
	task->target_ts = target_ts;
	task->query_now_ts = query_now_ts;
	task->window_start = window_start;
	task->window_end = window_end;
	strlcpy(task->touched_path, touched_path, sizeof(task->touched_path));
	strlcpy(task->unsafe_path, unsafe_path, sizeof(task->unsafe_path));
	strlcpy(task->xact_summary_path, xact_summary_path,
			sizeof(task->xact_summary_path));
	fb_wal_metadata_collect_capture_gucs(task);
}

static void
fb_wal_xact_fill_capture_gucs(FbWalXactFillTask *task)
{
	fb_wal_payload_copy_setting(task->archive_dest,
								sizeof(task->archive_dest),
								"pg_flashback.archive_dest");
	fb_wal_payload_copy_setting(task->archive_dir,
								sizeof(task->archive_dir),
								"pg_flashback.archive_dir");
	fb_wal_payload_copy_setting(task->debug_pg_wal_dir,
								sizeof(task->debug_pg_wal_dir),
								"pg_flashback.debug_pg_wal_dir");
	fb_wal_payload_copy_setting(task->memory_limit,
								sizeof(task->memory_limit),
								"pg_flashback.memory_limit");
	fb_wal_payload_copy_setting(task->spill_mode,
								sizeof(task->spill_mode),
								"pg_flashback.spill_mode");
	fb_wal_payload_copy_setting(task->show_progress,
								sizeof(task->show_progress),
								"pg_flashback.show_progress");
}

static void
fb_wal_xact_fill_apply_gucs(const FbWalXactFillTask *task)
{
	SetConfigOption("pg_flashback.archive_dest",
					task->archive_dest,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.archive_dir",
					task->archive_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.debug_pg_wal_dir",
					task->debug_pg_wal_dir,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.memory_limit",
					task->memory_limit,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.spill_mode",
					task->spill_mode,
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.show_progress",
					"off",
					PGC_USERSET,
					PGC_S_SESSION);
	SetConfigOption("pg_flashback.parallel_workers",
					"0",
					PGC_USERSET,
					PGC_S_SESSION);
}

static void
fb_wal_xact_fill_fill_task(FbWalXactFillTask *task,
						   const FbRelationInfo *info,
						   TimestampTz target_ts,
						   TimestampTz query_now_ts,
						   int window_start,
						   int window_end,
						   const char *status_path)
{
	if (task == NULL || info == NULL || status_path == NULL)
		return;

	MemSet(task, 0, sizeof(*task));
	task->status = FB_WAL_PAYLOAD_TASK_INIT;
	task->dboid = MyDatabaseId;
	task->useroid = GetUserId();
	task->relid = info->relid;
	task->target_ts = target_ts;
	task->query_now_ts = query_now_ts;
	task->window_start = window_start;
	task->window_end = window_end;
	strlcpy(task->status_path, status_path, sizeof(task->status_path));
	fb_wal_xact_fill_capture_gucs(task);
}

static uint64
fb_sidecar_file_identity_hash(const char *path, off_t bytes, time_t mtime)
{
	uint64 hash = UINT64CONST(1469598103934665603);

	hash = fb_prefilter_hash_bytes(hash, path, strlen(path));
	hash = fb_prefilter_hash_bytes(hash, &bytes, sizeof(bytes));
	hash = fb_prefilter_hash_bytes(hash, &mtime, sizeof(mtime));
	return hash;
}

static bool
fb_sidecar_read_bytes(int fd, void *buf, Size len)
{
	char   *pos = (char *) buf;
	Size	remaining = len;

	while (remaining > 0)
	{
		ssize_t bytes_read = read(fd, pos, remaining);

		if (bytes_read <= 0)
			return false;
		pos += bytes_read;
		remaining -= (Size) bytes_read;
	}

	return true;
}

static bool
fb_sidecar_write_bytes(int fd, const void *buf, Size len)
{
	const char *pos = (const char *) buf;
	Size		remaining = len;

	while (remaining > 0)
	{
		ssize_t bytes_written = write(fd, pos, remaining);

		if (bytes_written <= 0)
			return false;
		pos += bytes_written;
		remaining -= (Size) bytes_written;
	}

	return true;
}

static bool
fb_prefilter_sidecar_load(const char *cache_path,
						  uint64 file_identity_hash,
						  uint64 pattern_hash,
						  bool *hit)
{
	FbPrefilterSidecarFile file;
	int			fd;

	if (hit != NULL)
		*hit = false;
	if (cache_path == NULL || cache_path[0] == '\0' || hit == NULL)
		return false;

	fd = open(cache_path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;

	if (!fb_sidecar_read_bytes(fd, &file, sizeof(file)))
	{
		close(fd);
		return false;
	}

	close(fd);

	if (file.magic != FB_PREFILTER_SIDECAR_MAGIC ||
		file.version != FB_PREFILTER_SIDECAR_VERSION ||
		file.file_identity_hash != file_identity_hash ||
		file.pattern_hash != pattern_hash)
		return false;

	*hit = file.hit;
	return true;
}

static void
fb_prefilter_sidecar_store(const char *cache_path,
						   uint64 file_identity_hash,
						   uint64 pattern_hash,
						   bool hit)
{
	FbPrefilterSidecarFile file;
	int			fd;

	if (cache_path == NULL || cache_path[0] == '\0')
		return;

	MemSet(&file, 0, sizeof(file));
	file.magic = FB_PREFILTER_SIDECAR_MAGIC;
	file.version = FB_PREFILTER_SIDECAR_VERSION;
	file.file_identity_hash = file_identity_hash;
	file.pattern_hash = pattern_hash;
	file.hit = hit;

	fd = open(cache_path,
			  O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
			  S_IRUSR | S_IWUSR);
	if (fd < 0)
		return;

	(void) fb_sidecar_write_bytes(fd, &file, sizeof(file));
	close(fd);
}

/*
 * fb_bytes_contains_pattern
 *    WAL helper.
 */

static bool
fb_bytes_contains_pattern(const unsigned char *buf, Size buf_len,
						  const FbWalPrefilterPattern *pattern)
{
	if (buf == NULL || pattern == NULL || pattern->len == 0 ||
		buf_len < pattern->len)
		return false;

	return memmem(buf, buf_len, pattern->bytes, pattern->len) != NULL;
}

/*
 * fb_segment_path_matches_patterns
 *    WAL helper.
 */

static bool
fb_segment_path_matches_patterns(const FbWalSegmentEntry *entry,
								 const FbRelationInfo *info,
								 int wal_seg_size,
								 FbWalPrefilterPattern *patterns,
								 int pattern_count)
{
	int fd = -1;
	struct stat st;
	void *map = MAP_FAILED;
	bool summary_available = false;
	bool summary_hit = false;
	int i;

	if (pattern_count <= 0)
		return true;
	if (entry == NULL || info == NULL)
		return true;

	if (fb_summary_segment_matches(entry->path,
								   entry->bytes,
								   entry->timeline_id,
								   entry->segno,
								   wal_seg_size,
								   entry->source_kind,
								   info,
								   &summary_available,
								   &summary_hit))
		return summary_hit;

	fd = open(entry->path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return true;

	if (fstat(fd, &st) != 0)
	{
		close(fd);
		return true;
	}

	if (st.st_size == 0)
	{
		close(fd);
		return false;
	}

	map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
	close(fd);
	if (map == MAP_FAILED)
		return true;

	for (i = 0; i < pattern_count; i++)
	{
		if (fb_bytes_contains_pattern((const unsigned char *) map,
									  (Size) st.st_size,
									  &patterns[i]))
		{
			munmap(map, st.st_size);
			return true;
		}
	}

	munmap(map, st.st_size);
	return false;
}

/*
 * fb_segment_prefilter_worker
 *    WAL helper.
 */

static void *
fb_segment_prefilter_worker(void *arg)
{
	FbWalSegmentPrefilterTask *task = (FbWalSegmentPrefilterTask *) arg;
	int n;

	for (n = 0; n < task->index_count; n++)
	{
		int i = task->indexes[n];

		task->hit_map[i] = fb_segment_path_matches_patterns(&task->segments[i],
															 task->info,
															 task->wal_seg_size,
															 task->patterns,
															 task->pattern_count);
	}

	return NULL;
}

static void *
fb_segment_validate_worker(void *arg)
{
	FbWalSegmentValidateTask *task = (FbWalSegmentValidateTask *) arg;
	int n;

	for (n = 0; n < task->index_count; n++)
	{
		FbWalSegmentEntry *entry = &task->segments[task->indexes[n]];

		if ((FbWalSourceKind) entry->source_kind != FB_WAL_SOURCE_PG_WAL)
			continue;

		if (!fb_validate_segment_identity_path(entry->path,
												 entry->timeline_id,
												 entry->segno,
												 task->wal_seg_size))
		{
			entry->valid = false;
			entry->mismatch = true;
		}
	}

	return NULL;
}

/*
 * fb_append_prefilter_locator_patterns
 *    WAL helper.
 */

static void
fb_append_prefilter_locator_patterns(const RelFileLocator *locator,
									 FbWalPrefilterPattern *patterns,
									 int *pattern_count)
{
	RelFileLocator alt_locator;

	memcpy(patterns[*pattern_count].bytes, locator, sizeof(RelFileLocator));
	patterns[*pattern_count].len = sizeof(RelFileLocator);
	fb_prefilter_pattern_init(&patterns[*pattern_count]);
	(*pattern_count)++;

	if (FB_LOCATOR_SPCOID(*locator) == MyDatabaseTableSpace ||
		MyDatabaseTableSpace == InvalidOid)
		return;

	alt_locator = *locator;
	FB_LOCATOR_SPCOID(alt_locator) = MyDatabaseTableSpace;
	memcpy(patterns[*pattern_count].bytes, &alt_locator, sizeof(RelFileLocator));
	patterns[*pattern_count].len = sizeof(RelFileLocator);
	fb_prefilter_pattern_init(&patterns[*pattern_count]);
	(*pattern_count)++;
}

/*
 * fb_prepare_prefilter_patterns
 *    WAL helper.
 */

static void
fb_prepare_prefilter_patterns(const FbRelationInfo *info,
							  FbWalPrefilterPattern *patterns,
							  int *pattern_count)
{
	int count = 0;

	MemSet(patterns, 0, sizeof(FbWalPrefilterPattern) * 4);
	fb_append_prefilter_locator_patterns(&info->locator, patterns, &count);

	if (info->has_toast_locator)
		fb_append_prefilter_locator_patterns(&info->toast_locator, patterns, &count);

	*pattern_count = count;
}

/*
 * fb_prepare_segment_prefilter
 *    WAL helper.
 */

static void
fb_prepare_segment_prefilter(const FbRelationInfo *info, FbWalScanContext *ctx)
{
	FbWalSegmentEntry *segments;
	pthread_t workers[FB_WAL_PARALLEL_MAX_WORKERS];
	bool worker_started[FB_WAL_PARALLEL_MAX_WORKERS];
	FbWalSegmentPrefilterTask tasks[FB_WAL_PARALLEL_MAX_WORKERS];
	FbWalPrefilterPattern patterns[4];
	int *pending_indexes = NULL;
	int pattern_count = 0;
	int worker_count;
	int chunk_size;
	int pending_count = 0;
	uint64 pattern_hash;
	int i;

	if (ctx == NULL || info == NULL)
		return;

	if (ctx->segment_prefilter_ready)
		return;

	ctx->prefilter_total_segments = 0;
	ctx->current_segment_may_hit = true;

	if (ctx->resolved_segment_count == 0)
	{
		ctx->segment_prefilter_ready = true;
		ctx->segment_prefilter_used = false;
		return;
	}

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	ctx->prefilter_total_segments = ctx->resolved_segment_count;
	ctx->segment_hit_map = palloc0(sizeof(bool) * ctx->resolved_segment_count);
	fb_prepare_prefilter_patterns(info, patterns, &pattern_count);
	fb_runtime_ensure_initialized();
	pattern_hash = fb_prefilter_patterns_hash(patterns, pattern_count);
	pending_indexes = palloc(sizeof(int) * ctx->resolved_segment_count);

	for (i = 0; i < (int) ctx->resolved_segment_count; i++)
	{
		uint64 cache_key = fb_prefilter_cache_key(&segments[i], pattern_hash);

		if (!fb_prefilter_cache_lookup_memory(cache_key, &ctx->segment_hit_map[i]))
			pending_indexes[pending_count++] = i;
	}

	if (pending_count == 0)
		goto finalize_hits;

	worker_count = fb_parallel_worker_count(pending_count);
	chunk_size = (pending_count + worker_count - 1) / worker_count;

	for (i = 0; i < worker_count; i++)
	{
		int start = i * chunk_size;
		int end = Min(start + chunk_size, pending_count);

		worker_started[i] = false;

		tasks[i].segments = segments;
		tasks[i].indexes = pending_indexes + start;
		tasks[i].index_count = end - start;
		tasks[i].info = info;
		tasks[i].wal_seg_size = ctx->wal_seg_size;
		memcpy(tasks[i].patterns, patterns, sizeof(patterns));
		tasks[i].pattern_count = pattern_count;
		tasks[i].hit_map = ctx->segment_hit_map;

		if (start >= end)
			continue;

		if (worker_count == 1 ||
			pthread_create(&workers[i], NULL, fb_segment_prefilter_worker, &tasks[i]) != 0)
		{
			fb_segment_prefilter_worker(&tasks[i]);
		}
		else
			worker_started[i] = true;
	}

	for (i = 0; i < worker_count; i++)
	{
		if (worker_started[i])
			pthread_join(workers[i], NULL);
	}

	for (i = 0; i < pending_count; i++)
	{
		int segment_index = pending_indexes[i];
		uint64 cache_key = fb_prefilter_cache_key(&segments[segment_index], pattern_hash);

		fb_prefilter_cache_store_memory(cache_key, ctx->segment_hit_map[segment_index]);
	}

finalize_hits:
	if (pending_indexes != NULL)
		pfree(pending_indexes);

	ctx->prefilter_hit_segments = 0;
	for (i = 0; i < (int) ctx->resolved_segment_count; i++)
	{
		if (ctx->segment_hit_map[i])
			ctx->prefilter_hit_segments++;
	}

	if (ctx->prefilter_hit_segments == 0)
	{
		ctx->segment_prefilter_ready = true;
		ctx->segment_prefilter_used = false;
		ctx->prefilter_total_segments = 0;
		ctx->current_segment_may_hit = true;
		return;
	}

	ctx->segment_prefilter_ready = true;
	ctx->segment_prefilter_used = true;
}

/*
 * fb_build_prefilter_visit_windows
 *    WAL helper.
 */

static uint32
fb_build_prefilter_visit_windows(FbWalScanContext *ctx, FbWalVisitWindow **windows_out)
{
	FbWalSegmentEntry *segments;
	bool *selected;
	FbWalVisitWindow *windows;
	uint32 leading_index = 0;
	uint32 selected_count = 0;
	uint32 window_count = 0;
	uint32 i;

	if (windows_out != NULL)
		*windows_out = NULL;

	if (ctx == NULL || !ctx->segment_prefilter_used || ctx->segment_hit_map == NULL ||
		ctx->resolved_segment_count == 0)
		return 0;

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	selected = palloc0(sizeof(bool) * ctx->resolved_segment_count);

	/*
	 * Without an anchor hint, always keep the leading segment(s) so the first
	 * real scan can still discover a safe checkpoint anchor. Once a warm-query
	 * hint exists, start from that segment instead of forcing segment zero.
	 * Around each hit segment, add one neighbor on both sides to keep
	 * cross-segment record decoding safe.
	 */
	if (ctx->anchor_hint_found &&
		ctx->anchor_hint_segment_index < ctx->resolved_segment_count)
		leading_index = ctx->anchor_hint_segment_index;

	selected[leading_index] = true;
	selected_count++;
	if (leading_index + 1 < ctx->resolved_segment_count)
	{
		selected[leading_index + 1] = true;
		selected_count += selected[leading_index + 1] ? 1 : 0;
	}

	for (i = 0; i < ctx->resolved_segment_count; i++)
	{
		uint32 start;
		uint32 end;
		uint32 j;

		if (!ctx->segment_hit_map[i])
			continue;

		start = (i == 0) ? 0 : (i - 1);
		end = Min(i + 1, ctx->resolved_segment_count - 1);
		for (j = start; j <= end; j++)
		{
			if (!selected[j])
			{
				selected[j] = true;
				selected_count++;
			}
		}
	}

	if (selected_count >= ctx->resolved_segment_count)
	{
		pfree(selected);
		return 0;
	}

	windows = palloc0(sizeof(FbWalVisitWindow) * selected_count);
	for (i = 0; i < ctx->resolved_segment_count; i++)
	{
		uint32 start;
		uint32 end;

		if (!selected[i])
			continue;

		start = i;
		end = i;
		while (end + 1 < ctx->resolved_segment_count && selected[end + 1])
			end++;

		windows[window_count].segments = &segments[start];
		windows[window_count].segment_count = end - start + 1;
		windows[window_count].start_lsn =
			Max(fb_segment_start_lsn(&segments[start], ctx->wal_seg_size),
				ctx->start_lsn);
		windows[window_count].end_lsn =
			(end == ctx->resolved_segment_count - 1) ? ctx->end_lsn :
			fb_segment_end_lsn(&segments[end], ctx->wal_seg_size);
		windows[window_count].read_end_lsn = windows[window_count].end_lsn;
		if (windows[window_count].start_lsn >= windows[window_count].end_lsn)
		{
			i = end;
			continue;
		}
		window_count++;
		i = end;
	}

	pfree(selected);
	if (window_count == 0)
	{
		pfree(windows);
		return 0;
	}
	*windows_out = windows;
	return window_count;
}

static uint32
fb_build_summary_span_visit_windows(const FbRelationInfo *info,
									FbWalScanContext *ctx,
									const FbWalVisitWindow *base_windows,
									uint32 base_window_count,
									FbWalVisitWindow **windows_out)
{
	FbWalVisitWindow *windows = NULL;
	uint32 window_count = 0;
	uint32 window_capacity = 0;
	const FbWalVisitWindow *source_windows;
	uint32 source_window_count;
	uint32 i;

	if (windows_out != NULL)
		*windows_out = NULL;
	if (info == NULL || ctx == NULL)
		return 0;
	if (ctx->summary_cache == NULL)
		ctx->summary_cache = fb_summary_query_cache_create(CurrentMemoryContext);

	if (base_window_count == 0 || base_windows == NULL)
	{
		FbWalVisitWindow full_window;

		full_window.segments = (FbWalSegmentEntry *) ctx->resolved_segments;
		full_window.segment_count = ctx->resolved_segment_count;
		full_window.start_lsn = ctx->start_lsn;
		full_window.end_lsn = ctx->end_lsn;
		full_window.read_end_lsn = ctx->end_lsn;
		source_windows = &full_window;
		source_window_count = 1;
	}
	else
	{
		source_windows = base_windows;
		source_window_count = base_window_count;
	}

	for (i = 0; i < source_window_count; i++)
	{
		const FbWalVisitWindow *base = &source_windows[i];
		uint32 j;

		for (j = 0; j < base->segment_count; j++)
		{
			FbWalSegmentEntry *segment = &base->segments[j];
			const FbSummarySpan *spans = NULL;
			uint32 span_count = 0;
			uint32 k;

			ctx->summary_span_segments_read++;
			if (!fb_summary_segment_lookup_spans_cached(segment->path,
														segment->bytes,
														segment->timeline_id,
														segment->segno,
														ctx->wal_seg_size,
														segment->source_kind,
														info,
														ctx->summary_cache,
														&spans,
														&span_count))
			{
				ctx->summary_span_fallback_segments++;
				if (window_count == window_capacity)
				{
					window_capacity = (window_capacity == 0) ? 16 : window_capacity * 2;
					if (windows == NULL)
						windows = palloc0(sizeof(*windows) * window_capacity);
					else
						windows = repalloc(windows, sizeof(*windows) * window_capacity);
				}

				windows[window_count].segments = segment;
				windows[window_count].segment_count = 1;
				windows[window_count].start_lsn =
					Max(base->start_lsn,
						((XLogRecPtr) segment->segno) * (XLogRecPtr) ctx->wal_seg_size);
				windows[window_count].end_lsn =
					Min(base->end_lsn,
						(((XLogRecPtr) segment->segno) + 1) * (XLogRecPtr) ctx->wal_seg_size);
				windows[window_count].read_end_lsn = windows[window_count].end_lsn;
				window_count++;
				continue;
			}
			ctx->summary_span_covered_segments++;

			for (k = 0; k < span_count; k++)
			{
				XLogRecPtr span_start = Max(spans[k].start_lsn, base->start_lsn);
				XLogRecPtr span_end = Min(spans[k].end_lsn, base->end_lsn);
				int start_index;
				int end_index;

				if (span_start >= span_end)
					continue;

				start_index = fb_find_segment_index_for_lsn((FbWalSegmentEntry *) ctx->resolved_segments,
															  ctx->resolved_segment_count,
															  span_start,
															  ctx->wal_seg_size);
				end_index = fb_find_segment_index_for_lsn((FbWalSegmentEntry *) ctx->resolved_segments,
															ctx->resolved_segment_count,
															span_end - 1,
															ctx->wal_seg_size);
				if (start_index < 0 || end_index < start_index)
					continue;

				if (window_count == window_capacity)
				{
					window_capacity = (window_capacity == 0) ? 16 : window_capacity * 2;
					if (windows == NULL)
						windows = palloc0(sizeof(*windows) * window_capacity);
					else
						windows = repalloc(windows, sizeof(*windows) * window_capacity);
				}

				windows[window_count].segments =
					&((FbWalSegmentEntry *) ctx->resolved_segments)[start_index];
				windows[window_count].segment_count = (uint32) (end_index - start_index + 1);
				windows[window_count].start_lsn = span_start;
				windows[window_count].end_lsn = span_end;
				windows[window_count].read_end_lsn = span_end;
				window_count++;
			}
		}
	}

	if (window_count == 0)
	{
		if (windows != NULL)
			pfree(windows);
		return 0;
	}

	if (windows_out != NULL)
		*windows_out = windows;
	else
		pfree(windows);
	ctx->summary_span_windows = window_count;
	ctx->summary_span_public_builds =
		fb_summary_query_cache_span_public_builds(ctx->summary_cache);
	return window_count;
}

static int
fb_visit_window_cmp(const void *lhs, const void *rhs)
{
	const FbWalVisitWindow *left = (const FbWalVisitWindow *) lhs;
	const FbWalVisitWindow *right = (const FbWalVisitWindow *) rhs;

	if (left->start_lsn < right->start_lsn)
		return -1;
	if (left->start_lsn > right->start_lsn)
		return 1;
	if (left->end_lsn < right->end_lsn)
		return -1;
	if (left->end_lsn > right->end_lsn)
		return 1;
	return 0;
}

static uint32
fb_merge_visit_windows(FbWalScanContext *ctx,
						 FbWalVisitWindow **windows_io,
						 uint32 window_count)
{
	FbWalVisitWindow *windows;
	FbWalSegmentEntry *segments;
	uint32 merged_count = 0;
	uint32 i;

	if (ctx == NULL || windows_io == NULL || *windows_io == NULL || window_count <= 1)
		return window_count;

	windows = *windows_io;
	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	qsort(windows, window_count, sizeof(*windows), fb_visit_window_cmp);

	for (i = 0; i < window_count; i++)
	{
		FbWalVisitWindow *dst;
		const FbWalVisitWindow *src = &windows[i];
		int start_index;
		int end_index;

		if (src->start_lsn >= src->end_lsn)
			continue;

		start_index = fb_find_segment_index_for_lsn(segments,
													ctx->resolved_segment_count,
													src->start_lsn,
													ctx->wal_seg_size);
		end_index = fb_find_segment_index_for_lsn(segments,
												  ctx->resolved_segment_count,
												  src->end_lsn - 1,
												  ctx->wal_seg_size);
		if (start_index < 0 || end_index < start_index)
			continue;

		if (merged_count == 0)
		{
			windows[0].segments = &segments[start_index];
			windows[0].segment_count = (uint32) (end_index - start_index + 1);
			windows[0].start_lsn = src->start_lsn;
			windows[0].end_lsn = src->end_lsn;
			windows[0].read_end_lsn = src->read_end_lsn;
			merged_count = 1;
			continue;
		}

		dst = &windows[merged_count - 1];
		if (src->start_lsn <= dst->end_lsn)
		{
			int dst_start_index = (int) (dst->segments - segments);
			int dst_end_index = dst_start_index + (int) dst->segment_count - 1;

			if (src->end_lsn > dst->end_lsn)
				dst->end_lsn = src->end_lsn;
			if (src->read_end_lsn > dst->read_end_lsn)
				dst->read_end_lsn = src->read_end_lsn;
			if (start_index < dst_start_index)
				dst_start_index = start_index;
			if (end_index > dst_end_index)
				dst_end_index = end_index;
			dst->segments = &segments[dst_start_index];
			dst->segment_count = (uint32) (dst_end_index - dst_start_index + 1);
			continue;
		}

		windows[merged_count].segments = &segments[start_index];
		windows[merged_count].segment_count = (uint32) (end_index - start_index + 1);
		windows[merged_count].start_lsn = src->start_lsn;
		windows[merged_count].end_lsn = src->end_lsn;
		windows[merged_count].read_end_lsn = src->read_end_lsn;
		merged_count++;
	}

	if (merged_count == 0)
	{
		pfree(windows);
		*windows_io = NULL;
		return 0;
	}

	*windows_io = repalloc(windows, sizeof(*windows) * merged_count);
	return merged_count;
}

static uint32
fb_payload_locator_deduplicate_by_lsn(FbSummaryPayloadLocator *locators,
										uint32 count)
{
	uint32 write_index = 1;
	uint32 read_index;

	if (locators == NULL || count <= 1)
		return count;

	for (read_index = 1; read_index < count; read_index++)
	{
		if (locators[write_index - 1].record_start_lsn ==
			locators[read_index].record_start_lsn)
			continue;
		if (write_index != read_index)
			locators[write_index] = locators[read_index];
		write_index++;
	}

	return write_index;
}

static uint32
fb_build_xact_fallback_visit_windows(FbWalScanContext *ctx,
									 const FbWalVisitWindow *base_windows,
									 uint32 base_window_count,
									 FbWalVisitWindow **windows_out)
{
	FbWalVisitWindow *windows = NULL;
	FbWalSegmentEntry *segments;
	uint32 window_count = 0;
	uint32 i;

	if (windows_out != NULL)
		*windows_out = NULL;
	if (ctx == NULL)
		return 0;

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	if (segments == NULL || ctx->resolved_segment_count == 0)
		return 0;

	if (base_windows == NULL || base_window_count == 0)
	{
		windows = palloc0(sizeof(*windows));
		windows[0].segments = segments;
		windows[0].segment_count = ctx->resolved_segment_count;
		windows[0].start_lsn = ctx->start_lsn;
		windows[0].end_lsn = ctx->end_lsn;
		windows[0].read_end_lsn = ctx->end_lsn;
		if (windows_out != NULL)
			*windows_out = windows;
		return 1;
	}

	windows = palloc0(sizeof(*windows) * base_window_count);
	for (i = 0; i < base_window_count; i++)
	{
		const FbWalVisitWindow *base = &base_windows[i];
		int start_index;
		int end_index;

		if (base->start_lsn >= base->end_lsn)
			continue;

		start_index = fb_find_segment_index_for_lsn(segments,
													ctx->resolved_segment_count,
													base->start_lsn,
													ctx->wal_seg_size);
		end_index = fb_find_segment_index_for_lsn(segments,
												  ctx->resolved_segment_count,
												  base->end_lsn - 1,
												  ctx->wal_seg_size);
		if (start_index < 0 || end_index < start_index)
			continue;

		windows[window_count].segments = &segments[start_index];
		windows[window_count].segment_count = (uint32) (end_index - start_index + 1);
		windows[window_count].start_lsn =
			fb_segment_start_lsn(&segments[start_index], ctx->wal_seg_size);
		windows[window_count].end_lsn =
			fb_segment_end_lsn(&segments[end_index], ctx->wal_seg_size);
		windows[window_count].read_end_lsn = windows[window_count].end_lsn;
		window_count++;
	}

	if (window_count == 0)
	{
		pfree(windows);
		return 0;
	}

	window_count = fb_merge_visit_windows(ctx, &windows, window_count);
	if (windows_out != NULL)
		*windows_out = windows;
	else if (windows != NULL)
		pfree(windows);
	return window_count;
}

static uint32
fb_coalesce_payload_visit_windows(FbWalScanContext *ctx,
								  FbWalVisitWindow **windows_io,
								  uint32 window_count)
{
	FbWalVisitWindow *windows;
	FbWalSegmentEntry *segments;
	uint32 merged_count = 0;
	uint32 i;

	if (ctx == NULL || windows_io == NULL || *windows_io == NULL || window_count <= 1)
		return window_count;

	windows = *windows_io;
	segments = (FbWalSegmentEntry *) ctx->resolved_segments;

	for (i = 0; i < window_count; i++)
	{
		const FbWalVisitWindow *src = &windows[i];
		int start_index;
		int end_index;

		if (src->start_lsn >= src->end_lsn)
			continue;

		start_index = fb_find_segment_index_for_lsn(segments,
													ctx->resolved_segment_count,
													src->start_lsn,
													ctx->wal_seg_size);
		end_index = fb_find_segment_index_for_lsn(segments,
												  ctx->resolved_segment_count,
												  src->end_lsn - 1,
												  ctx->wal_seg_size);
		if (start_index < 0 || end_index < start_index)
			continue;

		if (merged_count == 0)
		{
			windows[0].segments = &segments[start_index];
			windows[0].segment_count = (uint32) (end_index - start_index + 1);
			windows[0].start_lsn = src->start_lsn;
			windows[0].end_lsn = src->end_lsn;
			windows[0].read_end_lsn = src->read_end_lsn;
			merged_count = 1;
			continue;
		}

		{
			FbWalVisitWindow *dst = &windows[merged_count - 1];
			int dst_start_index = (int) (dst->segments - segments);
			int dst_end_index = dst_start_index + (int) dst->segment_count - 1;

			/*
			 * Payload phase only copies target-relation records, but widening a
			 * window across segment gaps can still explode decode work on large
			 * time ranges. Keep this narrowing conservative: only collapse
			 * windows that already live inside the same covered segment slice.
			 */
			if (start_index == dst_start_index &&
				end_index == dst_end_index)
			{
				if (src->end_lsn > dst->end_lsn)
					dst->end_lsn = src->end_lsn;
				if (src->read_end_lsn > dst->read_end_lsn)
					dst->read_end_lsn = src->read_end_lsn;
				if (start_index < dst_start_index)
					dst_start_index = start_index;
				if (end_index > dst_end_index)
					dst_end_index = end_index;
				dst->segments = &segments[dst_start_index];
				dst->segment_count = (uint32) (dst_end_index - dst_start_index + 1);
				continue;
			}
		}

		windows[merged_count].segments = &segments[start_index];
		windows[merged_count].segment_count = (uint32) (end_index - start_index + 1);
		windows[merged_count].start_lsn = src->start_lsn;
		windows[merged_count].end_lsn = src->end_lsn;
		windows[merged_count].read_end_lsn = src->read_end_lsn;
		merged_count++;
	}

	if (merged_count == 0)
	{
		pfree(windows);
		*windows_io = NULL;
		return 0;
	}

	*windows_io = repalloc(windows, sizeof(*windows) * merged_count);
	return merged_count;
}

static uint32
fb_summary_seed_metadata_from_summary(const FbRelationInfo *info,
									  FbWalScanContext *ctx,
									  const FbWalVisitWindow *windows,
									  uint32 window_count,
									  HTAB *touched_xids,
									  HTAB *unsafe_xids,
									  FbWalVisitWindow **fallback_windows_out)
{
	FbWalSegmentEntry *segments;
	bool *segment_seen;
	FbWalVisitWindow *fallback_windows = NULL;
	uint32 fallback_count = 0;
	uint32 fallback_capacity = 0;
	uint32 i;

	if (fallback_windows_out != NULL)
		*fallback_windows_out = NULL;
	if (info == NULL || ctx == NULL || windows == NULL || window_count == 0 ||
		touched_xids == NULL || unsafe_xids == NULL)
		return 0;

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	segment_seen = palloc0(sizeof(bool) * ctx->resolved_segment_count);
	if (ctx->summary_cache == NULL)
		ctx->summary_cache = fb_summary_query_cache_create(CurrentMemoryContext);

	for (i = 0; i < window_count; i++)
	{
		const FbWalVisitWindow *window = &windows[i];
		uint32 j;

		for (j = 0; j < window->segment_count; j++)
		{
			uint32 segment_index = (uint32) ((window->segments - segments) + j);
			FbWalSegmentEntry *segment = &window->segments[j];
			TransactionId *summary_xids = NULL;
			FbSummaryUnsafeFact *summary_facts = NULL;
			uint32 summary_xid_count = 0;
			uint32 summary_fact_count = 0;
			bool has_xid_summary;
			bool has_unsafe_summary;
			uint32 fact_index;

			if (segment_index >= ctx->resolved_segment_count)
				elog(ERROR, "summary metadata seed segment index is out of bounds");
			if (segment_seen[segment_index])
				continue;
			segment_seen[segment_index] = true;

			has_xid_summary =
				fb_summary_segment_lookup_touched_xids_cached(segment->path,
															  segment->bytes,
															  segment->timeline_id,
															  segment->segno,
															  ctx->wal_seg_size,
															  segment->source_kind,
															  info,
															  ctx->summary_cache,
															  &summary_xids,
															  &summary_xid_count);
			has_unsafe_summary =
				fb_summary_segment_lookup_unsafe_facts_cached(segment->path,
															  segment->bytes,
															  segment->timeline_id,
															  segment->segno,
															  ctx->wal_seg_size,
															  segment->source_kind,
															  info,
															  ctx->summary_cache,
															  &summary_facts,
															  &summary_fact_count);
			if (!has_xid_summary || !has_unsafe_summary)
			{
				if (summary_xids != NULL)
					pfree(summary_xids);
				if (summary_facts != NULL)
					pfree(summary_facts);

				if (fallback_count == fallback_capacity)
				{
					fallback_capacity = (fallback_capacity == 0) ? 8 : fallback_capacity * 2;
					if (fallback_windows == NULL)
						fallback_windows = palloc0(sizeof(*fallback_windows) * fallback_capacity);
					else
						fallback_windows = repalloc(fallback_windows,
													sizeof(*fallback_windows) * fallback_capacity);
				}

				fallback_windows[fallback_count].segments = segment;
				fallback_windows[fallback_count].segment_count = 1;
				fallback_windows[fallback_count].start_lsn =
					Max(window->start_lsn,
						fb_segment_start_lsn(segment, ctx->wal_seg_size));
				fallback_windows[fallback_count].end_lsn =
					Min(window->end_lsn,
						fb_segment_end_lsn(segment, ctx->wal_seg_size));
				fallback_windows[fallback_count].read_end_lsn =
					fallback_windows[fallback_count].end_lsn;
				if (fallback_windows[fallback_count].start_lsn <
					fallback_windows[fallback_count].end_lsn)
					fallback_count++;
				continue;
			}

			for (fact_index = 0; fact_index < summary_xid_count; fact_index++)
				fb_mark_xid_touched(touched_xids, summary_xids[fact_index], ctx);
			for (fact_index = 0; fact_index < summary_fact_count; fact_index++)
			{
				fb_mark_xid_unsafe(unsafe_xids,
								   summary_facts[fact_index].xid,
								   (FbWalUnsafeReason) summary_facts[fact_index].reason,
								   (FbWalUnsafeScope) summary_facts[fact_index].scope,
								   (FbWalStorageChangeOp) summary_facts[fact_index].storage_op,
								   summary_facts[fact_index].record_lsn,
								   ctx);
				ctx->summary_unsafe_hits++;
			}

			if (summary_xids != NULL)
				pfree(summary_xids);
			if (summary_facts != NULL)
				pfree(summary_facts);
		}
	}

	pfree(segment_seen);
	ctx->metadata_fallback_windows = fallback_count;
	if (fallback_count == 0)
	{
		if (fallback_windows != NULL)
			pfree(fallback_windows);
		return 0;
	}

	if (fallback_windows_out != NULL)
		*fallback_windows_out = fallback_windows;
	else
		pfree(fallback_windows);
	return fallback_count;
}

/*
 * fb_build_materialize_visit_windows
 *    WAL helper.
 */

static uint32
fb_build_materialize_visit_windows(FbWalScanContext *ctx,
									 const FbWalVisitWindow *base_windows,
									 uint32 base_window_count,
									 XLogRecPtr start_lsn,
									 XLogRecPtr end_lsn,
									 FbWalVisitWindow **windows_out)
{
	FbWalVisitWindow *windows;
	uint32 capacity;
	uint32 window_count = 0;
	uint32 i;

	if (windows_out != NULL)
		*windows_out = NULL;

	if (ctx == NULL || XLogRecPtrIsInvalid(start_lsn))
		return 0;

	if (XLogRecPtrIsInvalid(end_lsn))
		end_lsn = ctx->end_lsn;
	if (start_lsn >= end_lsn)
		return 0;

	capacity = (base_window_count == 0) ? 1 : base_window_count;
	windows = palloc0(sizeof(FbWalVisitWindow) * capacity);

	if (base_window_count == 0)
	{
		windows[0].segments = (FbWalSegmentEntry *) ctx->resolved_segments;
		windows[0].segment_count = ctx->resolved_segment_count;
		windows[0].start_lsn = Max(ctx->start_lsn, start_lsn);
		windows[0].end_lsn = Min(ctx->end_lsn, end_lsn);
		windows[0].read_end_lsn = windows[0].end_lsn;
		*windows_out = windows;
		return 1;
	}

	for (i = 0; i < base_window_count; i++)
	{
		const FbWalVisitWindow *base = &base_windows[i];

		if (base->end_lsn <= start_lsn || base->start_lsn >= end_lsn)
			continue;

		windows[window_count] = *base;
		windows[window_count].start_lsn = Max(base->start_lsn, start_lsn);
		windows[window_count].end_lsn = Min(base->end_lsn, end_lsn);
		windows[window_count].read_end_lsn = windows[window_count].end_lsn;
		window_count++;
	}

	if (window_count == 0)
	{
		pfree(windows);
		return 0;
	}

	*windows_out = windows;
	return window_count;
}

static FbWalVisitWindow *
fb_copy_visit_windows(const FbWalVisitWindow *windows, uint32 window_count)
{
	FbWalVisitWindow *copy;

	if (windows == NULL || window_count == 0)
		return NULL;

	copy = palloc(sizeof(*copy) * window_count);
	memcpy(copy, windows, sizeof(*copy) * window_count);
	return copy;
}

static uint32
fb_build_payload_locator_visit_windows(FbWalScanContext *ctx,
										 const FbSummaryPayloadLocator *locators,
										 uint32 locator_count,
										 FbWalVisitWindow **windows_out)
{
	FbWalSegmentEntry *segments;
	FbWalVisitWindow *windows;
	XLogRecPtr gap_limit;
	uint32 segment_index = 0;
	uint32 window_count = 0;
	uint32 run_start_index = 0;
	uint32 run_end_index = 0;
	XLogRecPtr run_start_lsn = InvalidXLogRecPtr;
	XLogRecPtr run_end_lsn = InvalidXLogRecPtr;
	uint32 i;

	if (windows_out != NULL)
		*windows_out = NULL;
	if (ctx == NULL || locators == NULL || locator_count == 0)
		return 0;

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	if (segments == NULL || ctx->resolved_segment_count == 0)
		return 0;

	windows = palloc0(sizeof(*windows) * locator_count);
	gap_limit = Max((XLogRecPtr) (ctx->wal_seg_size / 8),
					(XLogRecPtr) (XLOG_BLCKSZ * 8));

	for (i = 0; i < locator_count; i++)
	{
		XLogSegNo locator_segno;

		XLByteToSeg(locators[i].record_start_lsn, locator_segno, ctx->wal_seg_size);
		while (segment_index < ctx->resolved_segment_count &&
			   segments[segment_index].segno < locator_segno)
			segment_index++;
		if (segment_index >= ctx->resolved_segment_count ||
			segments[segment_index].segno != locator_segno)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("summary payload locator segment is out of bounds"),
					 errdetail("Locator at %X/%08X does not map to a resolved WAL segment.",
							   LSN_FORMAT_ARGS(locators[i].record_start_lsn))));

		if (window_count == 0)
		{
			run_start_index = segment_index;
			run_end_index = segment_index;
			run_start_lsn = locators[i].record_start_lsn;
			run_end_lsn = locators[i].record_start_lsn + 1;
			window_count = 1;
			continue;
		}

		if (locators[i].record_start_lsn <= run_end_lsn + gap_limit)
		{
			run_end_index = segment_index;
			run_end_lsn = locators[i].record_start_lsn + 1;
			continue;
		}

		windows[window_count - 1].segments = &segments[run_start_index];
		windows[window_count - 1].segment_count =
			run_end_index - run_start_index + 1;
		windows[window_count - 1].start_lsn = run_start_lsn;
		windows[window_count - 1].end_lsn = run_end_lsn;
		windows[window_count - 1].read_end_lsn =
			fb_segment_end_lsn(&segments[run_end_index], ctx->wal_seg_size);

		run_start_index = segment_index;
		run_end_index = segment_index;
		run_start_lsn = locators[i].record_start_lsn;
		run_end_lsn = locators[i].record_start_lsn + 1;
		window_count++;
	}

	windows[window_count - 1].segments = &segments[run_start_index];
	windows[window_count - 1].segment_count =
		run_end_index - run_start_index + 1;
	windows[window_count - 1].start_lsn = run_start_lsn;
	windows[window_count - 1].end_lsn = run_end_lsn;
	windows[window_count - 1].read_end_lsn =
		fb_segment_end_lsn(&segments[run_end_index], ctx->wal_seg_size);

	if (windows_out != NULL)
		*windows_out = repalloc(windows, sizeof(*windows) * window_count);
	else
		pfree(windows);
	return window_count;
}

static uint32
fb_build_summary_payload_locator_plan(const FbRelationInfo *info,
										FbWalScanContext *ctx,
										const FbWalVisitWindow *base_windows,
										uint32 base_window_count,
										XLogRecPtr start_lsn,
										XLogRecPtr end_lsn,
										FbSummaryPayloadLocator **locators_out,
										FbWalVisitWindow **fallback_windows_out,
										uint32 *fallback_window_count_out,
										uint32 *fallback_segment_count_out,
										uint32 *segments_read_out,
										FbPayloadLocatorPlanDebug *plan_debug)
{
	FbWalSegmentEntry *segments;
	FbWalPayloadLocatorSegmentPlan *segment_plans = NULL;
	FbSummaryPayloadLocator *locators = NULL;
	FbWalVisitWindow *fallback_windows = NULL;
	uint32 locator_capacity = 0;
	uint32 locator_count = 0;
	uint32 fallback_capacity = 0;
	uint32 fallback_count = 0;
	uint32 fallback_segments = 0;
	uint32 segments_read = 0;
	uint32 i;

	if (locators_out != NULL)
		*locators_out = NULL;
	if (fallback_windows_out != NULL)
		*fallback_windows_out = NULL;
	if (fallback_window_count_out != NULL)
		*fallback_window_count_out = 0;
	if (fallback_segment_count_out != NULL)
		*fallback_segment_count_out = 0;
	if (segments_read_out != NULL)
		*segments_read_out = 0;

	if (info == NULL || ctx == NULL || XLogRecPtrIsInvalid(start_lsn))
		return 0;
	if (XLogRecPtrIsInvalid(end_lsn))
		end_lsn = ctx->end_lsn;
	if (start_lsn >= end_lsn)
		return 0;
	if (ctx->resolved_segments == NULL || ctx->resolved_segment_count == 0)
		return 0;

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	segment_plans =
		palloc0(sizeof(*segment_plans) * ctx->resolved_segment_count);

	for (i = 0; i < base_window_count; i++)
	{
		const FbWalVisitWindow *base = &base_windows[i];
		uint32 j;

		if (base->segments == NULL || base->segment_count == 0)
			continue;

		for (j = 0; j < base->segment_count; j++)
		{
			uint32 segment_index = (uint32) ((base->segments - segments) + j);
			FbWalSegmentEntry *segment = &base->segments[j];
			XLogRecPtr segment_start_lsn;
			XLogRecPtr segment_end_lsn;
			XLogRecPtr clipped_start;
			XLogRecPtr clipped_end;

			if (segment_index >= ctx->resolved_segment_count)
				elog(ERROR, "payload locator plan segment index is out of bounds");

			segment_start_lsn = fb_segment_start_lsn(segment, ctx->wal_seg_size);
			segment_end_lsn = segment_start_lsn + (XLogRecPtr) ctx->wal_seg_size;
			clipped_start = Max(start_lsn, Max(base->start_lsn, segment_start_lsn));
			clipped_end = Min(end_lsn, Min(base->end_lsn, segment_end_lsn));
			if (clipped_start >= clipped_end)
				continue;
			if (segment_plans[segment_index].segment == NULL)
			{
				segment_plans[segment_index].segment = segment;
				segment_plans[segment_index].start_lsn = clipped_start;
				segment_plans[segment_index].end_lsn = clipped_end;
			}
			else
			{
				if (clipped_start < segment_plans[segment_index].start_lsn)
					segment_plans[segment_index].start_lsn = clipped_start;
				if (clipped_end > segment_plans[segment_index].end_lsn)
					segment_plans[segment_index].end_lsn = clipped_end;
			}
		}
	}

	for (i = 0; i < ctx->resolved_segment_count; i++)
	{
		FbWalPayloadLocatorSegmentPlan *plan = &segment_plans[i];
		FbSummaryPayloadLocator *segment_locators = NULL;
		uint32 segment_locator_count = 0;
		uint32 k;

		if (plan->segment == NULL)
			continue;

		if (plan_debug != NULL)
		{
			uint32 source_index = (uint32) plan->segment->source_kind;

			plan_debug->base_segments++;
			if (source_index <= FB_WAL_SOURCE_CKWAL)
				plan_debug->base_by_source[source_index]++;
		}

		segments_read++;
		if (!fb_summary_segment_lookup_payload_locators_cached(plan->segment->path,
																 plan->segment->bytes,
																 plan->segment->timeline_id,
																 plan->segment->segno,
																 ctx->wal_seg_size,
																 plan->segment->source_kind,
																 info,
																 ctx->summary_cache,
																 &segment_locators,
																 &segment_locator_count))
		{
			if (plan_debug != NULL)
			{
				uint32 source_index = (uint32) plan->segment->source_kind;

				plan_debug->fallback_segments++;
				if (source_index <= FB_WAL_SOURCE_CKWAL)
					plan_debug->fallback_by_source[source_index]++;
				if (plan_debug->failed_sample_count <
					lengthof(plan_debug->failed_samples))
				{
					strlcpy(plan_debug->failed_samples[plan_debug->failed_sample_count].name,
							plan->segment->name,
							sizeof(plan_debug->failed_samples[0].name));
					plan_debug->failed_samples[plan_debug->failed_sample_count].source_kind =
						plan->segment->source_kind;
					plan_debug->failed_sample_count++;
				}
			}
			if (fallback_count == fallback_capacity)
			{
				fallback_capacity = (fallback_capacity == 0) ? 16 : fallback_capacity * 2;
				if (fallback_windows == NULL)
					fallback_windows = palloc0(sizeof(*fallback_windows) * fallback_capacity);
				else
					fallback_windows = repalloc(fallback_windows,
												sizeof(*fallback_windows) * fallback_capacity);
			}

			fallback_windows[fallback_count].segments = plan->segment;
			fallback_windows[fallback_count].segment_count = 1;
			fallback_windows[fallback_count].start_lsn = plan->start_lsn;
			fallback_windows[fallback_count].end_lsn = plan->end_lsn;
			fallback_windows[fallback_count].read_end_lsn = plan->end_lsn;
			fallback_count++;
			fallback_segments++;
			continue;
		}

		if (plan_debug != NULL)
		{
			uint32 source_index = (uint32) plan->segment->source_kind;

			plan_debug->success_segments++;
			if (source_index <= FB_WAL_SOURCE_CKWAL)
				plan_debug->success_by_source[source_index]++;
			if (segment_locator_count == 0)
				plan_debug->empty_success_segments++;
		}

		for (k = 0; k < segment_locator_count; k++)
		{
			if (!fb_wal_payload_kind_enabled(segment_locators[k].kind))
				continue;
			if (segment_locators[k].record_start_lsn < plan->start_lsn ||
				segment_locators[k].record_start_lsn >= plan->end_lsn)
				continue;

			if (locator_count == locator_capacity)
			{
				locator_capacity = (locator_capacity == 0) ? 64 : locator_capacity * 2;
				if (locators == NULL)
					locators = palloc0(sizeof(*locators) * locator_capacity);
				else
					locators = repalloc(locators, sizeof(*locators) * locator_capacity);
			}

			locators[locator_count++] = segment_locators[k];
			if (plan_debug != NULL)
			{
				uint32 source_index = (uint32) plan->segment->source_kind;

				plan_debug->locator_records++;
				if (source_index <= FB_WAL_SOURCE_CKWAL)
					plan_debug->locator_by_source[source_index]++;
			}
		}
		if (plan_debug != NULL && segment_locator_count > 0)
			plan_debug->segments_with_locators++;
		if (segment_locators != NULL)
			pfree(segment_locators);

	}

	if (locator_count == 0 && locators != NULL)
	{
		pfree(locators);
		locators = NULL;
	}
	else if (locator_count > 1)
		locator_count = fb_payload_locator_deduplicate_by_lsn(locators,
															 locator_count);
	if (fallback_count == 0 && fallback_windows != NULL)
	{
		pfree(fallback_windows);
		fallback_windows = NULL;
	}
	if (segment_plans != NULL)
		pfree(segment_plans);

	if (locators_out != NULL)
		*locators_out = locators;
	else if (locators != NULL)
		pfree(locators);
	if (fallback_windows_out != NULL)
		*fallback_windows_out = fallback_windows;
	else if (fallback_windows != NULL)
		pfree(fallback_windows);
	if (fallback_window_count_out != NULL)
		*fallback_window_count_out = fallback_count;
	if (fallback_segment_count_out != NULL)
		*fallback_segment_count_out = fallback_segments;
	if (segments_read_out != NULL)
		*segments_read_out = segments_read;
	return locator_count;
}

static uint32
fb_count_payload_locator_segments(FbWalScanContext *ctx,
								  const FbSummaryPayloadLocator *locators,
								  uint32 locator_count)
{
	XLogSegNo previous_segno = 0;
	uint32 segment_count = 0;
	uint32 i;

	if (ctx == NULL || locators == NULL || locator_count == 0)
		return 0;

	for (i = 0; i < locator_count; i++)
	{
		XLogSegNo current_segno;

		XLByteToSeg(locators[i].record_start_lsn, current_segno, ctx->wal_seg_size);
		if (i == 0 || current_segno != previous_segno)
		{
			segment_count++;
			previous_segno = current_segno;
		}
	}

	return segment_count;
}

static bool
fb_should_use_sparse_payload_scan(uint32 sparse_window_count,
								   uint32 windowed_window_count,
								   uint32 covered_segment_count)
{
	if (sparse_window_count == 0)
		return false;
	if (sparse_window_count <= 64)
		return false;
	if (windowed_window_count == 0)
		return true;

	/*
	 * Sparse mode only wins when the fragmented summary spans are so much more
	 * selective than their coalesced covered-slice windows that skipping the
	 * gaps outweighs per-window seek/redecode overhead. Cases like
	 * `approval_comments @ 2026-04-04 23:40:13` can still produce tens of
	 * thousands of sparse windows while the covered slices have already
	 * collapsed to a few hundred windows; forcing sparse there just repeats
	 * decode from the same segment slices and amplifies WalRead.
	 *
	 * Keep sparse for the much more extreme fragmentation profile that still
	 * materially cuts decode work on the large `documents` live case.
	 */
	if (covered_segment_count > 0 &&
		covered_segment_count <= 512 &&
		sparse_window_count < covered_segment_count * 128)
		return false;

	return sparse_window_count >= windowed_window_count * 8;
}

static bool
fb_wal_visit_windows_share_segment_slice(const FbWalVisitWindow *lhs,
										 const FbWalVisitWindow *rhs)
{
	if (lhs == NULL || rhs == NULL)
		return false;

	return lhs->segments == rhs->segments &&
		lhs->segment_count == rhs->segment_count;
}

static void
fb_wal_prepare_payload_read_window(FbWalScanContext *ctx,
								   const FbWalVisitWindow *window,
								   const FbWalSegmentEntry *all_segments,
								   uint32 all_segment_count,
								   FbWalVisitWindow *read_window)
{
	XLogRecPtr first_segment_start;

	if (read_window != NULL)
		MemSet(read_window, 0, sizeof(*read_window));
	if (ctx == NULL || window == NULL || read_window == NULL)
		return;

	*read_window = *window;
	if (window->segments == NULL || window->segment_count == 0)
		return;

	first_segment_start =
		fb_segment_start_lsn(&window->segments[0], ctx->wal_seg_size);
	read_window->start_lsn = first_segment_start;

	if (all_segments != NULL &&
		all_segment_count > 0 &&
		window->segments >= all_segments &&
		window->segments < all_segments + all_segment_count)
	{
		uint32 start_index = (uint32) (window->segments - all_segments);

		if (start_index > 0)
		{
			const FbWalSegmentEntry *previous = &all_segments[start_index - 1];

			if (previous->segno + 1 == window->segments[0].segno)
			{
				read_window->segments = (FbWalSegmentEntry *) previous;
				read_window->segment_count = window->segment_count + 1;
				read_window->start_lsn =
					fb_segment_start_lsn(previous, ctx->wal_seg_size);
			}
		}
	}
}

/*
 * fb_split_parallel_materialize_windows
 *    WAL helper.
 *
 * Keep payload replay semantics unchanged while exposing more independent
 * windows to payload workers. Each logical chunk may borrow one trailing
 * segment only for cross-segment record decoding; the emitted LSN range still
 * stops at the end of the logical chunk, so records are not duplicated.
 */

static uint32
fb_split_parallel_materialize_windows(FbWalScanContext *ctx,
									  FbWalVisitWindow **windows_io,
									  uint32 window_count)
{
	FbWalVisitWindow *base_windows;
	FbWalVisitWindow *split_windows;
	uint32 total_segments = 0;
	uint32 target_windows;
	uint32 target_segments_per_window;
	uint32 split_count = 0;
	uint32 i;

	if (ctx == NULL || windows_io == NULL || *windows_io == NULL || window_count == 0)
		return window_count;
	if (ctx->parallel_workers <= 1 || window_count >= (uint32) ctx->parallel_workers)
		return window_count;

	base_windows = *windows_io;
	for (i = 0; i < window_count; i++)
		total_segments += base_windows[i].segment_count;
	if (total_segments <= window_count)
		return window_count;

	target_windows = Min(total_segments, (uint32) ctx->parallel_workers);
	if (target_windows <= window_count)
		return window_count;

	target_segments_per_window =
		Max((uint32) 1, (total_segments + target_windows - 1) / target_windows);
	split_windows = palloc0(sizeof(FbWalVisitWindow) * total_segments);

	for (i = 0; i < window_count; i++)
	{
		const FbWalVisitWindow *base = &base_windows[i];
		uint32 segment_offset = 0;

		while (segment_offset < base->segment_count)
		{
			uint32 remaining = base->segment_count - segment_offset;
			uint32 logical_count = Min(target_segments_per_window, remaining);
			uint32 actual_count = logical_count;
			bool has_decode_tail = false;
			FbWalSegmentEntry *start_segment;
			XLogRecPtr start_lsn_chunk;
			XLogRecPtr end_lsn_chunk;
			XLogRecPtr read_end_lsn_chunk;

			if (logical_count < remaining)
			{
				actual_count++;
				has_decode_tail = true;
			}

			start_segment = &base->segments[segment_offset];
			start_lsn_chunk =
				(segment_offset == 0) ?
				base->start_lsn :
				fb_segment_start_lsn(start_segment, ctx->wal_seg_size);
			end_lsn_chunk =
				has_decode_tail ?
				fb_segment_end_lsn(&base->segments[segment_offset + logical_count - 1],
								   ctx->wal_seg_size) :
				base->end_lsn;
			read_end_lsn_chunk =
				has_decode_tail ?
				fb_segment_end_lsn(&base->segments[segment_offset + actual_count - 1],
								   ctx->wal_seg_size) :
				base->end_lsn;
			if (start_lsn_chunk < end_lsn_chunk)
			{
				split_windows[split_count].segments = start_segment;
				split_windows[split_count].segment_count = actual_count;
				split_windows[split_count].start_lsn = start_lsn_chunk;
				split_windows[split_count].end_lsn = end_lsn_chunk;
				split_windows[split_count].read_end_lsn = read_end_lsn_chunk;
				split_count++;
			}
			segment_offset += logical_count;
		}
	}

	if (split_count == 0 || split_count == window_count)
	{
		pfree(split_windows);
		return window_count;
	}

	pfree(base_windows);
	*windows_io = split_windows;
	return split_count;
}

/*
 * fb_open_file_at_path
 *    WAL helper.
 */

static int
fb_open_file_at_path(const char *path, FbWalOpenPattern open_pattern)
{
	int fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

#ifdef USE_POSIX_FADVISE
	if (fd >= 0 && open_pattern == FB_WAL_OPEN_SEQUENTIAL)
	{
		(void) posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#ifdef POSIX_FADV_WILLNEED
		(void) posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
#endif
	}
#endif

	return fd;
}

/*
 * fb_read_wal_segment_size_from_path
 *    WAL helper.
 */

static int
fb_read_wal_segment_size_from_path(const char *path)
{
	PGAlignedXLogBlock buf;
	FILE *fp;
	size_t bytes_read;
	XLogLongPageHeader longhdr;
	int wal_seg_size;

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

/*
 * fb_append_segment_entry
 *    WAL helper.
 */

static void
fb_append_segment_entry(FbWalSegmentEntry **segments,
						   int *segment_count,
						   int *segment_capacity,
						   const char *directory,
						   const char *name,
						   TimeLineID timeline_id,
						   FbWalSourceKind source_kind,
						   off_t bytes)
{
	FbWalSegmentEntry *entry;

	if (*segment_count == *segment_capacity)
	{
		*segment_capacity = (*segment_capacity == 0) ? 16 : (*segment_capacity * 2);
		if (*segments == NULL)
			*segments = palloc(sizeof(FbWalSegmentEntry) * (*segment_capacity));
		else
			*segments = repalloc(*segments,
								 sizeof(FbWalSegmentEntry) * (*segment_capacity));
	}

	entry = &(*segments)[(*segment_count)++];
	MemSet(entry, 0, sizeof(*entry));
	strlcpy(entry->name, name, sizeof(entry->name));
	entry->timeline_id = timeline_id;
	entry->bytes = bytes;
	entry->partial = false;
	entry->valid = true;
	entry->mismatch = false;
	entry->ignored = false;
	entry->source_kind = (int) source_kind;
	snprintf(entry->path, sizeof(entry->path), "%s/%s", directory, name);
}

/*
 * fb_append_segment_candidate
 *    WAL helper.
 */

static void
fb_append_segment_candidate(FbWalSegmentEntry **segments,
							int *segment_count,
							int *segment_capacity,
							const FbWalSegmentEntry *entry)
{
	if (*segment_count == *segment_capacity)
	{
		*segment_capacity = (*segment_capacity == 0) ? 16 : (*segment_capacity * 2);
		if (*segments == NULL)
			*segments = palloc(sizeof(FbWalSegmentEntry) * (*segment_capacity));
		else
			*segments = repalloc(*segments,
								 sizeof(FbWalSegmentEntry) * (*segment_capacity));
	}

	(*segments)[(*segment_count)++] = *entry;
}

/*
 * fb_collect_segments_from_directory
 *    WAL helper.
 */

static void
fb_collect_segments_from_directory(const char *directory,
								   FbWalSourceKind source_kind,
								   FbWalSegmentEntry **segments,
								   int *segment_count,
								   int *segment_capacity)
{
	DIR *dir;
	struct dirent *de;

	dir = AllocateDir(directory);
	if (dir == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not open %s directory: %s",
						fb_wal_source_name(source_kind), directory)));

	while ((de = ReadDir(dir, directory)) != NULL)
	{
		TimeLineID timeline_id;
		char path[MAXPGPATH];
		struct stat st;

		if (!IsXLogFileName(de->d_name))
			continue;
		if (!fb_parse_timeline_id(de->d_name, &timeline_id))
			continue;

		snprintf(path, sizeof(path), "%s/%s", directory, de->d_name);
		if (stat(path, &st) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat WAL segment \"%s\": %m", path)));

		fb_append_segment_entry(segments, segment_count, segment_capacity,
							   directory, de->d_name, timeline_id,
							   source_kind, st.st_size);
	}

	FreeDir(dir);
}

/*
 * fb_find_segment_by_segno
 *    WAL helper.
 */

static FbWalSegmentEntry *
fb_find_segment_by_segno(FbWalSegmentEntry *segments, int segment_count,
						 XLogSegNo segno)
{
	uint64 direct_index;
	int i;

	if (segments == NULL || segment_count <= 0)
		return NULL;

	if (segno >= segments[0].segno)
	{
		direct_index = (uint64) (segno - segments[0].segno);
		if (direct_index < (uint64) segment_count &&
			segments[direct_index].segno == segno)
			return &segments[direct_index];
	}

	for (i = 0; i < segment_count; i++)
	{
		if (segments[i].segno == segno)
			return &segments[i];
	}

	return NULL;
}

static int
fb_find_segment_index_for_lsn(FbWalSegmentEntry *segments, int segment_count,
							 XLogRecPtr lsn, int wal_seg_size)
{
	XLogSegNo segno;
	int i;

	if (segments == NULL || segment_count <= 0 || XLogRecPtrIsInvalid(lsn))
		return -1;

	XLByteToSeg(lsn, segno, wal_seg_size);
	for (i = 0; i < segment_count; i++)
	{
		if (segments[i].segno == segno)
			return i;
	}

	return -1;
}

/*
 * fb_segment_start_lsn
 *    WAL helper.
 */

static XLogRecPtr
fb_segment_start_lsn(const FbWalSegmentEntry *entry, int wal_seg_size)
{
	XLogRecPtr lsn;

	if (entry == NULL)
		return InvalidXLogRecPtr;

	XLogSegNoOffsetToRecPtr(entry->segno, 0, wal_seg_size, lsn);
	return lsn;
}

/*
 * fb_segment_end_lsn
 *    WAL helper.
 */

static XLogRecPtr
fb_segment_end_lsn(const FbWalSegmentEntry *entry, int wal_seg_size)
{
	XLogRecPtr lsn;
	off_t bytes;

	if (entry == NULL)
		return InvalidXLogRecPtr;

	lsn = fb_segment_start_lsn(entry, wal_seg_size);
	bytes = Min((off_t) wal_seg_size, entry->bytes);
	if (bytes < 0)
		bytes = 0;

	return lsn + (XLogRecPtr) bytes;
}

/*
 * fb_validate_segment_identity
 *    WAL helper.
 */

static bool
fb_read_segment_identity_path(const char *path,
							  TimeLineID *timeline_id,
							  XLogSegNo *segno,
							  int wal_seg_size)
{
	PGAlignedXLogBlock buf;
	int fd;
	ssize_t bytes_read;
	XLogLongPageHeader longhdr;

	if (path == NULL || timeline_id == NULL || segno == NULL)
		return false;

	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;

	bytes_read = read(fd, buf.data, XLOG_BLCKSZ);
	close(fd);
	if (bytes_read != XLOG_BLCKSZ)
		return false;

	longhdr = (XLogLongPageHeader) buf.data;
	if (!IsValidWalSegSize(longhdr->xlp_seg_size))
		return false;
	if (longhdr->xlp_seg_size != wal_seg_size)
		return false;

	*timeline_id = longhdr->std.xlp_tli;
	XLByteToSeg(longhdr->std.xlp_pageaddr, *segno, wal_seg_size);
	return true;
}

static bool
fb_validate_segment_identity_path(const char *path,
								  TimeLineID timeline_id,
								  XLogSegNo segno,
								  int wal_seg_size)
{
	PGAlignedXLogBlock buf;
	int fd;
	ssize_t bytes_read;
	XLogLongPageHeader longhdr;
	XLogRecPtr expected_pageaddr;

	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;

	bytes_read = read(fd, buf.data, XLOG_BLCKSZ);
	close(fd);
	if (bytes_read != XLOG_BLCKSZ)
		return false;

	longhdr = (XLogLongPageHeader) buf.data;
	if (!IsValidWalSegSize(longhdr->xlp_seg_size))
		return false;
	if (longhdr->xlp_seg_size != wal_seg_size)
		return false;

	XLogSegNoOffsetToRecPtr(segno, 0, wal_seg_size, expected_pageaddr);
	if (longhdr->std.xlp_pageaddr != expected_pageaddr)
		return false;
	if (longhdr->std.xlp_tli != timeline_id)
		return false;

	return true;
}

static bool
fb_validate_segment_identity(FbWalSegmentEntry *entry, int wal_seg_size)
{
	if (entry == NULL)
		return false;

	return fb_validate_segment_identity_path(entry->path,
											 entry->timeline_id,
											 entry->segno,
											 wal_seg_size);
}

static bool
fb_segment_candidate_exists(FbWalSegmentEntry *segments,
							int segment_count,
							TimeLineID timeline_id,
							XLogSegNo segno,
							const FbWalSegmentEntry *exclude)
{
	int i;

	if (segments == NULL || segment_count <= 0)
		return false;

	for (i = 0; i < segment_count; i++)
	{
		FbWalSegmentEntry *entry = &segments[i];
		FbWalSourceKind source_kind;

		if (entry == exclude || entry->ignored)
			continue;
		if (entry->timeline_id != timeline_id || entry->segno != segno)
			continue;

		source_kind = (FbWalSourceKind) entry->source_kind;
		if (source_kind == FB_WAL_SOURCE_ARCHIVE_DEST ||
			source_kind == FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY ||
			source_kind == FB_WAL_SOURCE_CKWAL)
			return true;
		if (source_kind == FB_WAL_SOURCE_PG_WAL &&
			entry->valid && !entry->mismatch)
			return true;
	}

	return false;
}

/*
 * fb_append_selected_segment
 *    WAL helper.
 */

static void
fb_append_selected_segment(FbWalSegmentEntry **selected,
						   int *selected_count,
						   int *selected_capacity,
						   const FbWalSegmentEntry *entry,
						   FbWalScanContext *ctx)
{
	if (*selected_count == *selected_capacity)
	{
		*selected_capacity = (*selected_capacity == 0) ? 16 : (*selected_capacity * 2);
		if (*selected == NULL)
			*selected = palloc(sizeof(FbWalSegmentEntry) * (*selected_capacity));
		else
			*selected = repalloc(*selected,
								 sizeof(FbWalSegmentEntry) * (*selected_capacity));
	}

	(*selected)[(*selected_count)++] = *entry;
	switch ((FbWalSourceKind) entry->source_kind)
	{
		case FB_WAL_SOURCE_PG_WAL:
			ctx->pg_wal_segment_count++;
			break;
		case FB_WAL_SOURCE_ARCHIVE_DEST:
		case FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY:
			ctx->archive_segment_count++;
			break;
		case FB_WAL_SOURCE_CKWAL:
			ctx->ckwal_segment_count++;
			break;
	}
}

/*
 * fb_try_ckwal_segment
 *    WAL helper.
 */

static bool
fb_try_ckwal_segment(FbWalScanContext *ctx,
					 TimeLineID timeline_id,
					 XLogSegNo segno,
					 int wal_seg_size,
					 FbWalSegmentEntry *entry)
{
	char fname[MAXPGPATH];

	MemSet(entry, 0, sizeof(*entry));
	XLogFileName(fname, timeline_id, segno, wal_seg_size);

	if (!fb_ckwal_restore_segment(timeline_id, segno, wal_seg_size,
								  entry->path, sizeof(entry->path)))
		return false;
	strlcpy(entry->name, fname, sizeof(entry->name));
	entry->timeline_id = timeline_id;
	entry->segno = segno;
	{
		struct stat st;

		if (stat(entry->path, &st) != 0)
			return false;
		entry->bytes = st.st_size;
		entry->partial = (st.st_size < wal_seg_size);
	}
	entry->valid = true;
	entry->mismatch = false;
	entry->source_kind = (int) FB_WAL_SOURCE_CKWAL;
	ctx->ckwal_invoked = true;

	return true;
}

/*
 * fb_collect_archive_segments
 *    WAL helper.
 */

static void
fb_collect_archive_segments(FbWalScanContext *ctx)
{
	FbWalSegmentEntry *candidates = NULL;
	FbWalSegmentEntry *direct = NULL;
	FbWalSegmentEntry *selected = NULL;
	pthread_t workers[FB_WAL_PARALLEL_MAX_WORKERS];
	bool worker_started[FB_WAL_PARALLEL_MAX_WORKERS];
	FbWalSegmentValidateTask tasks[FB_WAL_PARALLEL_MAX_WORKERS];
	char *archive_dir;
	FbArchiveDirSource archive_source;
	const char *archive_setting_name;
	char *pg_wal_dir = NULL;
	int *validate_indexes = NULL;
	int candidate_count = 0;
	int candidate_capacity = 0;
	int direct_count = 0;
	int direct_capacity = 0;
	int selected_count = 0;
	int selected_capacity = 0;
	int highest_tli = 0;
	int validate_count = 0;
	int worker_count = 1;
	int chunk_size = 1;
	int direct_index = 0;
	int i;
	XLogRecPtr last_segment_start;
	off_t last_segment_bytes;
	XLogSegNo highest_segno = 0;
	FbWalSegmentEntry *highest_invalid_pg_entry = NULL;

	archive_dir = fb_resolve_archive_dir(&archive_source, &archive_setting_name);
	ctx->using_archive_dest = archive_source != FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR;
	ctx->using_legacy_archive_dir = archive_source == FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR;
	pg_wal_dir = fb_get_pg_wal_dir();

	if (ctx->using_archive_dest)
	{
		fb_collect_segments_from_directory(pg_wal_dir, FB_WAL_SOURCE_PG_WAL,
										   &candidates, &candidate_count,
										   &candidate_capacity);
		fb_collect_segments_from_directory(archive_dir, FB_WAL_SOURCE_ARCHIVE_DEST,
										   &candidates, &candidate_count,
										   &candidate_capacity);
	}
	else
	{
		fb_collect_segments_from_directory(archive_dir,
										   FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY,
										   &candidates, &candidate_count,
										   &candidate_capacity);
	}

	if (candidate_count == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s contains no WAL segments: %s",
						archive_setting_name,
						archive_dir)));

	for (i = 0; i < candidate_count; i++)
	{
		if (candidates[i].timeline_id > (TimeLineID) highest_tli)
			highest_tli = candidates[i].timeline_id;
	}

	ctx->timeline_id = (TimeLineID) highest_tli;

	if (archive_dir != NULL)
		pfree(archive_dir);

	for (i = 0; i < candidate_count; i++)
	{
		TimeLineID timeline_id = 0;

		if (candidates[i].timeline_id != ctx->timeline_id)
			continue;
		if (ctx->wal_seg_size == 0)
			ctx->wal_seg_size = fb_read_wal_segment_size_from_path(candidates[i].path);

		XLogFromFileName(candidates[i].name, &timeline_id, &candidates[i].segno,
						 ctx->wal_seg_size);
		candidates[i].partial = (candidates[i].bytes < ctx->wal_seg_size);
		if ((FbWalSourceKind) candidates[i].source_kind == FB_WAL_SOURCE_PG_WAL)
		{
			if (validate_indexes == NULL)
				validate_indexes = palloc(sizeof(int) * candidate_count);
			validate_indexes[validate_count++] = i;
		}
	}

	if (validate_count > 0)
	{
		if (ctx->parallel_workers > 0 && validate_count > 1)
		{
			worker_count = fb_parallel_worker_count(validate_count);
			chunk_size = (validate_count + worker_count - 1) / worker_count;
			for (i = 0; i < worker_count; i++)
			{
				int start = i * chunk_size;
				int end = Min(start + chunk_size, validate_count);

				worker_started[i] = false;
				tasks[i].segments = candidates;
				tasks[i].indexes = validate_indexes + start;
				tasks[i].index_count = end - start;
				tasks[i].wal_seg_size = ctx->wal_seg_size;
				if (start >= end)
					continue;

				if (worker_count == 1 ||
					pthread_create(&workers[i], NULL, fb_segment_validate_worker,
								   &tasks[i]) != 0)
					fb_segment_validate_worker(&tasks[i]);
				else
					worker_started[i] = true;
			}

			for (i = 0; i < worker_count; i++)
			{
				if (worker_started[i])
					pthread_join(workers[i], NULL);
			}
		}
		else
		{
			for (i = 0; i < validate_count; i++)
			{
				FbWalSegmentEntry *entry = &candidates[validate_indexes[i]];

				if (!fb_validate_segment_identity(entry, ctx->wal_seg_size))
				{
					entry->valid = false;
					entry->mismatch = true;
				}
			}
		}
	}

	qsort(candidates, candidate_count, sizeof(FbWalSegmentEntry), fb_segment_name_cmp);

	for (i = 0; i < candidate_count; )
	{
		XLogSegNo segno;
		FbWalSegmentEntry *pg_entry = NULL;
		FbWalSegmentEntry *archive_entry = NULL;
		FbWalSegmentEntry *ckwal_entry = NULL;
		FbWalSegmentEntry *invalid_pg_entry = NULL;
		FbWalSegmentEntry *chosen;
		int j;

		if (candidates[i].timeline_id != ctx->timeline_id || candidates[i].ignored)
		{
			i++;
			continue;
		}

		segno = candidates[i].segno;
		for (j = i; j < candidate_count &&
			 candidates[j].timeline_id == ctx->timeline_id &&
			 candidates[j].segno == segno; j++)
		{
			if (candidates[j].ignored)
				continue;

			if ((FbWalSourceKind) candidates[j].source_kind == FB_WAL_SOURCE_PG_WAL)
			{
				if (candidates[j].valid)
					pg_entry = &candidates[j];
				else
					invalid_pg_entry = &candidates[j];
			}
			else if ((FbWalSourceKind) candidates[j].source_kind == FB_WAL_SOURCE_CKWAL)
				ckwal_entry = &candidates[j];
			else
				archive_entry = &candidates[j];
		}

		if (invalid_pg_entry != NULL &&
			(highest_invalid_pg_entry == NULL ||
			 segno > highest_invalid_pg_entry->segno))
			highest_invalid_pg_entry = invalid_pg_entry;

		if (archive_entry != NULL)
			chosen = archive_entry;
		else if (ckwal_entry != NULL)
			chosen = ckwal_entry;
		else if (pg_entry != NULL)
			chosen = pg_entry;
		else
			chosen = NULL;

		if (chosen != NULL)
		{
			if (segno > highest_segno)
				highest_segno = segno;
			fb_append_segment_candidate(&direct, &direct_count, &direct_capacity,
										chosen);
		}

		i = j;
	}

	if (highest_segno == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no WAL segments remain on selected timeline %u", ctx->timeline_id)));

	direct_index = direct_count - 1;
	while (true)
	{
		FbWalSegmentEntry *chosen = NULL;
		FbWalSegmentEntry recovered_entry;

		while (direct_index >= 0 && direct[direct_index].segno > highest_segno)
			direct_index--;

		if (direct_index >= 0 && direct[direct_index].segno == highest_segno)
			chosen = &direct[direct_index--];
		else if (fb_try_ckwal_segment(ctx, ctx->timeline_id, highest_segno,
									  ctx->wal_seg_size, &recovered_entry))
			chosen = &recovered_entry;
		else if (selected_count == 0 && highest_invalid_pg_entry != NULL &&
				 highest_invalid_pg_entry->segno == highest_segno)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("WAL not complete: pg_wal contains recycled or mismatched segments and internal recovery could not reconstruct a usable segment"),
					 errdetail("segment=%s source=%s",
							   highest_invalid_pg_entry->name,
							   highest_invalid_pg_entry->path)));
		else if (selected_count == 0)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("WAL not complete: could not resolve segment for segno %llu",
							(unsigned long long) highest_segno)));
		else
		{
			ctx->retained_suffix_only = true;
			if (direct_index >= 0)
				strlcpy(ctx->retained_gap_left_segment,
						direct[direct_index].name,
						sizeof(ctx->retained_gap_left_segment));
			if (selected_count > 0)
				strlcpy(ctx->retained_gap_right_segment,
						selected[selected_count - 1].name,
						sizeof(ctx->retained_gap_right_segment));
			break;
		}

		fb_append_selected_segment(&selected, &selected_count, &selected_capacity,
								   chosen, ctx);

		if (highest_segno == 0)
			break;
		highest_segno--;
	}

	if (selected_count == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no WAL segments remain on selected timeline %u", ctx->timeline_id)));

	for (i = 0; i < selected_count / 2; i++)
	{
		FbWalSegmentEntry tmp = selected[i];

		selected[i] = selected[selected_count - i - 1];
		selected[selected_count - i - 1] = tmp;
	}

	strlcpy(ctx->first_segment, selected[0].name, sizeof(ctx->first_segment));
	strlcpy(ctx->last_segment, selected[selected_count - 1].name,
			sizeof(ctx->last_segment));
	ctx->total_segments = selected_count;
	ctx->segments_complete = true;
	ctx->resolved_segments = selected;
	ctx->resolved_segment_count = selected_count;

	XLogSegNoOffsetToRecPtr(selected[0].segno, 0, ctx->wal_seg_size, ctx->start_lsn);
	XLogSegNoOffsetToRecPtr(selected[selected_count - 1].segno, 0,
							ctx->wal_seg_size, last_segment_start);

	last_segment_bytes = Min((off_t) ctx->wal_seg_size, selected[selected_count - 1].bytes);
	if (last_segment_bytes <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL segment \"%s\" is empty", selected[selected_count - 1].path)));

	ctx->end_lsn = last_segment_start + (XLogRecPtr) last_segment_bytes;
	ctx->end_is_partial = (last_segment_bytes < ctx->wal_seg_size);

	if (candidates != NULL)
		pfree(candidates);
	if (direct != NULL)
		pfree(direct);
	if (validate_indexes != NULL)
		pfree(validate_indexes);
	if (pg_wal_dir != NULL)
		pfree(pg_wal_dir);
}

/*
 * fb_wal_prepare_segment
 *    WAL helper.
 */

static FbWalSegmentEntry *
fb_wal_prepare_segment(FbWalReaderPrivate *private, XLogSegNo next_segno)
{
	FbWalScanContext *ctx = private->ctx;
	FbWalSegmentEntry *entry;
	FbWalSegmentEntry *segments;

	if (private->last_open_segno_valid &&
		private->last_open_segno == next_segno &&
		private->last_open_entry != NULL)
		return private->last_open_entry;

	entry = fb_find_segment_by_segno((FbWalSegmentEntry *) ctx->resolved_segments,
									 ctx->resolved_segment_count,
									 next_segno);
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: missing segment for segno %llu",
						(unsigned long long) next_segno)));

	ctx->current_segment_may_hit = true;
	if (ctx->segment_prefilter_used && ctx->segment_hit_map != NULL)
	{
		segments = (FbWalSegmentEntry *) ctx->resolved_segments;
		ctx->current_segment_may_hit = ctx->segment_hit_map[entry - segments];
	}
	if (!private->last_open_segno_valid || private->last_open_segno != next_segno)
	{
		private->last_open_segno = next_segno;
		private->last_open_segno_valid = true;
		private->last_open_entry = entry;
		ctx->visited_segment_count++;
	}

	return entry;
}

/*
 * fb_wal_close_private_file
 *    WAL helper.
 */

static void
fb_wal_close_private_file(FbWalReaderPrivate *private)
{
	if (private == NULL || !private->current_file_open)
		return;

	CloseTransientFile(private->current_file);
	private->current_file = -1;
	private->current_file_open = false;
	private->current_file_segno_valid = false;
}

/*
 * fb_wal_open_segment
 *    WAL helper.
 */

#if PG_VERSION_NUM >= 130000
static void
fb_wal_open_segment(XLogReaderState *state, XLogSegNo next_segno,
					TimeLineID *timeline_id)
{
	FbWalReaderPrivate *private = (FbWalReaderPrivate *) state->private_data;
	FbWalSegmentEntry *entry;

	entry = fb_wal_prepare_segment(private, next_segno);

	*timeline_id = entry->timeline_id;
	state->seg.ws_file = fb_open_file_at_path(entry->path, private->open_pattern);
	if (state->seg.ws_file < 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: missing segment %s in %s",
						entry->name, entry->path)));
}

/*
 * fb_wal_close_segment
 *    WAL helper.
 */

static void
fb_wal_close_segment(XLogReaderState *state)
{
	CloseTransientFile(state->seg.ws_file);
	state->seg.ws_file = -1;
}
#endif

/*
 * fb_wal_read_page
 *    WAL helper.
 */

static int
#if PG_VERSION_NUM < 130000
fb_wal_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr, int req_len,
				 XLogRecPtr target_rec_ptr, char *read_buf, TimeLineID *page_tli)
#else
fb_wal_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr, int req_len,
				 XLogRecPtr target_rec_ptr, char *read_buf)
#endif
{
	FbWalReaderPrivate *private = (FbWalReaderPrivate *) state->private_data;
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

#if PG_VERSION_NUM < 130000
	{
		XLogSegNo target_segno;
		uint32 segment_offset;
		FbWalSegmentEntry *entry;
		int nread;

		XLByteToSeg(target_page_ptr, target_segno, state->wal_segment_size);
		segment_offset = target_page_ptr % state->wal_segment_size;
		entry = fb_wal_prepare_segment(private, target_segno);

		if (page_tli != NULL)
			*page_tli = entry->timeline_id;

		if (!private->current_file_open ||
			!private->current_file_segno_valid ||
			private->current_file_segno != target_segno)
		{
			fb_wal_close_private_file(private);
			private->current_file = fb_open_file_at_path(entry->path,
														 private->open_pattern);
			if (private->current_file < 0)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("WAL not complete: missing segment %s in %s",
								entry->name, entry->path)));
			private->current_file_open = true;
			private->current_file_segno = target_segno;
			private->current_file_segno_valid = true;
		}

		nread = FileRead(private->current_file, read_buf, count, segment_offset, 0);
		if (nread < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL segment \"%s\", offset %u: %m",
							entry->name, segment_offset)));
		if (nread != count)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read WAL segment \"%s\", offset %u: read %d of %d",
							entry->name, segment_offset, nread, count)));
	}
#else
	{
		WALReadError errinfo;

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
	}
#endif

	return count;
}

/*
 * fb_create_touched_xid_hash
 *    WAL helper.
 */

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

/*
 * fb_create_unsafe_xid_hash
 *    WAL helper.
 */

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

static void
fb_append_touched_xid_hash_samples(StringInfo buf, HTAB *xids, uint32 limit)
{
	HASH_SEQ_STATUS status;
	FbTouchedXidEntry *entry;
	uint32 emitted = 0;

	if (buf == NULL)
		return;

	appendStringInfoChar(buf, '[');
	if (xids != NULL)
	{
		hash_seq_init(&status, xids);
		while ((entry = (FbTouchedXidEntry *) hash_seq_search(&status)) != NULL)
		{
			if (limit == 0 || emitted < limit)
			{
				if (emitted > 0)
					appendStringInfoString(buf, ",");
				appendStringInfo(buf, "%u", entry->xid);
				emitted++;
			}
		}
	}
	appendStringInfoChar(buf, ']');
}

static void
fb_append_payload_locator_failed_samples(StringInfo buf,
										 const FbPayloadLocatorPlanDebug *plan_debug)
{
	uint32 i;

	if (buf == NULL)
		return;

	appendStringInfoChar(buf, '[');
	if (plan_debug != NULL)
	{
		for (i = 0; i < plan_debug->failed_sample_count; i++)
		{
			if (i > 0)
				appendStringInfoString(buf, ",");
			appendStringInfo(buf,
							 "%s:%s",
							 plan_debug->failed_samples[i].name,
							 fb_wal_source_name((FbWalSourceKind)
												plan_debug->failed_samples[i].source_kind));
		}
	}
	appendStringInfoChar(buf, ']');
}

/*
 * fb_create_xid_status_hash
 *    WAL helper.
 */

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

static HTAB *
fb_create_count_only_xid_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(FbCountOnlyXidEntry);
	ctl.hcxt = CurrentMemoryContext;

	return hash_create("fb count-only xid stats",
					   128,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * fb_hash_has_xid
 *    WAL helper.
 */

static bool
fb_hash_has_xid(HTAB *hash, TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return false;

	return hash_search(hash, &xid, HASH_FIND, NULL) != NULL;
}

/*
 * fb_get_xid_status_entry
 *    WAL helper.
 */

static FbXidStatusEntry *
fb_get_xid_status_entry(HTAB *hash, TransactionId xid, bool *found)
{
	if (!TransactionIdIsValid(xid))
		return NULL;

	return (FbXidStatusEntry *) hash_search(hash, &xid, HASH_ENTER, found);
}

/*
 * fb_record_is_speculative_insert
 *    WAL helper.
 */

static bool
fb_record_is_speculative_insert(const FbRecordRef *record)
{
	const xl_heap_insert *xlrec;

	if (record == NULL || record->kind != FB_WAL_RECORD_HEAP_INSERT ||
		record->main_data_len < SizeOfHeapInsert || record->main_data == NULL)
		return false;

	xlrec = (const xl_heap_insert *) record->main_data;
	return (xlrec->flags & XLH_INSERT_IS_SPECULATIVE) != 0;
}

/*
 * fb_record_is_super_delete
 *    WAL helper.
 */

static bool
fb_record_is_super_delete(const FbRecordRef *record)
{
	const xl_heap_delete *xlrec;

	if (record == NULL || record->kind != FB_WAL_RECORD_HEAP_DELETE ||
		record->main_data_len < SizeOfHeapDelete || record->main_data == NULL)
		return false;

	xlrec = (const xl_heap_delete *) record->main_data;
	return (xlrec->flags & XLH_DELETE_IS_SUPER) != 0;
}

static bool
fb_wal_xid_committed_after_target(const FbWalRecordIndex *index,
								  TransactionId xid)
{
	FbXidStatusEntry *status_entry;

	if (index == NULL || !TransactionIdIsValid(xid))
		return false;

	status_entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													&xid,
													HASH_FIND,
													NULL);
	if (status_entry == NULL)
		return false;

	return status_entry->status == FB_WAL_XID_COMMITTED &&
		status_entry->commit_ts > index->target_ts &&
		status_entry->commit_ts <= index->query_now_ts;
}

static void
fb_count_only_note_metadata_record(FbWalIndexBuildState *state,
								   XLogReaderState *reader,
								   FbWalRecordKind kind)
{
	FbCountOnlyXidEntry *entry;
	TransactionId xid;
	bool found = false;
	const xl_heap_insert *insert_rec;
	const xl_heap_multi_insert *multi_insert_rec;
	const xl_heap_delete *delete_rec;

	if (state == NULL || reader == NULL || state->count_only_xids == NULL)
		return;
	if (!fb_record_touches_main_relation(reader, state->info))
		return;

	xid = fb_record_xid(reader);
	if (!TransactionIdIsValid(xid))
		return;

	entry = (FbCountOnlyXidEntry *) hash_search(state->count_only_xids,
												&xid,
												HASH_ENTER,
												&found);
	if (!found)
		MemSet(((char *) entry) + sizeof(TransactionId),
			   0,
			   sizeof(*entry) - sizeof(TransactionId));

	switch (kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
			insert_rec = (const xl_heap_insert *) XLogRecGetData(reader);
			if (XLogRecGetDataLen(reader) < SizeOfHeapInsert ||
				(insert_rec->flags & XLH_INSERT_IS_SPECULATIVE) != 0)
				return;
			entry->target_record_count++;
			entry->insert_row_count++;
			break;
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			multi_insert_rec = (const xl_heap_multi_insert *) XLogRecGetData(reader);
			if (XLogRecGetDataLen(reader) < SizeOfHeapMultiInsert ||
				(multi_insert_rec->flags & XLH_INSERT_IS_SPECULATIVE) != 0)
				return;
			entry->target_record_count++;
			entry->insert_row_count += multi_insert_rec->ntuples;
			break;
		case FB_WAL_RECORD_HEAP_DELETE:
			delete_rec = (const xl_heap_delete *) XLogRecGetData(reader);
			if (XLogRecGetDataLen(reader) < SizeOfHeapDelete ||
				(delete_rec->flags & XLH_DELETE_IS_SUPER) != 0)
				return;
			entry->target_record_count++;
			entry->delete_row_count++;
			break;
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
			entry->target_record_count++;
			entry->update_record_count++;
			break;
		default:
			break;
	}
}

static void
fb_count_only_finalize_index(FbWalIndexBuildState *state)
{
	HASH_SEQ_STATUS seq;
	FbCountOnlyXidEntry *entry;

	if (state == NULL || state->index == NULL || state->count_only_xids == NULL)
		return;

	hash_seq_init(&seq, state->count_only_xids);
	while ((entry = (FbCountOnlyXidEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (!fb_wal_xid_committed_after_target(state->index, entry->xid))
			continue;

		state->index->target_record_count += entry->target_record_count;
		state->index->target_insert_count += entry->insert_row_count;
		state->index->target_delete_count += entry->delete_row_count;
		state->index->target_update_count += entry->update_record_count;
	}
}

static bool
fb_record_touches_main_relation(XLogReaderState *reader,
								const FbRelationInfo *info)
{
	int block_id;

	for (block_id = 0; block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader); block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum, &blkno))
			continue;
		(void) blkno;
		if (forknum != MAIN_FORKNUM)
			continue;
		if (RelFileLocatorEquals(locator, info->locator))
			return true;
	}

	return false;
}

static void
fb_index_note_count_only_record(XLogReaderState *reader,
								const FbRelationInfo *info,
								FbWalRecordKind kind,
								FbWalRecordIndex *index)
{
	const xl_heap_insert *insert_rec;
	const xl_heap_multi_insert *multi_insert_rec;
	const xl_heap_delete *delete_rec;

	if (reader == NULL || info == NULL || index == NULL)
		return;
	if (reader->ReadRecPtr < index->anchor_redo_lsn)
		return;
	if (!fb_record_touches_main_relation(reader, info))
		return;
	if (!fb_wal_xid_committed_after_target(index, fb_record_xid(reader)))
		return;

	switch (kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
			insert_rec = (const xl_heap_insert *) XLogRecGetData(reader);
			if (XLogRecGetDataLen(reader) < SizeOfHeapInsert ||
				(insert_rec->flags & XLH_INSERT_IS_SPECULATIVE) != 0)
				return;
			index->target_record_count++;
			index->target_insert_count++;
			break;
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			multi_insert_rec = (const xl_heap_multi_insert *) XLogRecGetData(reader);
			if (XLogRecGetDataLen(reader) < SizeOfHeapMultiInsert ||
				(multi_insert_rec->flags & XLH_INSERT_IS_SPECULATIVE) != 0)
				return;
			index->target_record_count++;
			index->target_insert_count += multi_insert_rec->ntuples;
			break;
		case FB_WAL_RECORD_HEAP_DELETE:
			delete_rec = (const xl_heap_delete *) XLogRecGetData(reader);
			if (XLogRecGetDataLen(reader) < SizeOfHeapDelete ||
				(delete_rec->flags & XLH_DELETE_IS_SUPER) != 0)
				return;
			index->target_record_count++;
			index->target_delete_count++;
			break;
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
			index->target_record_count++;
			index->target_update_count++;
			break;
		default:
			break;
	}
}

static void
fb_wal_set_record_status(const FbWalRecordIndex *index, FbRecordRef *record)
{
	FbXidStatusEntry *status_entry;

	if (record == NULL)
		return;

	record->commit_ts = 0;
	record->commit_lsn = InvalidXLogRecPtr;
	record->aborted = false;
	record->committed_after_target = false;
	record->committed_before_target = false;

	if (index == NULL || record->lsn < index->anchor_redo_lsn)
		return;

	status_entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													&record->xid,
													HASH_FIND, NULL);
	if (status_entry == NULL)
		return;

	record->commit_ts = status_entry->commit_ts;
	record->commit_lsn = status_entry->commit_lsn;
	record->aborted = (status_entry->status == FB_WAL_XID_ABORTED);
	record->committed_after_target =
		(status_entry->status == FB_WAL_XID_COMMITTED &&
		 status_entry->commit_ts > index->target_ts &&
		 status_entry->commit_ts <= index->query_now_ts);
	record->committed_before_target =
		(status_entry->status == FB_WAL_XID_COMMITTED &&
		 status_entry->commit_ts <= index->target_ts);
}

static void
fb_wal_record_materializer_init(FbWalRecordMaterializer *materializer,
								 const FbWalRecordIndex *index)
{
	if (materializer == NULL)
		return;

	MemSet(materializer, 0, sizeof(*materializer));
	materializer->index = index;
	if (index == NULL || index->resolved_segments == NULL ||
		index->resolved_segment_count == 0)
		return;

	materializer->info.locator = index->target_locator;
	materializer->info.toast_locator = index->toast_locator;
	materializer->info.has_toast_locator = index->has_toast_locator;

	materializer->ctx.timeline_id = index->resolved_segments[0].timeline_id;
	materializer->ctx.wal_seg_size = index->wal_seg_size;
	materializer->ctx.resolved_segments = (void *) index->resolved_segments;
	materializer->ctx.resolved_segment_count = index->resolved_segment_count;
	materializer->ctx.current_segment_may_hit = true;

	materializer->private.timeline_id = materializer->ctx.timeline_id;
	materializer->private.endptr = InvalidXLogRecPtr;
	materializer->private.current_file = -1;
	materializer->private.open_pattern = FB_WAL_OPEN_SPARSE;
	materializer->private.ctx = &materializer->ctx;
}

static void
fb_wal_record_materializer_note_usage(FbWalRecordMaterializer *materializer)
{
	if (materializer == NULL)
		return;

	if (!materializer->initialized)
	{
		materializer->reset_count++;
		materializer->initialized = true;
	}
	else
		materializer->reuse_count++;
}

static void
fb_wal_record_materializer_ensure_reader(FbWalRecordMaterializer *materializer)
{
	if (materializer == NULL || materializer->reader != NULL)
		return;
	if (materializer->index == NULL || materializer->index->resolved_segments == NULL ||
		materializer->index->resolved_segment_count == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot materialize WAL record without resolved segments")));

#if PG_VERSION_NUM < 130000
	materializer->reader = XLogReaderAllocate(materializer->index->wal_seg_size,
											  fb_wal_read_page,
											  &materializer->private);
#else
	materializer->archive_dir = fb_get_effective_archive_dir();
	materializer->reader = XLogReaderAllocate(materializer->index->wal_seg_size,
											  materializer->archive_dir,
											  XL_ROUTINE(.page_read = fb_wal_read_page,
														 .segment_open = fb_wal_open_segment,
														 .segment_close = fb_wal_close_segment),
											  &materializer->private);
#endif
	if (materializer->reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a reusable WAL reader.")));
}

static void
fb_wal_record_materializer_destroy(FbWalRecordMaterializer *materializer)
{
	const FbWalRecordIndex *index;

	if (materializer == NULL)
		return;

	index = materializer->index;
	if (materializer->reader != NULL)
		XLogReaderFree(materializer->reader);
	fb_wal_close_private_file(&materializer->private);
	if (materializer->archive_dir != NULL)
		pfree(materializer->archive_dir);
	if (index != NULL)
	{
		((FbWalRecordIndex *) index)->record_materializer_resets +=
			materializer->reset_count;
		((FbWalRecordIndex *) index)->record_materializer_reuses +=
			materializer->reuse_count;
		((FbWalRecordIndex *) index)->locator_stub_materializations +=
			materializer->locator_stub_materializations;
		((FbWalRecordIndex *) index)->deferred_payload_materializations +=
			materializer->deferred_payload_materializations;
	}
}

static void
fb_wal_record_materializer_load_record(FbWalRecordMaterializer *materializer,
										XLogRecPtr record_lsn,
										FbRecordRef *record_out,
										bool include_payload)
{
	if (materializer != NULL)
		materializer->locator_stub_materializations++;
	fb_wal_load_record_by_lsn(materializer != NULL ? materializer->index : NULL,
							  record_lsn,
							  record_out,
							  include_payload,
							  materializer);
}

static void
fb_wal_deserialize_record(const FbWalRecordIndex *index,
						  const char *data,
						  Size len,
						  FbRecordRef *record)
{
	FbSerializedRecordHeader hdr;
	const char *ptr;
	int block_index;

	if (record == NULL)
		return;

	MemSet(record, 0, sizeof(*record));
	if (data == NULL || len < sizeof(hdr))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("fb WAL record spool entry is truncated")));

	memcpy(&hdr, data, sizeof(hdr));
	ptr = data + sizeof(hdr);

	record->kind = hdr.kind;
	record->lsn = hdr.lsn;
	record->end_lsn = hdr.end_lsn;
	record->xid = hdr.xid;
	record->info = hdr.info;
	record->init_page = hdr.init_page;
	record->block_count = hdr.block_count;
	record->main_data_len = hdr.main_data_len;
	if (!hdr.payload_deferred && record->main_data_len > 0)
	{
		record->main_data = (char *) ptr;
		ptr += record->main_data_len;
	}

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		FbRecordBlockRef *dst = &record->blocks[block_index];
		const FbSerializedRecordBlockRef *src = &hdr.blocks[block_index];

		dst->in_use = src->in_use;
		dst->block_id = src->block_id;
		dst->locator = src->locator;
		dst->forknum = src->forknum;
		dst->blkno = src->blkno;
		dst->is_main_relation = src->is_main_relation;
		dst->is_toast_relation = src->is_toast_relation;
		dst->has_image = src->has_image;
		dst->apply_image = src->apply_image;
		dst->has_data = src->has_data;
		dst->data_len = src->data_len;
		if (!hdr.payload_deferred && dst->has_image)
		{
			dst->image = (char *) ptr;
			ptr += BLCKSZ;
		}
		if (!hdr.payload_deferred && dst->has_data && dst->data_len > 0)
		{
			dst->data = (char *) ptr;
			ptr += dst->data_len;
		}
	}

	fb_wal_set_record_status(index, record);
}

static void
fb_wal_materialize_record_payload(const FbWalRecordIndex *index,
								  FbRecordRef *record,
								  FbWalRecordMaterializer *materializer)
{
	FbWalRecordMaterializer local_materializer;
	FbWalRecordMaterializer *active_materializer = materializer;
	XLogReaderState *reader;
	XLogRecord *xlog_record;
	char *errormsg = NULL;
	int block_index;
	bool destroy_local = false;

	if (index == NULL || record == NULL)
		return;
	if (!fb_record_ref_needs_payload_materialization(record))
		return;
	if (XLogRecPtrIsInvalid(record->lsn))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot materialize deferred WAL payload without a record LSN")));

	if (active_materializer == NULL)
	{
		fb_wal_record_materializer_init(&local_materializer, index);
		active_materializer = &local_materializer;
		destroy_local = true;
	}

	active_materializer->deferred_payload_materializations++;
	fb_wal_record_materializer_note_usage(active_materializer);
	fb_wal_record_materializer_ensure_reader(active_materializer);
	active_materializer->ctx.start_lsn = record->lsn;
	active_materializer->ctx.end_lsn = record->end_lsn;
	active_materializer->private.endptr = record->end_lsn;
	active_materializer->private.endptr_reached = false;
	reader = active_materializer->reader;

#if PG_VERSION_NUM < 130000
	xlog_record = XLogReadRecord(reader, record->lsn, &errormsg);
#else
	XLogBeginRead(reader, record->lsn);
	xlog_record = XLogReadRecord(reader, &errormsg);
#endif
	if (xlog_record == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("deferred WAL payload read failed at %X/%08X",
						LSN_FORMAT_ARGS(record->lsn)),
				 errdetail("%s", errormsg != NULL ? errormsg :
							   "WAL reader returned no record for a deferred payload entry.")));
	if (reader->ReadRecPtr != record->lsn ||
		reader->EndRecPtr != record->end_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("deferred WAL payload desynchronized at %X/%08X",
						LSN_FORMAT_ARGS(record->lsn)),
				 errdetail("Expected %X/%08X-%X/%08X but reader returned %X/%08X-%X/%08X.",
							   LSN_FORMAT_ARGS(record->lsn),
							   LSN_FORMAT_ARGS(record->end_lsn),
							   LSN_FORMAT_ARGS(reader->ReadRecPtr),
							   LSN_FORMAT_ARGS(reader->EndRecPtr))));

	if (record->main_data_len > 0 && record->main_data == NULL)
	{
		if (XLogRecGetDataLen(reader) != record->main_data_len)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("deferred WAL main-data length mismatch at %X/%08X",
							LSN_FORMAT_ARGS(record->lsn))));
		record->main_data = fb_copy_bytes(XLogRecGetData(reader), record->main_data_len);
	}

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		FbRecordBlockRef *block_ref = &record->blocks[block_index];

		if (block_ref->has_image &&
			block_ref->apply_image &&
			block_ref->image == NULL)
		{
			char page[BLCKSZ];

			if (!XLogRecHasBlockImage(reader, block_ref->block_id) ||
				!RestoreBlockImage(reader, block_ref->block_id, page))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("deferred WAL page image missing at %X/%08X",
								LSN_FORMAT_ARGS(record->lsn))));
			block_ref->image = fb_copy_bytes(page, BLCKSZ);
		}

		if (block_ref->has_data &&
			block_ref->data_len > 0 &&
			block_ref->data == NULL)
		{
			Size datalen = 0;
			char *data = XLogRecGetBlockData(reader, block_ref->block_id, &datalen);

			if (data == NULL || datalen != block_ref->data_len)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("deferred WAL block-data mismatch at %X/%08X",
								LSN_FORMAT_ARGS(record->lsn))));
			block_ref->data = fb_copy_bytes(data, datalen);
		}
	}

	if (destroy_local)
		fb_wal_record_materializer_destroy(&local_materializer);
}

FbWalRecordCursor *
fb_wal_record_cursor_open(const FbWalRecordIndex *index, FbSpoolDirection direction)
{
	FbWalRecordCursor *cursor;

	cursor = palloc0(sizeof(*cursor));
	cursor->index = index;
	cursor->direction = direction;
	if (index != NULL)
	{
		cursor->materializer = palloc0(sizeof(*cursor->materializer));
		fb_wal_record_materializer_init(cursor->materializer, index);
	}
	initStringInfo(&cursor->raw);
	if (index != NULL)
	{
		cursor->head_count = fb_spool_log_count(index->record_log);
		if (cursor->head_count > 0)
			cursor->head_cursor = fb_spool_cursor_open(index->record_log, direction);
		if (fb_spool_log_count(index->record_tail_log) > 0)
			cursor->tail_cursor = fb_spool_cursor_open(index->record_tail_log, direction);

		if (direction == FB_SPOOL_FORWARD)
		{
			cursor->reading_tail = (cursor->head_cursor == NULL);
			cursor->active_cursor = cursor->reading_tail ? cursor->tail_cursor :
				cursor->head_cursor;
		}
		else
		{
			cursor->reading_tail = (cursor->tail_cursor != NULL);
			cursor->active_cursor = cursor->reading_tail ? cursor->tail_cursor :
				cursor->head_cursor;
		}
	}
	return cursor;
}

static bool
fb_wal_record_cursor_read_mode(FbWalRecordCursor *cursor,
							   FbRecordRef *record,
							   uint32 *record_index,
							   FbWalCursorReadMode mode)
{
	uint32 item_index = 0;

	if (cursor == NULL)
		return false;

	if (cursor->current_materialized)
	{
		fb_record_release_temp(&cursor->current);
		cursor->current_materialized = false;
	}

	while (cursor->active_cursor != NULL)
	{
		if (fb_spool_cursor_read(cursor->active_cursor, &cursor->raw, &item_index))
			break;

		if (cursor->direction == FB_SPOOL_FORWARD)
		{
			if (!cursor->reading_tail && cursor->tail_cursor != NULL)
			{
				cursor->reading_tail = true;
				cursor->active_cursor = cursor->tail_cursor;
				continue;
			}
		}
		else
		{
			if (cursor->reading_tail && cursor->head_cursor != NULL)
			{
				cursor->reading_tail = false;
				cursor->active_cursor = cursor->head_cursor;
				continue;
			}
		}

		return false;
	}

	if (cursor->active_cursor == NULL)
		return false;

	if (cursor->reading_tail)
		item_index += cursor->head_count;

	if (cursor->raw.len >= sizeof(FbSerializedRecordHeader) &&
		((const FbSerializedRecordHeader *) cursor->raw.data)->locator_only)
	{
		const FbSerializedRecordHeader *hdr =
			(const FbSerializedRecordHeader *) cursor->raw.data;

		fb_wal_record_materializer_load_record(cursor->materializer,
											  hdr->lsn,
											  &cursor->current,
											  mode == FB_WAL_CURSOR_READ_FULL);
		cursor->current_materialized = true;
	}
	else
	{
		fb_wal_deserialize_record(cursor->index,
								  cursor->raw.data,
								  cursor->raw.len,
								  &cursor->current);
		if (mode == FB_WAL_CURSOR_READ_FULL &&
			fb_record_ref_needs_payload_materialization(&cursor->current))
		{
			fb_wal_materialize_record_payload(cursor->index,
											  &cursor->current,
											  cursor->materializer);
			cursor->current_materialized = true;
		}
	}
	if (record != NULL)
		*record = cursor->current;
	if (record_index != NULL)
		*record_index = item_index;
	return true;
}

bool
fb_wal_record_cursor_seek(FbWalRecordCursor *cursor, uint32 record_index)
{
	uint32 tail_index;
	uint32 tail_count;

	if (cursor == NULL || cursor->direction != FB_SPOOL_FORWARD)
		return false;

	tail_count = (cursor->index == NULL) ? 0 :
		fb_spool_log_count(cursor->index->record_tail_log);
	if (record_index > cursor->head_count + tail_count)
		return false;

	if (record_index < cursor->head_count ||
		(record_index == cursor->head_count && cursor->head_cursor != NULL && cursor->tail_cursor == NULL))
	{
		cursor->reading_tail = false;
		cursor->active_cursor = cursor->head_cursor;
		return (cursor->head_cursor != NULL) &&
			fb_spool_cursor_seek_item(cursor->head_cursor, record_index);
	}

	tail_index = record_index - cursor->head_count;
	cursor->reading_tail = true;
	cursor->active_cursor = cursor->tail_cursor;
	return (cursor->tail_cursor != NULL) &&
		fb_spool_cursor_seek_item(cursor->tail_cursor, tail_index);
}

bool
fb_wal_record_cursor_read(FbWalRecordCursor *cursor,
						  FbRecordRef *record,
						  uint32 *record_index)
{
	return fb_wal_record_cursor_read_mode(cursor,
										  record,
										  record_index,
										  FB_WAL_CURSOR_READ_FULL);
}

bool
fb_wal_record_cursor_read_skeleton(FbWalRecordCursor *cursor,
								   FbRecordRef *record,
								   uint32 *record_index)
{
	return fb_wal_record_cursor_read_mode(cursor,
										  record,
										  record_index,
										  FB_WAL_CURSOR_READ_SKELETON);
}

void
fb_wal_record_cursor_close(FbWalRecordCursor *cursor)
{
	if (cursor == NULL)
		return;

	if (cursor->current_materialized)
		fb_record_release_temp(&cursor->current);

	if (cursor->head_cursor != NULL)
		fb_spool_cursor_close(cursor->head_cursor);
	if (cursor->tail_cursor != NULL)
		fb_spool_cursor_close(cursor->tail_cursor);
	if (cursor->raw.data != NULL)
		pfree(cursor->raw.data);
	if (cursor->materializer != NULL)
	{
		fb_wal_record_materializer_destroy(cursor->materializer);
		pfree(cursor->materializer);
	}
	pfree(cursor);
}

bool
fb_wal_record_load(const FbWalRecordIndex *index,
				   uint32 record_index,
				   FbRecordRef *record)
{
	FbWalRecordCursor *cursor;
	bool ok;

	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_FORWARD);
	if (!fb_wal_record_cursor_seek(cursor, record_index))
	{
		fb_wal_record_cursor_close(cursor);
		return false;
	}
	ok = fb_wal_record_cursor_read(cursor, record, NULL);
	fb_wal_record_cursor_close(cursor);
	return ok;
}

static bool
fb_wal_build_heap_record_ref(XLogReaderState *reader,
							 const FbRelationInfo *info,
							 FbWalRecordKind kind,
							 FbRecordRef *record_out,
							 bool include_payload)
{
	int block_count = 0;
	int block_id;

	if (reader == NULL || info == NULL || record_out == NULL)
		return false;

	MemSet(record_out, 0, sizeof(*record_out));
	record_out->kind = kind;
	record_out->lsn = reader->ReadRecPtr;
	record_out->end_lsn = reader->EndRecPtr;
	record_out->xid = fb_record_xid(reader);
	record_out->info = XLogRecGetInfo(reader);
	record_out->init_page = ((XLogRecGetInfo(reader) & XLOG_HEAP_INIT_PAGE) != 0);
	record_out->main_data_len = XLogRecGetDataLen(reader);
	if (include_payload && record_out->main_data_len > 0)
		record_out->main_data = fb_copy_bytes(XLogRecGetData(reader),
											  XLogRecGetDataLen(reader));

	for (block_id = 0;
		 block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader) &&
		 block_count < FB_WAL_MAX_BLOCK_REFS;
		 block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum, &blkno))
			continue;
		if (forknum != MAIN_FORKNUM)
			continue;
		if (!fb_locator_matches_relation(&locator, info))
			continue;

		fb_fill_record_block_ref(&record_out->blocks[block_count],
								 reader,
								 block_id,
								 info,
								 include_payload);
		block_count++;
	}

	record_out->block_count = block_count;
	return block_count > 0;
}

static bool
fb_wal_build_xlog_fpi_record_ref(XLogReaderState *reader,
								 const FbRelationInfo *info,
								 FbWalRecordKind kind,
								 FbRecordRef *record_out,
								 bool include_payload)
{
	int block_count = 0;
	int block_id;

	if (reader == NULL || info == NULL || record_out == NULL)
		return false;

	MemSet(record_out, 0, sizeof(*record_out));
	record_out->kind = kind;
	record_out->lsn = reader->ReadRecPtr;
	record_out->end_lsn = reader->EndRecPtr;
	record_out->xid = InvalidTransactionId;
	record_out->info = XLogRecGetInfo(reader);
	record_out->init_page = false;

	for (block_id = 0;
		 block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader) &&
		 block_count < FB_WAL_MAX_BLOCK_REFS;
		 block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum, &blkno))
			continue;
		if (forknum != MAIN_FORKNUM)
			continue;
		if (!fb_locator_matches_relation(&locator, info))
			continue;

		fb_fill_record_block_ref(&record_out->blocks[block_count],
								 reader,
								 block_id,
								 info,
								 include_payload);
		block_count++;
	}

	record_out->block_count = block_count;
	return block_count > 0;
}

static void
fb_wal_load_record_by_lsn(const FbWalRecordIndex *index,
						  XLogRecPtr record_lsn,
						  FbRecordRef *record_out,
						  bool include_payload,
						  FbWalRecordMaterializer *materializer)
{
	FbWalRecordMaterializer local_materializer;
	FbWalRecordMaterializer *active_materializer = materializer;
	XLogReaderState *reader;
	XLogRecord *xlog_record;
	uint8 rmid;
	char *errormsg = NULL;
	bool built = false;
	bool destroy_local = false;

	if (index == NULL || record_out == NULL)
		return;
	if (XLogRecPtrIsInvalid(record_lsn))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("cannot materialize WAL record without a start LSN")));

	if (active_materializer == NULL)
	{
		fb_wal_record_materializer_init(&local_materializer, index);
		active_materializer = &local_materializer;
		destroy_local = true;
	}

	fb_wal_record_materializer_note_usage(active_materializer);
	fb_wal_record_materializer_ensure_reader(active_materializer);
	active_materializer->ctx.start_lsn = record_lsn;
	active_materializer->ctx.end_lsn = InvalidXLogRecPtr;
	active_materializer->private.endptr = InvalidXLogRecPtr;
	active_materializer->private.endptr_reached = false;
	reader = active_materializer->reader;

#if PG_VERSION_NUM < 130000
	xlog_record = XLogReadRecord(reader, record_lsn, &errormsg);
#else
	XLogBeginRead(reader, record_lsn);
	xlog_record = XLogReadRecord(reader, &errormsg);
#endif
	if (xlog_record == NULL || reader->ReadRecPtr != record_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("locator-only WAL record read failed at %X/%08X",
						LSN_FORMAT_ARGS(record_lsn)),
				 errdetail("%s", errormsg != NULL ? errormsg :
							   "WAL reader returned no record for a locator-only spool entry.")));

	rmid = XLogRecGetRmid(reader);
	MemSet(record_out, 0, sizeof(*record_out));

	if (rmid == RM_HEAP_ID)
	{
		uint8 heap_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		switch (heap_code)
		{
			case XLOG_HEAP_INSERT:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_INSERT,
													 record_out,
													 include_payload);
				break;
			case XLOG_HEAP_DELETE:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_DELETE,
													 record_out,
													 include_payload);
				break;
			case XLOG_HEAP_UPDATE:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_UPDATE,
													 record_out,
													 include_payload);
				break;
			case XLOG_HEAP_HOT_UPDATE:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_HOT_UPDATE,
													 record_out,
													 include_payload);
				break;
			case XLOG_HEAP_CONFIRM:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_CONFIRM,
													 record_out,
													 include_payload);
				break;
			case XLOG_HEAP_LOCK:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_LOCK,
													 record_out,
													 include_payload);
				break;
			case XLOG_HEAP_INPLACE:
				built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_HEAP_INPLACE,
													 record_out,
													 include_payload);
				break;
			default:
				break;
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

#if PG_VERSION_NUM >= 170000
		if (info_code == XLOG_HEAP2_PRUNE_ON_ACCESS ||
			info_code == XLOG_HEAP2_PRUNE_VACUUM_SCAN ||
			info_code == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP)
			built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
												 FB_WAL_RECORD_HEAP2_PRUNE,
												 record_out,
												 include_payload);
#elif PG_VERSION_NUM >= 140000
		if (info_code == XLOG_HEAP2_PRUNE)
			built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
												 FB_WAL_RECORD_HEAP2_PRUNE,
												 record_out,
												 include_payload);
#else
		if (info_code == XLOG_HEAP2_CLEAN)
			built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
												 FB_WAL_RECORD_HEAP2_PRUNE,
												 record_out,
												 include_payload);
#endif
		else if (info_code == XLOG_HEAP2_VISIBLE)
			built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
												 FB_WAL_RECORD_HEAP2_VISIBLE,
												 record_out,
												 include_payload);
		else if (info_code == XLOG_HEAP2_MULTI_INSERT)
			built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
												 FB_WAL_RECORD_HEAP2_MULTI_INSERT,
												 record_out,
												 include_payload);
		else if (info_code == XLOG_HEAP2_LOCK_UPDATED)
			built = fb_wal_build_heap_record_ref(reader, &active_materializer->info,
												 FB_WAL_RECORD_HEAP2_LOCK_UPDATED,
												 record_out,
												 include_payload);
	}
	else if (rmid == RM_XLOG_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_FPI)
			built = fb_wal_build_xlog_fpi_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_XLOG_FPI,
													 record_out,
													 include_payload);
		else if (info_code == XLOG_FPI_FOR_HINT)
			built = fb_wal_build_xlog_fpi_record_ref(reader, &active_materializer->info,
													 FB_WAL_RECORD_XLOG_FPI_FOR_HINT,
													 record_out,
													 include_payload);
	}

	if (!built)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("locator-only WAL record could not be materialized at %X/%08X",
						LSN_FORMAT_ARGS(record_lsn))));

	fb_wal_set_record_status(index, record_out);
	if (destroy_local)
		fb_wal_record_materializer_destroy(&local_materializer);
}

bool
fb_wal_decode_record_ref(XLogReaderState *reader,
						   const FbRelationInfo *info,
						   FbRecordRef *record_out)
{
	uint8 rmid;

	if (reader == NULL || info == NULL || record_out == NULL)
		return false;

	rmid = XLogRecGetRmid(reader);

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (!fb_heap_record_matches_target(reader, info))
			return false;

		switch (info_code)
		{
			case XLOG_HEAP_INSERT:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_INSERT,
													 record_out,
													 true);
			case XLOG_HEAP_DELETE:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_DELETE,
													 record_out,
													 true);
			case XLOG_HEAP_UPDATE:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_UPDATE,
													 record_out,
													 true);
			case XLOG_HEAP_HOT_UPDATE:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_HOT_UPDATE,
													 record_out,
													 true);
			case XLOG_HEAP_CONFIRM:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_CONFIRM,
													 record_out,
													 true);
			case XLOG_HEAP_LOCK:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_LOCK,
													 record_out,
													 true);
			case XLOG_HEAP_INPLACE:
				return fb_wal_build_heap_record_ref(reader, info,
													 FB_WAL_RECORD_HEAP_INPLACE,
													 record_out,
													 true);
			default:
				return false;
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (
#if PG_VERSION_NUM >= 170000
			(info_code == XLOG_HEAP2_PRUNE_ON_ACCESS ||
			 info_code == XLOG_HEAP2_PRUNE_VACUUM_SCAN ||
			 info_code == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP) &&
#elif PG_VERSION_NUM >= 140000
			info_code == XLOG_HEAP2_PRUNE &&
#else
			info_code == XLOG_HEAP2_CLEAN &&
#endif
			fb_record_touches_relation(reader, info))
			return fb_wal_build_heap_record_ref(reader, info,
												 FB_WAL_RECORD_HEAP2_PRUNE,
												 record_out,
												 true);
		if (info_code == XLOG_HEAP2_VISIBLE &&
			fb_record_touches_relation(reader, info))
			return fb_wal_build_heap_record_ref(reader, info,
												 FB_WAL_RECORD_HEAP2_VISIBLE,
												 record_out,
												 true);
		if (info_code == XLOG_HEAP2_MULTI_INSERT &&
			fb_record_touches_relation(reader, info))
			return fb_wal_build_heap_record_ref(reader, info,
												 FB_WAL_RECORD_HEAP2_MULTI_INSERT,
												 record_out,
												 true);
		if (info_code == XLOG_HEAP2_LOCK_UPDATED &&
			fb_record_touches_relation(reader, info))
			return fb_wal_build_heap_record_ref(reader, info,
												 FB_WAL_RECORD_HEAP2_LOCK_UPDATED,
												 record_out,
												 true);
		return false;
	}
	else if (rmid == RM_XLOG_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if ((info_code == XLOG_FPI || info_code == XLOG_FPI_FOR_HINT) &&
			fb_record_touches_relation(reader, info))
			return fb_wal_build_xlog_fpi_record_ref(reader,
													  info,
													  info_code == XLOG_FPI ?
													  FB_WAL_RECORD_XLOG_FPI :
													  FB_WAL_RECORD_XLOG_FPI_FOR_HINT,
													  record_out,
													  true);
		return false;
	}

	return false;
}

void
fb_wal_release_record(FbRecordRef *record)
{
	fb_record_release_temp(record);
}

static void
fb_wal_reset_record_stats(FbWalRecordIndex *index)
{
	if (index == NULL)
		return;

	index->total_record_count = 0;
	index->kept_record_count = 0;
	index->target_record_count = 0;
	index->target_commit_count = 0;
	index->target_abort_count = 0;
	index->target_insert_count = 0;
	index->target_delete_count = 0;
	index->target_update_count = 0;
	index->payload_covered_segment_count = 0;
	index->payload_scanned_record_count = 0;
	index->payload_kept_record_count = 0;
	if (index->precomputed_missing_blocks != NULL)
	{
		hash_destroy(index->precomputed_missing_blocks);
		index->precomputed_missing_blocks = NULL;
	}
	index->precomputed_missing_block_count = 0;
}

static HTAB *
fb_wal_create_precomputed_missing_block_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbWalBlockKey);
	ctl.entrysize = sizeof(FbWalPrecomputedMissingBlock);
	return hash_create("fb wal precomputed missing blocks",
					   128,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS);
}

static HTAB *
fb_wal_create_block_init_state_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbWalBlockKey);
	ctl.entrysize = sizeof(FbWalBlockInitState);
	return hash_create("fb wal block init state",
					   128,
					   &ctl,
					   HASH_ELEM | HASH_BLOBS);
}

static bool
fb_wal_record_block_has_wal_id(const FbRecordRef *record,
							   int block_index,
							   uint8 wal_block_id)
{
	const FbRecordBlockRef *block_ref;

	if (record == NULL ||
		block_index < 0 ||
		block_index >= record->block_count)
		return false;

	block_ref = &record->blocks[block_index];
	return block_ref->in_use && block_ref->block_id == wal_block_id;
}

bool
fb_wal_record_block_materializes_page(const FbRecordRef *record, int block_index)
{
	const FbRecordBlockRef *block_ref;

	if (record == NULL ||
		block_index < 0 ||
		block_index >= record->block_count)
		return false;

	block_ref = &record->blocks[block_index];
	if (!block_ref->in_use)
		return false;
	if (fb_record_block_has_materializing_image(block_ref))
		return true;

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			return fb_wal_record_block_has_wal_id(record, block_index, 0) &&
				record->init_page;
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
			return fb_wal_record_block_has_wal_id(record, block_index, 0) &&
				record->init_page;
		default:
			break;
	}

	return false;
}

static bool
fb_wal_record_block_requires_initialized_page(const FbRecordRef *record,
											  int block_index)
{
	if (record == NULL ||
		block_index < 0 ||
		block_index >= record->block_count)
		return false;

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
		case FB_WAL_RECORD_HEAP_DELETE:
		case FB_WAL_RECORD_HEAP_CONFIRM:
		case FB_WAL_RECORD_HEAP_INPLACE:
		case FB_WAL_RECORD_HEAP2_VISIBLE:
		case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			return fb_wal_record_block_has_wal_id(record, block_index, 0);
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
			return fb_wal_record_block_has_wal_id(record, block_index, 0) ||
				fb_wal_record_block_has_wal_id(record, block_index, 1);
		case FB_WAL_RECORD_XLOG_FPI:
		case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
		case FB_WAL_RECORD_HEAP_LOCK:
		case FB_WAL_RECORD_HEAP2_PRUNE:
		case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
			return false;
	}

	return false;
}

static uint32
fb_wal_count_window_segments(const FbWalVisitWindow *windows,
							 uint32 window_count)
{
	uint32 total = 0;
	uint32 i;

	if (windows == NULL)
		return 0;

	for (i = 0; i < window_count; i++)
		total += windows[i].segment_count;

	return total;
}

static void
fb_wal_note_precomputed_missing_blocks(FbWalRecordIndex *index,
									   HTAB *block_states,
									   const FbRecordRef *record,
									   uint32 record_index)
{
	int block_index;

	if (index == NULL || block_states == NULL || record == NULL)
		return;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record->blocks[block_index];
		FbWalBlockKey key;
		FbWalBlockInitState *state;
		bool found = false;

		if (!block_ref->in_use)
			continue;

		key.locator = block_ref->locator;
		key.forknum = block_ref->forknum;
		key.blkno = block_ref->blkno;

		state = (FbWalBlockInitState *) hash_search(block_states,
													 &key,
													 HASH_ENTER,
													 &found);
		if (!found)
		{
			MemSet(state, 0, sizeof(*state));
			state->key = key;
		}

		if (state->missing_noted)
			continue;

		if (fb_wal_record_block_materializes_page(record, block_index))
		{
			state->initialized = true;
			continue;
		}

		if (fb_wal_record_block_requires_initialized_page(record, block_index) &&
			!state->initialized)
		{
			FbWalPrecomputedMissingBlock *missing;
			bool missing_found = false;

			if (index->precomputed_missing_blocks == NULL)
				index->precomputed_missing_blocks =
					fb_wal_create_precomputed_missing_block_hash();
			missing = (FbWalPrecomputedMissingBlock *)
				hash_search(index->precomputed_missing_blocks,
							&state->key,
							HASH_ENTER,
							&missing_found);
			if (!missing_found)
			{
				MemSet(missing, 0, sizeof(*missing));
				missing->key = state->key;
				missing->first_record_index = record_index;
				missing->first_record_lsn = record->lsn;
			}
			state->missing_noted = true;
		}
	}
}

static void
fb_wal_finalize_record_stats(FbWalRecordIndex *index)
{
	FbWalRecordCursor *cursor;
	FbRecordRef record;
	HTAB	   *block_states;
	uint32		record_index = 0;

	if (index == NULL || index->record_count == 0)
		return;

	fb_wal_reset_record_stats(index);
	index->precomputed_missing_blocks = fb_wal_create_precomputed_missing_block_hash();
	block_states = fb_wal_create_block_init_state_hash();
	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_FORWARD);
	while (fb_wal_record_cursor_read_skeleton(cursor, &record, &record_index))
	{
		fb_index_note_materialized_record(index, &record);
		fb_wal_note_precomputed_missing_blocks(index, block_states, &record,
											  record_index);
	}
	fb_wal_record_cursor_close(cursor);
	hash_destroy(block_states);
	if (index->precomputed_missing_blocks != NULL)
		index->precomputed_missing_block_count =
			hash_get_num_entries(index->precomputed_missing_blocks);
}

static void
fb_wal_enable_incremental_payload_stats(FbWalRecordIndex *index,
										FbWalIndexBuildState *state)
{
	if (index == NULL || state == NULL || state->payload_block_states != NULL)
		return;

	index->kept_record_count = 0;
	index->target_record_count = 0;
	index->target_insert_count = 0;
	index->target_delete_count = 0;
	index->target_update_count = 0;
	if (index->precomputed_missing_blocks != NULL)
	{
		hash_destroy(index->precomputed_missing_blocks);
		index->precomputed_missing_blocks = NULL;
	}
	index->precomputed_missing_block_count = 0;
	state->payload_block_states = fb_wal_create_block_init_state_hash();
}

/*
 * fb_copy_bytes
 *    WAL helper.
 */

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

static bool
fb_checkpoint_sidecar_load(const FbWalSegmentEntry *entry,
						   const char *meta_dir,
						   FbCheckpointSidecarEntry **entries_out,
						   uint32 *entry_count_out)
{
	struct stat st;
	uint64 file_identity_hash;
	FbCheckpointSidecarHeader hdr;
	FbCheckpointSidecarEntry *entries = NULL;
	char path[MAXPGPATH];
	int			fd;

	if (entries_out != NULL)
		*entries_out = NULL;
	if (entry_count_out != NULL)
		*entry_count_out = 0;
	if (entry == NULL || meta_dir == NULL || meta_dir[0] == '\0')
		return false;
	if (stat(entry->path, &st) != 0)
		return false;

	file_identity_hash = fb_sidecar_file_identity_hash(entry->path,
													  st.st_size,
													  st.st_mtime);
	snprintf(path, sizeof(path), "%s/checkpoint-%016llx.meta",
			 meta_dir, (unsigned long long) file_identity_hash);

	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;

	if (!fb_sidecar_read_bytes(fd, &hdr, sizeof(hdr)))
	{
		close(fd);
		return false;
	}

	if (hdr.magic != FB_CHECKPOINT_SIDECAR_MAGIC ||
		hdr.version != FB_CHECKPOINT_SIDECAR_VERSION ||
		hdr.file_identity_hash != file_identity_hash ||
		hdr.timeline_id != entry->timeline_id ||
		hdr.segno != entry->segno)
	{
		close(fd);
		return false;
	}

	if (hdr.checkpoint_count > 0)
	{
		entries = palloc(sizeof(FbCheckpointSidecarEntry) * hdr.checkpoint_count);
		if (!fb_sidecar_read_bytes(fd,
								   entries,
								   sizeof(FbCheckpointSidecarEntry) * hdr.checkpoint_count))
		{
			pfree(entries);
			close(fd);
			return false;
		}
	}

	close(fd);

	if (entries_out != NULL)
		*entries_out = entries;
	else if (entries != NULL)
		pfree(entries);
	if (entry_count_out != NULL)
		*entry_count_out = hdr.checkpoint_count;
	return true;
}

static bool
fb_checkpoint_sidecar_find_best(const FbWalSegmentEntry *entry,
								   const char *meta_dir,
								   TimestampTz target_ts,
								   uint32 *eligible_count_out,
								   bool *found_out,
								   FbCheckpointSidecarEntry *best_entry_out)
{
	struct stat st;
	uint64 file_identity_hash;
	FbCheckpointSidecarHeader hdr;
	char path[MAXPGPATH];
	uint32 eligible = 0;
	bool found = false;
	FbCheckpointSidecarEntry best_entry;
	int fd;
	uint32 i;

	if (eligible_count_out != NULL)
		*eligible_count_out = 0;
	if (found_out != NULL)
		*found_out = false;
	if (best_entry_out != NULL)
		MemSet(best_entry_out, 0, sizeof(*best_entry_out));
	if (entry == NULL || meta_dir == NULL || meta_dir[0] == '\0')
		return false;
	if (stat(entry->path, &st) != 0)
		return false;

	file_identity_hash = fb_sidecar_file_identity_hash(entry->path,
													  st.st_size,
													  st.st_mtime);
	snprintf(path, sizeof(path), "%s/checkpoint-%016llx.meta",
			 meta_dir, (unsigned long long) file_identity_hash);

	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;

	if (!fb_sidecar_read_bytes(fd, &hdr, sizeof(hdr)))
	{
		close(fd);
		return false;
	}

	if (hdr.magic != FB_CHECKPOINT_SIDECAR_MAGIC ||
		hdr.version != FB_CHECKPOINT_SIDECAR_VERSION ||
		hdr.file_identity_hash != file_identity_hash ||
		hdr.timeline_id != entry->timeline_id ||
		hdr.segno != entry->segno)
	{
		close(fd);
		return false;
	}

	for (i = 0; i < hdr.checkpoint_count; i++)
	{
		FbCheckpointSidecarEntry current;

		if (!fb_sidecar_read_bytes(fd, &current, sizeof(current)))
		{
			close(fd);
			return false;
		}

		if (current.checkpoint_ts > target_ts)
			continue;

		eligible++;
		if (!found ||
			current.checkpoint_ts > best_entry.checkpoint_ts ||
			(current.checkpoint_ts == best_entry.checkpoint_ts &&
			 current.redo_lsn >= best_entry.redo_lsn))
		{
			best_entry = current;
			found = true;
		}
	}

	close(fd);

	if (eligible_count_out != NULL)
		*eligible_count_out = eligible;
	if (found_out != NULL)
		*found_out = found;
	if (found && best_entry_out != NULL)
		*best_entry_out = best_entry;
	return true;
}

static void *
fb_checkpoint_hint_worker(void *arg)
{
	FbCheckpointHintTask *task = (FbCheckpointHintTask *) arg;
	int i;

	for (i = 0; i < task->index_count; i++)
	{
		uint32 eligible_count = 0;
		bool found = false;
		FbCheckpointSidecarEntry best_entry;

		if (!fb_checkpoint_sidecar_find_best(&task->segments[task->indexes[i]],
											 task->meta_dir,
											 task->target_ts,
											 &eligible_count,
											 &found,
											 &best_entry))
			continue;

		task->checkpoint_entry_count += eligible_count;
		if (!found)
			continue;

		if (!task->found ||
			best_entry.checkpoint_ts > task->best_entry.checkpoint_ts ||
			(best_entry.checkpoint_ts == task->best_entry.checkpoint_ts &&
			 best_entry.redo_lsn >= task->best_entry.redo_lsn))
		{
			task->best_entry = best_entry;
			task->best_segment_index = (uint32) task->indexes[i];
			task->found = true;
		}
	}

	return NULL;
}

static void
fb_checkpoint_sidecar_append(const FbWalSegmentEntry *entry,
							 TimestampTz checkpoint_ts,
							 XLogRecPtr checkpoint_lsn,
							 XLogRecPtr redo_lsn)
{
	char	   *meta_dir;
	FbCheckpointSidecarEntry *entries = NULL;
	uint32		entry_count = 0;
	uint32		i;
	bool		already_present = false;
	struct stat st;
	uint64		file_identity_hash;
	FbCheckpointSidecarHeader hdr;
	char		path[MAXPGPATH];
	int			fd;

	if (entry == NULL)
		return;
	if (stat(entry->path, &st) != 0)
		return;

	fb_runtime_ensure_initialized();
	meta_dir = fb_runtime_meta_dir();
	(void) fb_checkpoint_sidecar_load(entry, meta_dir, &entries, &entry_count);

	for (i = 0; i < entry_count; i++)
	{
		if (entries[i].checkpoint_lsn == checkpoint_lsn)
		{
			already_present = true;
			break;
		}
	}

	if (already_present)
	{
		if (entries != NULL)
			pfree(entries);
		pfree(meta_dir);
		return;
	}

	if (entries == NULL)
		entries = palloc(sizeof(FbCheckpointSidecarEntry) * (entry_count + 1));
	else
		entries = repalloc(entries,
						   sizeof(FbCheckpointSidecarEntry) * (entry_count + 1));
	entries[entry_count].checkpoint_ts = checkpoint_ts;
	entries[entry_count].checkpoint_lsn = checkpoint_lsn;
	entries[entry_count].redo_lsn = redo_lsn;
	entry_count++;

	file_identity_hash = fb_sidecar_file_identity_hash(entry->path,
													  st.st_size,
													  st.st_mtime);
	snprintf(path, sizeof(path), "%s/checkpoint-%016llx.meta",
			 meta_dir, (unsigned long long) file_identity_hash);

	MemSet(&hdr, 0, sizeof(hdr));
	hdr.magic = FB_CHECKPOINT_SIDECAR_MAGIC;
	hdr.version = FB_CHECKPOINT_SIDECAR_VERSION;
	hdr.file_identity_hash = file_identity_hash;
	hdr.timeline_id = entry->timeline_id;
	hdr.segno = entry->segno;
	hdr.checkpoint_count = entry_count;

	fd = open(path,
			  O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
			  S_IRUSR | S_IWUSR);
	if (fd >= 0)
	{
		(void) fb_sidecar_write_bytes(fd, &hdr, sizeof(hdr));
		if (entry_count > 0)
			(void) fb_sidecar_write_bytes(fd,
										  entries,
										  sizeof(FbCheckpointSidecarEntry) * entry_count);
		close(fd);
	}

	if (entries != NULL)
		pfree(entries);
	pfree(meta_dir);
}

static void
fb_maybe_seed_anchor_hint(FbWalScanContext *ctx)
{
	FbWalSegmentEntry *segments;
	pthread_t workers[FB_WAL_PARALLEL_MAX_WORKERS];
	bool worker_started[FB_WAL_PARALLEL_MAX_WORKERS];
	FbCheckpointHintTask tasks[FB_WAL_PARALLEL_MAX_WORKERS];
	FbCheckpointSidecarEntry best_entry;
	char	   *meta_dir;
	int		   *indexes = NULL;
	uint32		best_segment_index = 0;
	uint32		i;
	bool		found = false;
	int			worker_count = 1;
	int			chunk_size = 1;

	if (ctx == NULL || ctx->resolved_segment_count == 0 || ctx->resolved_segments == NULL)
		return;

	fb_runtime_ensure_initialized();
	meta_dir = fb_runtime_meta_dir();
	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	ctx->checkpoint_sidecar_entries = 0;
	indexes = palloc(sizeof(int) * ctx->resolved_segment_count);
	for (i = 0; i < ctx->resolved_segment_count; i++)
		indexes[i] = (int) i;

	if (ctx->parallel_workers > 0 && ctx->resolved_segment_count > 1)
	{
		worker_count = fb_parallel_worker_count((int) ctx->resolved_segment_count);
		chunk_size = ((int) ctx->resolved_segment_count + worker_count - 1) / worker_count;
	}

	for (i = 0; i < (uint32) worker_count; i++)
	{
		int start = (int) i * chunk_size;
		int end = Min(start + chunk_size, (int) ctx->resolved_segment_count);

		worker_started[i] = false;
		MemSet(&tasks[i], 0, sizeof(tasks[i]));
		tasks[i].segments = segments;
		tasks[i].indexes = indexes + start;
		tasks[i].index_count = end - start;
		tasks[i].target_ts = ctx->target_ts;
		strlcpy(tasks[i].meta_dir, meta_dir, sizeof(tasks[i].meta_dir));
		if (start >= end)
			continue;

		if (worker_count == 1 ||
			pthread_create(&workers[i], NULL, fb_checkpoint_hint_worker,
						   &tasks[i]) != 0)
			fb_checkpoint_hint_worker(&tasks[i]);
		else
			worker_started[i] = true;
	}

	for (i = 0; i < (uint32) worker_count; i++)
	{
		if (worker_started[i])
			pthread_join(workers[i], NULL);
	}

	for (i = 0; i < (uint32) worker_count; i++)
	{
		ctx->checkpoint_sidecar_entries += tasks[i].checkpoint_entry_count;
		if (!tasks[i].found)
			continue;
		if (!found ||
			tasks[i].best_entry.checkpoint_ts > best_entry.checkpoint_ts ||
			(tasks[i].best_entry.checkpoint_ts == best_entry.checkpoint_ts &&
			 tasks[i].best_entry.redo_lsn >= best_entry.redo_lsn))
		{
			best_entry = tasks[i].best_entry;
			best_segment_index = tasks[i].best_segment_index;
			found = true;
		}
	}

	pfree(indexes);
	pfree(meta_dir);

	if (!found)
		return;

	ctx->anchor_hint_found = true;
	ctx->anchor_found = true;
	ctx->anchor_checkpoint_lsn = best_entry.checkpoint_lsn;
	ctx->anchor_redo_lsn = best_entry.redo_lsn;
	ctx->anchor_time = best_entry.checkpoint_ts;
	ctx->anchor_hint_segment_index =
		fb_find_segment_index_for_lsn((FbWalSegmentEntry *) ctx->resolved_segments,
									 ctx->resolved_segment_count,
									 best_entry.redo_lsn,
									 ctx->wal_seg_size);
	if (ctx->anchor_hint_segment_index < 0)
		ctx->anchor_hint_segment_index = best_segment_index;
	if (best_entry.redo_lsn > ctx->start_lsn)
	{
		ctx->start_lsn = best_entry.redo_lsn;
		ctx->start_lsn_pruned = true;
	}
}

/*
 * fb_note_checkpoint_record
 *    WAL helper.
 */

static void
fb_note_checkpoint_record(XLogReaderState *reader, FbWalScanContext *ctx)
{
	uint8 rmid = XLogRecGetRmid(reader);
	uint8 info_code;
	CheckPoint *checkpoint;
	TimestampTz checkpoint_ts;
	XLogSegNo segno;
	FbWalSegmentEntry *entry;

	if (rmid != RM_XLOG_ID)
		return;

	info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (info_code != XLOG_CHECKPOINT_SHUTDOWN &&
		info_code != XLOG_CHECKPOINT_ONLINE)
		return;

	checkpoint = (CheckPoint *) XLogRecGetData(reader);
	checkpoint_ts = time_t_to_timestamptz(checkpoint->time);
	XLByteToSeg(reader->ReadRecPtr, segno, ctx->wal_seg_size);
	entry = fb_find_segment_by_segno((FbWalSegmentEntry *) ctx->resolved_segments,
									 ctx->resolved_segment_count,
									 segno);
	if (entry != NULL)
		fb_checkpoint_sidecar_append(entry,
									 checkpoint_ts,
									 reader->ReadRecPtr,
									 checkpoint->redo);

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

static bool
fb_wal_anchor_probe_visitor(XLogReaderState *reader, void *arg)
{
	FbWalAnchorProbeState *state = (FbWalAnchorProbeState *) arg;
	CheckPoint *checkpoint;
	TimestampTz checkpoint_ts;
	uint8 info_code;

	if (reader == NULL || state == NULL || state->ctx == NULL)
		return true;
	if (XLogRecGetRmid(reader) != RM_XLOG_ID)
		return true;

	info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (info_code != XLOG_CHECKPOINT_SHUTDOWN &&
		info_code != XLOG_CHECKPOINT_ONLINE)
		return true;

	checkpoint = (CheckPoint *) XLogRecGetData(reader);
	checkpoint_ts = time_t_to_timestamptz(checkpoint->time);
	state->saw_checkpoint = true;
	state->first_checkpoint_ts = checkpoint_ts;
	fb_note_checkpoint_record(reader, state->ctx);
	return false;
}

static void
fb_wal_raise_missing_anchor_error(const FbWalScanContext *ctx,
								  bool saw_checkpoint,
								  TimestampTz first_checkpoint_ts)
{
	char *target_ts_str = NULL;

	if (ctx != NULL)
		target_ts_str = pstrdup(timestamptz_to_str(ctx->target_ts));

	if (ctx != NULL && ctx->retained_suffix_only)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: target timestamp predates retained continuous WAL suffix"),
				 errdetail("target=%s first_retained_segment=%s gap_between=%s and %s",
						   target_ts_str,
						   ctx->first_segment,
						   ctx->retained_gap_left_segment,
						   ctx->retained_gap_right_segment)));

	if (ctx != NULL && saw_checkpoint)
	{
		char *first_checkpoint_ts_str = pstrdup(timestamptz_to_str(first_checkpoint_ts));

		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: target timestamp predates earliest checkpoint in retained WAL"),
				 errdetail("target=%s earliest_checkpoint=%s first_retained_segment=%s",
						   target_ts_str,
						   first_checkpoint_ts_str,
						   ctx->first_segment)));
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("WAL not complete: retained WAL contains no checkpoint record")));
}

static void
fb_wal_probe_anchor_coverage(FbWalScanContext *ctx)
{
	FbWalAnchorProbeState state;
	XLogRecPtr saved_first_record_lsn;
	XLogRecPtr saved_last_record_lsn;
	uint64 saved_records_scanned;

	if (ctx == NULL || ctx->anchor_found ||
		ctx->resolved_segments == NULL || ctx->resolved_segment_count == 0)
		return;

	MemSet(&state, 0, sizeof(state));
	state.ctx = ctx;
	saved_first_record_lsn = ctx->first_record_lsn;
	saved_last_record_lsn = ctx->last_record_lsn;
	saved_records_scanned = ctx->records_scanned;

	fb_wal_visit_records(ctx, fb_wal_anchor_probe_visitor, &state);

	ctx->first_record_lsn = saved_first_record_lsn;
	ctx->last_record_lsn = saved_last_record_lsn;
	ctx->records_scanned = saved_records_scanned;

	if (!ctx->anchor_found)
		fb_wal_raise_missing_anchor_error(ctx,
										  state.saw_checkpoint,
										  state.first_checkpoint_ts);
}

static void
fb_maybe_activate_tail_payload_capture(XLogReaderState *reader,
									   FbWalIndexBuildState *state)
{
	FbWalRecordIndex *index;
	CheckPoint *checkpoint;
	TimestampTz checkpoint_ts;
	uint8 rmid;
	uint8 info_code;

	if (reader == NULL || state == NULL || !state->collect_metadata ||
		!state->tail_capture_allowed || state->capture_payload)
		return;

	index = state->index;
	if (index == NULL || state->ctx == NULL || !state->ctx->anchor_found)
		return;

	rmid = XLogRecGetRmid(reader);
	if (rmid != RM_XLOG_ID)
		return;

	info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (info_code != XLOG_CHECKPOINT_SHUTDOWN &&
		info_code != XLOG_CHECKPOINT_ONLINE)
		return;

	checkpoint = (CheckPoint *) XLogRecGetData(reader);
	checkpoint_ts = time_t_to_timestamptz(checkpoint->time);
	if (checkpoint_ts <= state->ctx->target_ts)
		return;

	state->capture_payload = true;
	state->payload_log = &index->record_tail_log;
	state->payload_label = "wal-records-tail";
	index->tail_inline_payload = true;
	index->tail_cutover_lsn = reader->EndRecPtr;
}

/*
 * fb_index_append_record
 *    WAL helper.
 */

static FbSpoolLog *
fb_index_ensure_record_log(FbWalRecordIndex *index,
						   FbSpoolLog **log_ptr,
						   const char *label)
{
	if (index == NULL || log_ptr == NULL)
		return NULL;

	if (*log_ptr == NULL)
	{
		if (index->spool_session == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("pg_flashback internal error: temporary WAL workspace was not initialized for this query"),
					 errdetail("WAL payload capture reached record spool creation before the per-query temporary workspace was available."),
					 errhint("Retry the query. If the error persists, check server logs and rebuild/reinstall pg_flashback.")));
		*log_ptr = fb_spool_log_create(index->spool_session, label);
	}

	return *log_ptr;
}

static bool
fb_record_ref_needs_payload_materialization(const FbRecordRef *record)
{
	int block_index;

	if (record == NULL)
		return false;
	if (record->main_data_len > 0 && record->main_data == NULL)
		return true;

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record->blocks[block_index];

		if (block_ref->has_data &&
			block_ref->data_len > 0 &&
			block_ref->data == NULL)
			return true;
		if (block_ref->has_image &&
			block_ref->apply_image &&
			block_ref->image == NULL)
			return true;
	}

	return false;
}

static void
fb_index_append_locator_stub(FbWalRecordIndex *index,
							 FbSpoolLog **log_ptr,
							 const char *label,
							 XLogRecPtr record_lsn)
{
	StringInfoData buf;
	FbSerializedRecordHeader hdr;
	FbSpoolLog *log;

	if (index == NULL || log_ptr == NULL || label == NULL ||
		XLogRecPtrIsInvalid(record_lsn))
		return;

	log = fb_index_ensure_record_log(index, log_ptr, label);
	initStringInfo(&buf);
	MemSet(&hdr, 0, sizeof(hdr));
	hdr.lsn = record_lsn;
	hdr.locator_only = true;
	appendBinaryStringInfo(&buf, (const char *) &hdr, sizeof(hdr));
	fb_spool_log_append(log, buf.data, buf.len);
	pfree(buf.data);
	index->record_count++;
}

static void
fb_index_append_record(FbWalRecordIndex *index,
					   FbSpoolLog **log_ptr,
					   const char *label,
					   const FbRecordRef *record,
					   bool defer_payload_body)
{
	StringInfoData buf;
	FbSerializedRecordHeader hdr;
	FbSpoolLog *log;
	int block_index;

	if (index == NULL || log_ptr == NULL || label == NULL || record == NULL)
		return;

	log = fb_index_ensure_record_log(index, log_ptr, label);

	initStringInfo(&buf);
	MemSet(&hdr, 0, sizeof(hdr));
	hdr.kind = record->kind;
	hdr.lsn = record->lsn;
	hdr.end_lsn = record->end_lsn;
	hdr.xid = record->xid;
	hdr.info = record->info;
	hdr.payload_deferred = defer_payload_body;
	hdr.init_page = record->init_page;
	hdr.block_count = record->block_count;
	hdr.main_data_len = record->main_data_len;
	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *src = &record->blocks[block_index];
		FbSerializedRecordBlockRef *dst = &hdr.blocks[block_index];
		bool store_image =
			fb_record_block_has_applicable_image(src) && src->image != NULL;

		dst->in_use = src->in_use;
		dst->block_id = src->block_id;
		dst->locator = src->locator;
		dst->forknum = src->forknum;
		dst->blkno = src->blkno;
		dst->is_main_relation = src->is_main_relation;
		dst->is_toast_relation = src->is_toast_relation;
		dst->has_image = store_image;
		dst->apply_image = src->apply_image;
		dst->has_data = src->has_data;
		dst->data_len = src->data_len;
	}

	appendBinaryStringInfo(&buf, (const char *) &hdr, sizeof(hdr));
	if (!defer_payload_body &&
		record->main_data_len > 0 &&
		record->main_data != NULL)
		appendBinaryStringInfo(&buf, record->main_data, record->main_data_len);
	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record->blocks[block_index];
		bool store_image =
			fb_record_block_has_applicable_image(block_ref) &&
			block_ref->image != NULL;

		if (!defer_payload_body && store_image)
			appendBinaryStringInfo(&buf, block_ref->image, BLCKSZ);
		if (!defer_payload_body &&
			block_ref->has_data &&
			block_ref->data_len > 0 &&
			block_ref->data != NULL)
			appendBinaryStringInfo(&buf, block_ref->data, block_ref->data_len);
	}

	fb_spool_log_append(log, buf.data, buf.len);
	pfree(buf.data);
	index->record_count++;
}

static void
fb_index_note_record_metadata(FbWalRecordIndex *index)
{
	if (index == NULL)
		return;

	index->total_record_count++;
}

static void
fb_index_note_materialized_record(FbWalRecordIndex *index,
								  FbRecordRef *record)
{
	if (index == NULL || record == NULL)
		return;

	fb_wal_set_record_status(index, record);
	if (record->lsn < index->anchor_redo_lsn)
		return;

	index->kept_record_count++;
	if (!record->committed_after_target)
		return;

	switch (record->kind)
	{
		case FB_WAL_RECORD_HEAP_INSERT:
			if (!fb_record_is_speculative_insert(record))
			{
				index->target_record_count++;
				index->target_insert_count++;
			}
			break;
		case FB_WAL_RECORD_HEAP_DELETE:
			if (!fb_record_is_super_delete(record))
			{
				index->target_record_count++;
				index->target_delete_count++;
			}
			break;
		case FB_WAL_RECORD_HEAP_UPDATE:
		case FB_WAL_RECORD_HEAP_HOT_UPDATE:
			index->target_record_count++;
			index->target_update_count++;
			break;
		default:
			break;
	}
}

static void
fb_record_release_temp(FbRecordRef *record)
{
	int block_index;

	if (record == NULL)
		return;

	if (record->main_data != NULL)
	{
		pfree(record->main_data);
		record->main_data = NULL;
	}

	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		FbRecordBlockRef *block_ref = &record->blocks[block_index];

		if (block_ref->image != NULL)
		{
			pfree(block_ref->image);
			block_ref->image = NULL;
		}
		if (block_ref->data != NULL)
		{
			pfree(block_ref->data);
			block_ref->data = NULL;
		}
	}
}

static bool
fb_wal_payload_kind_enabled(uint8 kind)
{
	return kind != FB_WAL_RECORD_XLOG_FPI_FOR_HINT &&
		kind != FB_WAL_RECORD_HEAP_CONFIRM &&
		kind != FB_WAL_RECORD_HEAP_LOCK &&
		kind != FB_WAL_RECORD_HEAP2_VISIBLE &&
		kind != FB_WAL_RECORD_HEAP2_LOCK_UPDATED;
}

/*
 * fb_record_block_copy_image
 *    WAL helper.
 */

static void
fb_record_block_copy_image(FbRecordBlockRef *block_ref,
						   XLogReaderState *reader,
						   bool copy_payload)
{
	char page[BLCKSZ];
	bool apply_image;

	if (!XLogRecHasBlockImage(reader, block_ref->block_id))
		return;
	apply_image = XLogRecBlockImageApply(reader, block_ref->block_id);
	block_ref->apply_image = apply_image;
	block_ref->has_image = true;
	if (!copy_payload)
		return;
	if (!RestoreBlockImage(reader, block_ref->block_id, page))
		return;

	block_ref->image = fb_copy_bytes(page, BLCKSZ);
}

/*
 * fb_fill_record_block_ref
 *    WAL helper.
 */

static void
fb_fill_record_block_ref(FbRecordBlockRef *block_ref, XLogReaderState *reader,
						 uint8 block_id, const FbRelationInfo *info,
						 bool copy_payload)
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
	Size datalen = 0;
	char *data;

	MemSet(block_ref, 0, sizeof(*block_ref));
	if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum, &blkno))
		return;

	block_ref->in_use = true;
	block_ref->block_id = block_id;
	block_ref->locator = locator;
	block_ref->forknum = forknum;
	block_ref->blkno = blkno;
	block_ref->is_main_relation = RelFileLocatorEquals(locator, info->locator);
	block_ref->is_toast_relation = info->has_toast_locator &&
		RelFileLocatorEquals(locator, info->toast_locator);

	if (FB_XLOGREC_HAS_BLOCK_DATA(reader, block_id))
	{
		data = XLogRecGetBlockData(reader, block_id, &datalen);
		block_ref->has_data = (data != NULL && datalen > 0);
		if (block_ref->has_data)
		{
			block_ref->data_len = datalen;
			if (copy_payload)
				block_ref->data = fb_copy_bytes(data, datalen);
		}
	}

	fb_record_block_copy_image(block_ref, reader, copy_payload);
}

/*
 * fb_note_xid_status
 *    WAL helper.
 */

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
fb_append_xact_summary(XLogReaderState *reader,
					   FbWalIndexBuildState *state)
{
	FbSpoolLog *log;
	FbWalXactSummaryHeader hdr;
	StringInfoData buf;
	uint8 info_code;

	if (reader == NULL || state == NULL || state->xact_summary_log == NULL ||
		state->xact_summary_label == NULL)
		return;
	if (XLogRecGetRmid(reader) != RM_XACT_ID)
		return;

	info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	MemSet(&hdr, 0, sizeof(hdr));
	hdr.xid = fb_record_xid(reader);
	hdr.record_lsn = reader->ReadRecPtr;
	hdr.end_lsn = reader->EndRecPtr;

	switch (info_code)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_COMMIT_PREPARED:
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				xl_xact_parsed_commit parsed;

				ParseCommitRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				hdr.status = FB_WAL_XID_COMMITTED;
				hdr.timestamp = parsed.xact_time;
				hdr.subxact_count = (uint32) parsed.nsubxacts;

				log = fb_index_ensure_record_log(state->index,
												 state->xact_summary_log,
												 state->xact_summary_label);
				initStringInfo(&buf);
				appendBinaryStringInfo(&buf, (const char *) &hdr, sizeof(hdr));
				if (parsed.nsubxacts > 0)
					appendBinaryStringInfo(&buf,
										   (const char *) parsed.subxacts,
										   sizeof(TransactionId) * parsed.nsubxacts);
				fb_spool_log_append(log, buf.data, buf.len);
				pfree(buf.data);
				break;
			}
		case XLOG_XACT_ABORT:
		case XLOG_XACT_ABORT_PREPARED:
			{
				xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(reader);
				xl_xact_parsed_abort parsed;

				ParseAbortRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				hdr.status = FB_WAL_XID_ABORTED;
				hdr.timestamp = parsed.xact_time;
				hdr.subxact_count = (uint32) parsed.nsubxacts;

				log = fb_index_ensure_record_log(state->index,
												 state->xact_summary_log,
												 state->xact_summary_label);
				initStringInfo(&buf);
				appendBinaryStringInfo(&buf, (const char *) &hdr, sizeof(hdr));
				if (parsed.nsubxacts > 0)
					appendBinaryStringInfo(&buf,
										   (const char *) parsed.subxacts,
										   sizeof(TransactionId) * parsed.nsubxacts);
				fb_spool_log_append(log, buf.data, buf.len);
				pfree(buf.data);
				break;
			}
		default:
			break;
	}
}

static uint32
fb_apply_xact_summary_entry(FbWalScanContext *ctx,
							FbWalRecordIndex *index,
							HTAB *touched_xids,
							HTAB *unsafe_xids,
							const char *data,
							Size len)
{
	FbWalXactSummaryHeader hdr;
	const TransactionId *subxids;
	FbUnsafeXidEntry *unsafe_entry;
	FbXidStatusEntry *status_entry;
	bool found = false;
	bool already_resolved;
	bool is_touched;
	uint32 hits = 0;
	uint32 i;

	if (ctx == NULL || index == NULL || touched_xids == NULL ||
		unsafe_xids == NULL || data == NULL || len < sizeof(hdr))
		return 0;

	memcpy(&hdr, data, sizeof(hdr));
	if (len != sizeof(hdr) + sizeof(TransactionId) * hdr.subxact_count)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("fb xact summary spool entry has invalid length")));
	subxids = (const TransactionId *) (data + sizeof(hdr));

	is_touched = fb_hash_has_xid(touched_xids, hdr.xid);
	unsafe_entry = fb_find_unsafe_xid(unsafe_xids, hdr.xid);
	if (is_touched || unsafe_entry != NULL)
	{
		status_entry = fb_get_xid_status_entry(index->xid_statuses, hdr.xid, &found);
		already_resolved = (found && status_entry->status != FB_WAL_XID_UNKNOWN);
		fb_note_xid_status(index->xid_statuses, hdr.xid,
						   hdr.status, hdr.timestamp, hdr.end_lsn);
		if (!already_resolved)
		{
			hits++;
			if (is_touched &&
				hdr.timestamp > ctx->target_ts &&
				hdr.timestamp <= ctx->query_now_ts)
			{
				if (hdr.status == FB_WAL_XID_COMMITTED)
					index->target_commit_count++;
				else if (hdr.status == FB_WAL_XID_ABORTED)
					index->target_abort_count++;
			}
		}
		if (unsafe_entry != NULL &&
			hdr.status == FB_WAL_XID_COMMITTED &&
			hdr.timestamp > ctx->target_ts &&
			hdr.timestamp <= ctx->query_now_ts &&
			fb_unsafe_entry_requires_reject(unsafe_entry) &&
			!ctx->unsafe)
		{
			fb_capture_unsafe_context(ctx, unsafe_entry, hdr.xid, hdr.timestamp);
			fb_mark_unsafe(ctx, unsafe_entry->reason);
		}
	}

	for (i = 0; i < hdr.subxact_count; i++)
	{
		is_touched = fb_hash_has_xid(touched_xids, subxids[i]);
		unsafe_entry = fb_find_unsafe_xid(unsafe_xids, subxids[i]);
		if (!is_touched && unsafe_entry == NULL)
			continue;

		status_entry = fb_get_xid_status_entry(index->xid_statuses, subxids[i], &found);
		already_resolved = (found && status_entry->status != FB_WAL_XID_UNKNOWN);
		fb_note_xid_status(index->xid_statuses, subxids[i],
						   hdr.status, hdr.timestamp, hdr.end_lsn);
		if (!already_resolved)
		{
			hits++;
			if (is_touched &&
				hdr.timestamp > ctx->target_ts &&
				hdr.timestamp <= ctx->query_now_ts)
			{
				if (hdr.status == FB_WAL_XID_COMMITTED)
					index->target_commit_count++;
				else if (hdr.status == FB_WAL_XID_ABORTED)
					index->target_abort_count++;
			}
		}
		if (unsafe_entry != NULL &&
			hdr.status == FB_WAL_XID_COMMITTED &&
			hdr.timestamp > ctx->target_ts &&
			hdr.timestamp <= ctx->query_now_ts &&
			fb_unsafe_entry_requires_reject(unsafe_entry) &&
			!ctx->unsafe)
		{
			fb_capture_unsafe_context(ctx, unsafe_entry, subxids[i], hdr.timestamp);
			fb_mark_unsafe(ctx, unsafe_entry->reason);
		}
	}

	return hits;
}

static uint32
fb_wal_apply_xact_summary_log(FbWalScanContext *ctx,
							  FbWalRecordIndex *index,
							  HTAB *touched_xids,
							  HTAB *unsafe_xids)
{
	FbSpoolCursor *cursor;
	StringInfoData buf;
	uint32 item_index;
	uint32 hits = 0;

	if (ctx == NULL || index == NULL || index->xact_summary_log == NULL)
		return 0;
	if ((touched_xids == NULL || hash_get_num_entries(touched_xids) <= 0) &&
		(unsafe_xids == NULL || hash_get_num_entries(unsafe_xids) <= 0))
		return 0;

	cursor = fb_spool_cursor_open(index->xact_summary_log, FB_SPOOL_FORWARD);
	if (cursor == NULL)
		return 0;

	initStringInfo(&buf);
	while (fb_spool_cursor_read(cursor, &buf, &item_index))
	{
		hits += fb_apply_xact_summary_entry(ctx,
											index,
											touched_xids,
											unsafe_xids,
											buf.data,
											buf.len);
		if (ctx->unsafe)
			break;
	}
	pfree(buf.data);
	fb_spool_cursor_close(cursor);
	return hits;
}

static bool
fb_wal_xact_touched_lookup(TransactionId xid,
						   const TransactionId *touched_xids,
						   uint32 touched_count)
{
	return TransactionIdIsValid(xid) &&
		touched_xids != NULL &&
		touched_count > 0 &&
		bsearch(&xid, touched_xids, touched_count,
				sizeof(TransactionId), fb_transactionid_cmp) != NULL;
}

static const FbUnsafeXidEntry *
fb_wal_xact_unsafe_lookup(TransactionId xid,
						  const FbUnsafeXidEntry *unsafe_entries,
						  uint32 unsafe_count)
{
	FbUnsafeXidEntry key;

	if (!TransactionIdIsValid(xid) || unsafe_entries == NULL || unsafe_count == 0)
		return NULL;

	MemSet(&key, 0, sizeof(key));
	key.xid = xid;
	return (const FbUnsafeXidEntry *) bsearch(&key,
											  unsafe_entries,
											  unsafe_count,
											  sizeof(FbUnsafeXidEntry),
											  fb_unsafe_xid_entry_cmp);
}

static void
fb_wal_xact_merge_statuses(FbWalRecordIndex *index,
						   const char *path,
						   uint32 item_count)
{
	FbSpoolLog *source;
	FbSpoolCursor *cursor;
	StringInfoData buf;
	uint32 item_index;

	if (index == NULL || path == NULL || item_count == 0)
		return;

	source = fb_spool_log_open_readonly(path, item_count);
	cursor = fb_spool_cursor_open(source, FB_SPOOL_FORWARD);
	initStringInfo(&buf);
	while (fb_spool_cursor_read(cursor, &buf, &item_index))
	{
		FbXidStatusEntry entry;

		if (buf.len != sizeof(entry))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("fb xid status spool entry has invalid length")));
		memcpy(&entry, buf.data, sizeof(entry));
		fb_note_xid_status(index->xid_statuses,
						   entry.xid,
						   entry.status,
						   entry.commit_ts,
						   entry.commit_lsn);
	}
	pfree(buf.data);
	fb_spool_cursor_close(cursor);
	fb_spool_log_close(source);
	unlink(path);
}

/*
 * fb_copy_heap_record_ref
 *    WAL helper.
 */

static void
fb_copy_heap_record_ref(XLogReaderState *reader, const FbRelationInfo *info,
						FbWalRecordKind kind, FbWalIndexBuildState *state)
{
	FbRecordRef record;
	FbWalRecordIndex *index = state->index;
	int block_count = 0;
	int block_id;

	MemSet(&record, 0, sizeof(record));
	record.kind = kind;
	record.lsn = reader->ReadRecPtr;
	record.end_lsn = reader->EndRecPtr;
	record.xid = fb_record_xid(reader);
	record.info = XLogRecGetInfo(reader);
	record.init_page = ((XLogRecGetInfo(reader) & XLOG_HEAP_INIT_PAGE) != 0);
	record.main_data = fb_copy_bytes(XLogRecGetData(reader),
									 XLogRecGetDataLen(reader));
	record.main_data_len = XLogRecGetDataLen(reader);

	for (block_id = 0;
		 block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader) && block_count < FB_WAL_MAX_BLOCK_REFS;
		 block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum,
									  &blkno))
			continue;
		if (forknum != MAIN_FORKNUM)
			continue;
		if (!fb_locator_matches_relation(&locator, info))
			continue;

		fb_fill_record_block_ref(&record.blocks[block_count], reader, block_id, info, true);
		block_count++;
	}

	record.block_count = block_count;
	fb_index_append_record(index,
						   state->payload_log,
						   state->payload_label,
						   &record,
						   state->defer_payload_body);
	if (state->payload_block_states != NULL && index->record_count > 0)
	{
		fb_index_note_materialized_record(index, &record);
		fb_wal_note_precomputed_missing_blocks(index,
											  state->payload_block_states,
											  &record,
											  index->record_count - 1);
	}
	fb_record_release_temp(&record);
}

/*
 * fb_copy_xlog_fpi_record_ref
 *    WAL helper.
 */

static void
fb_copy_xlog_fpi_record_ref(XLogReaderState *reader, const FbRelationInfo *info,
							FbWalRecordKind kind, FbWalIndexBuildState *state)
{
	FbRecordRef record;
	FbWalRecordIndex *index = state->index;
	int block_count = 0;
	int block_id;

	MemSet(&record, 0, sizeof(record));
	record.kind = kind;
	record.lsn = reader->ReadRecPtr;
	record.end_lsn = reader->EndRecPtr;
	record.xid = InvalidTransactionId;
	record.info = XLogRecGetInfo(reader);
	record.init_page = false;

	for (block_id = 0;
		 block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader) && block_count < FB_WAL_MAX_BLOCK_REFS;
		 block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum,
									  &blkno))
			continue;
		if (forknum != MAIN_FORKNUM)
			continue;
		if (!fb_locator_matches_relation(&locator, info))
			continue;

		fb_fill_record_block_ref(&record.blocks[block_count], reader, block_id, info, true);
		block_count++;
	}

	record.block_count = block_count;
	fb_index_append_record(index,
						   state->payload_log,
						   state->payload_label,
						   &record,
						   state->defer_payload_body);
	if (state->payload_block_states != NULL && index->record_count > 0)
	{
		fb_index_note_materialized_record(index, &record);
		fb_wal_note_precomputed_missing_blocks(index,
											  state->payload_block_states,
											  &record,
											  index->record_count - 1);
	}
	fb_record_release_temp(&record);
}

/*
 * fb_heap_record_matches_target
 *    WAL helper.
 */

static bool
fb_heap_record_matches_target(XLogReaderState *reader, const FbRelationInfo *info)
{
	return fb_record_touches_relation(reader, info);
}

/*
 * fb_mark_xid_touched
 *    WAL helper.
 */

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

/*
 * fb_mark_xid_unsafe
 *    WAL helper.
 */

static void
fb_mark_xid_unsafe(HTAB *unsafe_xids, TransactionId xid,
				   FbWalUnsafeReason reason,
				   FbWalUnsafeScope scope,
				   FbWalStorageChangeOp storage_op,
				   XLogRecPtr lsn,
				   FbWalScanContext *ctx)
{
	FbUnsafeXidEntry *entry;
	bool found;

	if (!TransactionIdIsValid(xid))
	{
		if (reason == FB_WAL_UNSAFE_STORAGE_CHANGE &&
			(storage_op == FB_WAL_STORAGE_CHANGE_STANDBY_LOCK ||
			 storage_op == FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE))
			return;

		ctx->unsafe_xid = InvalidTransactionId;
		ctx->unsafe_commit_ts = 0;
		ctx->unsafe_record_lsn = lsn;
		ctx->unsafe_scope = scope;
		ctx->unsafe_storage_op = storage_op;
		fb_mark_unsafe(ctx, reason);
		return;
	}

	entry = (FbUnsafeXidEntry *) hash_search(unsafe_xids, &xid, HASH_ENTER,
											 &found);
	if (!found)
	{
		entry->xid = xid;
		entry->reason = reason;
		entry->scope = scope;
		entry->storage_op = storage_op;
		entry->lsn = lsn;
	}
	else if (entry->reason == FB_WAL_UNSAFE_STORAGE_CHANGE &&
			 reason != FB_WAL_UNSAFE_STORAGE_CHANGE)
		entry->reason = reason;
	else if (entry->reason == FB_WAL_UNSAFE_STORAGE_CHANGE &&
			 reason == FB_WAL_UNSAFE_STORAGE_CHANGE)
	{
		bool entry_generic = (entry->storage_op == FB_WAL_STORAGE_CHANGE_UNKNOWN ||
							  entry->storage_op == FB_WAL_STORAGE_CHANGE_STANDBY_LOCK);
		bool incoming_specific = (storage_op == FB_WAL_STORAGE_CHANGE_SMGR_CREATE ||
								  storage_op == FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE);

		if (incoming_specific && entry_generic)
		{
			entry->scope = scope;
			entry->storage_op = storage_op;
			entry->lsn = lsn;
		}
	}
}

/*
 * fb_find_unsafe_xid
 *    WAL helper.
 */

static FbUnsafeXidEntry *
fb_find_unsafe_xid(HTAB *unsafe_xids, TransactionId xid)
{
	if (!TransactionIdIsValid(xid))
		return NULL;

	return (FbUnsafeXidEntry *) hash_search(unsafe_xids, &xid, HASH_FIND, NULL);
}

static void
fb_wal_metadata_dump_touched_xids(HTAB *touched_xids,
								  const char *path,
								  uint32 *count_out)
{
	FbSpoolLog *log;
	HASH_SEQ_STATUS status;
	FbTouchedXidEntry *entry;
	uint32 count = 0;

	if (count_out != NULL)
		*count_out = 0;
	if (touched_xids == NULL || path == NULL)
		return;

	log = fb_spool_log_create_path(path);
	hash_seq_init(&status, touched_xids);
	while ((entry = (FbTouchedXidEntry *) hash_seq_search(&status)) != NULL)
	{
		fb_spool_log_append(log, &entry->xid, sizeof(entry->xid));
		count++;
	}
	fb_spool_log_close(log);
	if (count_out != NULL)
		*count_out = count;
}

static void
fb_wal_metadata_dump_unsafe_xids(HTAB *unsafe_xids,
								 const char *path,
								 uint32 *count_out)
{
	FbSpoolLog *log;
	HASH_SEQ_STATUS status;
	FbUnsafeXidEntry *entry;
	uint32 count = 0;

	if (count_out != NULL)
		*count_out = 0;
	if (unsafe_xids == NULL || path == NULL)
		return;

	log = fb_spool_log_create_path(path);
	hash_seq_init(&status, unsafe_xids);
	while ((entry = (FbUnsafeXidEntry *) hash_seq_search(&status)) != NULL)
	{
		fb_spool_log_append(log, entry, sizeof(*entry));
		count++;
	}
	fb_spool_log_close(log);
	if (count_out != NULL)
		*count_out = count;
}

static void
fb_wal_metadata_merge_touched_xids(FbWalScanContext *ctx,
								   HTAB *touched_xids,
								   const char *path,
								   uint32 item_count)
{
	FbSpoolLog *source;
	FbSpoolCursor *cursor;
	StringInfoData buf;
	uint32 item_index;

	if (ctx == NULL || touched_xids == NULL || path == NULL || item_count == 0)
		return;

	source = fb_spool_log_open_readonly(path, item_count);
	cursor = fb_spool_cursor_open(source, FB_SPOOL_FORWARD);
	initStringInfo(&buf);
	while (fb_spool_cursor_read(cursor, &buf, &item_index))
	{
		TransactionId xid;

		if (buf.len != sizeof(xid))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("fb touched xid spool entry has invalid length")));
		memcpy(&xid, buf.data, sizeof(xid));
		fb_mark_xid_touched(touched_xids, xid, ctx);
	}
	pfree(buf.data);
	fb_spool_cursor_close(cursor);
	fb_spool_log_close(source);
	unlink(path);
}

static void
fb_wal_metadata_merge_unsafe_xids(FbWalScanContext *ctx,
								  HTAB *unsafe_xids,
								  const char *path,
								  uint32 item_count)
{
	FbSpoolLog *source;
	FbSpoolCursor *cursor;
	StringInfoData buf;
	uint32 item_index;

	if (ctx == NULL || unsafe_xids == NULL || path == NULL || item_count == 0)
		return;

	source = fb_spool_log_open_readonly(path, item_count);
	cursor = fb_spool_cursor_open(source, FB_SPOOL_FORWARD);
	initStringInfo(&buf);
	while (fb_spool_cursor_read(cursor, &buf, &item_index))
	{
		FbUnsafeXidEntry entry;

		if (buf.len != sizeof(entry))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("fb unsafe xid spool entry has invalid length")));
		memcpy(&entry, buf.data, sizeof(entry));
		fb_mark_xid_unsafe(unsafe_xids,
						   entry.xid,
						   entry.reason,
						   entry.scope,
						   entry.storage_op,
						   entry.lsn,
						   ctx);
	}
	pfree(buf.data);
	fb_spool_cursor_close(cursor);
	fb_spool_log_close(source);
	unlink(path);
}

/*
 * fb_record_xid
 *    WAL helper.
 */

static TransactionId
fb_record_xid(XLogReaderState *reader)
{
	TransactionId xid = FB_XLOGREC_GET_TOP_XID(reader);

	if (TransactionIdIsValid(xid))
		return xid;

	return XLogRecGetXid(reader);
}

/*
 * fb_mark_record_xids_touched
 *    WAL helper.
 */

static void
fb_mark_record_xids_touched(XLogReaderState *reader, HTAB *touched_xids,
							FbWalScanContext *ctx)
{
	TransactionId xid = XLogRecGetXid(reader);
	TransactionId top_xid = FB_XLOGREC_GET_TOP_XID(reader);

	fb_mark_xid_touched(touched_xids, xid, ctx);
	if (top_xid != xid)
		fb_mark_xid_touched(touched_xids, top_xid, ctx);
}

/*
 * fb_locator_matches_relation
 *    WAL helper.
 */

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

static FbWalUnsafeScope
fb_relation_scope_from_locator(const RelFileLocator *locator,
							   const FbRelationInfo *info)
{
	if (RelFileLocatorEquals(*locator, info->locator))
		return FB_WAL_UNSAFE_SCOPE_MAIN;

	if (info->has_toast_locator &&
		RelFileLocatorEquals(*locator, info->toast_locator))
		return FB_WAL_UNSAFE_SCOPE_TOAST;

	return FB_WAL_UNSAFE_SCOPE_NONE;
}

static FbWalUnsafeScope
fb_relation_scope_from_relid(Oid relid, const FbRelationInfo *info)
{
	if (relid == info->relid)
		return FB_WAL_UNSAFE_SCOPE_MAIN;
	if (OidIsValid(info->toast_relid) && relid == info->toast_relid)
		return FB_WAL_UNSAFE_SCOPE_TOAST;

	return FB_WAL_UNSAFE_SCOPE_NONE;
}

/*
 * fb_record_touches_relation
 *    WAL helper.
 */

static bool
fb_record_touches_relation(XLogReaderState *reader, const FbRelationInfo *info)
{
	int block_id;

	for (block_id = 0; block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader); block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum,
									  &blkno))
			continue;

		(void) blkno;

		if (forknum != MAIN_FORKNUM)
			continue;

		if (fb_locator_matches_relation(&locator, info))
			return true;
	}

	return false;
}

/*
 * fb_mark_unsafe
 *    WAL helper.
 */

static void
fb_mark_unsafe(FbWalScanContext *ctx, FbWalUnsafeReason reason)
{
	if (ctx->unsafe)
		return;

	ctx->unsafe = true;
	ctx->unsafe_reason = reason;
}

static bool
fb_unsafe_entry_requires_reject(const FbUnsafeXidEntry *entry)
{
	if (entry == NULL)
		return false;

	if (entry->reason != FB_WAL_UNSAFE_STORAGE_CHANGE)
		return true;

	if (entry->storage_op == FB_WAL_STORAGE_CHANGE_STANDBY_LOCK)
		return false;

	if (entry->storage_op == FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE)
		return false;

	return true;
}

static void
fb_capture_unsafe_context(FbWalScanContext *ctx,
						  const FbUnsafeXidEntry *entry,
						  TransactionId xid,
						  TimestampTz commit_ts)
{
	if (ctx->unsafe)
		return;

	ctx->unsafe_xid = xid;
	ctx->unsafe_commit_ts = commit_ts;
	if (entry != NULL)
	{
		ctx->unsafe_record_lsn = entry->lsn;
		ctx->unsafe_scope = entry->scope;
		ctx->unsafe_storage_op = entry->storage_op;
	}
}

/*
 * fb_heap_truncate_matches_relation
 *    WAL helper.
 */

static bool
fb_heap_truncate_matches_relation(XLogReaderState *reader,
								  const FbRelationInfo *info)
{
	xl_heap_truncate *xlrec = (xl_heap_truncate *) XLogRecGetData(reader);
	uint32 i;

	if (xlrec->dbId != FB_LOCATOR_DBOID(info->locator))
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

/*
 * fb_heap_rewrite_matches_relation
 *    WAL helper.
 */

static bool
fb_heap_rewrite_matches_relation(XLogReaderState *reader,
								 const FbRelationInfo *info)
{
	xl_heap_rewrite_mapping *xlrec;

	xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);

	if (xlrec->mapped_db != FB_LOCATOR_DBOID(info->locator))
		return false;

	if (xlrec->mapped_rel == info->relid)
		return true;

	if (OidIsValid(info->toast_relid) && xlrec->mapped_rel == info->toast_relid)
		return true;

	return false;
}

/*
 * fb_smgr_record_matches_relation
 *    WAL helper.
 */

static bool
fb_smgr_record_matches_relation(XLogReaderState *reader,
								const FbRelationInfo *info,
								FbWalUnsafeScope *matched_scope,
								FbWalStorageChangeOp *matched_op)
{
	uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

	if (matched_scope != NULL)
		*matched_scope = FB_WAL_UNSAFE_SCOPE_NONE;
	if (matched_op != NULL)
		*matched_op = FB_WAL_STORAGE_CHANGE_UNKNOWN;

	if (info_code == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(reader);

		if (xlrec->forkNum != MAIN_FORKNUM)
			return false;

		{
			RelFileLocator locator = FB_XL_SMGR_CREATE_LOCATOR(xlrec);
			FbWalUnsafeScope scope = fb_relation_scope_from_locator(&locator, info);

			if (scope == FB_WAL_UNSAFE_SCOPE_NONE)
				return false;
			if (matched_scope != NULL)
				*matched_scope = scope;
			if (matched_op != NULL)
				*matched_op = FB_WAL_STORAGE_CHANGE_SMGR_CREATE;
			return true;
		}
	}
	else if (info_code == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(reader);

		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) == 0)
			return false;

		{
			RelFileLocator locator = FB_XL_SMGR_TRUNCATE_LOCATOR(xlrec);
			FbWalUnsafeScope scope = fb_relation_scope_from_locator(&locator, info);

			if (scope == FB_WAL_UNSAFE_SCOPE_NONE)
				return false;
			if (matched_scope != NULL)
				*matched_scope = scope;
			if (matched_op != NULL)
				*matched_op = FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE;
			return true;
		}
	}

	return false;
}

/*
 * fb_standby_record_matches_relation
 *    WAL helper.
 */

static bool
fb_standby_record_matches_relation(XLogReaderState *reader,
								   const FbRelationInfo *info,
								   TransactionId *matched_xid,
								   FbWalUnsafeScope *matched_scope)
{
	uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	xl_standby_locks *xlrec;
	int i;

	if (matched_xid != NULL)
		*matched_xid = InvalidTransactionId;
	if (matched_scope != NULL)
		*matched_scope = FB_WAL_UNSAFE_SCOPE_NONE;

	if (info_code != XLOG_STANDBY_LOCK)
		return false;

	xlrec = (xl_standby_locks *) XLogRecGetData(reader);
	for (i = 0; i < xlrec->nlocks; i++)
	{
		xl_standby_lock *lock = &xlrec->locks[i];

			if (lock->dbOid != FB_LOCATOR_DBOID(info->locator))
			continue;
		if (lock->relOid != info->relid &&
			(!OidIsValid(info->toast_relid) || lock->relOid != info->toast_relid))
			continue;

		if (matched_xid != NULL)
			*matched_xid = lock->xid;
		if (matched_scope != NULL)
			*matched_scope = fb_relation_scope_from_relid(lock->relOid, info);
		return true;
	}

	return false;
}

/*
 * fb_note_xact_record
 *    WAL helper.
 */

static void
fb_note_xact_record(XLogReaderState *reader, HTAB *touched_xids,
					HTAB *unsafe_xids,
					FbWalScanContext *ctx)
{
	uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	TransactionId xid = fb_record_xid(reader);
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
					if (fb_unsafe_entry_requires_reject(unsafe_entry))
					{
						fb_capture_unsafe_context(ctx, unsafe_entry, xid,
												  parsed.xact_time);
						fb_mark_unsafe(ctx, unsafe_entry->reason);
					}
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

/*
 * fb_note_xact_status_for_touched
 *    WAL helper.
 */

static uint32
fb_note_xact_status_for_touched(XLogReaderState *reader,
								HTAB *touched_xids,
								HTAB *unsafe_xids,
								FbWalRecordIndex *index,
								FbWalScanContext *ctx)
{
	uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	TransactionId xid = fb_record_xid(reader);
	FbUnsafeXidEntry *unsafe_entry = fb_find_unsafe_xid(unsafe_xids, xid);
	uint32 resolved_count = 0;

	switch (info_code)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_COMMIT_PREPARED:
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				xl_xact_parsed_commit parsed;
				TimestampTz commit_ts;
				bool top_touched;
				bool top_was_unknown;
				int i;

				ParseCommitRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				commit_ts = parsed.xact_time;
				top_touched = fb_hash_has_xid(touched_xids, xid);
				top_was_unknown =
					(fb_get_xid_status_entry(index->xid_statuses, xid, NULL) == NULL ||
					 fb_get_xid_status_entry(index->xid_statuses, xid, NULL)->status ==
					 FB_WAL_XID_UNKNOWN);
				if (unsafe_entry != NULL)
					fb_note_xid_status(index->xid_statuses, xid,
									   FB_WAL_XID_COMMITTED, commit_ts,
									   reader->EndRecPtr);
				if (top_touched)
				{
					fb_note_xid_status(index->xid_statuses, xid,
									   FB_WAL_XID_COMMITTED, commit_ts,
									   reader->EndRecPtr);
					if (commit_ts > ctx->target_ts &&
						commit_ts <= ctx->query_now_ts)
						index->target_commit_count++;
				}
				if ((top_touched || unsafe_entry != NULL) && top_was_unknown)
					resolved_count++;

				for (i = 0; i < parsed.nsubxacts; i++)
				{
					TransactionId subxid = parsed.subxacts[i];
					bool sub_was_unknown;

					if (!fb_hash_has_xid(touched_xids, subxid))
						continue;
					sub_was_unknown =
						(fb_get_xid_status_entry(index->xid_statuses, subxid, NULL) == NULL ||
						 fb_get_xid_status_entry(index->xid_statuses, subxid, NULL)->status ==
						 FB_WAL_XID_UNKNOWN);

					fb_note_xid_status(index->xid_statuses, subxid,
									   FB_WAL_XID_COMMITTED, commit_ts,
									   reader->EndRecPtr);
					if (sub_was_unknown)
						resolved_count++;
					if (commit_ts > ctx->target_ts &&
						commit_ts <= ctx->query_now_ts)
						index->target_commit_count++;
				}

				if (commit_ts > ctx->target_ts &&
					commit_ts <= ctx->query_now_ts &&
					fb_unsafe_entry_requires_reject(unsafe_entry))
				{
					fb_capture_unsafe_context(ctx, unsafe_entry, xid, commit_ts);
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
				bool top_touched;
				bool top_was_unknown;
				int i;

				ParseAbortRecord(XLogRecGetInfo(reader), xlrec, &parsed);
				abort_ts = parsed.xact_time;
				top_touched = fb_hash_has_xid(touched_xids, xid);
				top_was_unknown =
					(fb_get_xid_status_entry(index->xid_statuses, xid, NULL) == NULL ||
					 fb_get_xid_status_entry(index->xid_statuses, xid, NULL)->status ==
					 FB_WAL_XID_UNKNOWN);
				if (unsafe_entry != NULL)
					fb_note_xid_status(index->xid_statuses, xid,
									   FB_WAL_XID_ABORTED, abort_ts,
									   reader->EndRecPtr);

				if (top_touched)
				{
					fb_note_xid_status(index->xid_statuses, xid,
									   FB_WAL_XID_ABORTED, abort_ts,
									   reader->EndRecPtr);
					if (abort_ts > ctx->target_ts &&
						abort_ts <= ctx->query_now_ts)
						index->target_abort_count++;
				}
				if ((top_touched || unsafe_entry != NULL) && top_was_unknown)
					resolved_count++;

				for (i = 0; i < parsed.nsubxacts; i++)
				{
					TransactionId subxid = parsed.subxacts[i];
					bool sub_was_unknown;

					if (!fb_hash_has_xid(touched_xids, subxid))
						continue;
					sub_was_unknown =
						(fb_get_xid_status_entry(index->xid_statuses, subxid, NULL) == NULL ||
						 fb_get_xid_status_entry(index->xid_statuses, subxid, NULL)->status ==
						 FB_WAL_XID_UNKNOWN);

					fb_note_xid_status(index->xid_statuses, subxid,
									   FB_WAL_XID_ABORTED, abort_ts,
									   reader->EndRecPtr);
					if (sub_was_unknown)
						resolved_count++;
					if (abort_ts > ctx->target_ts &&
						abort_ts <= ctx->query_now_ts)
						index->target_abort_count++;
				}
				break;
			}
		default:
			break;
	}

	return resolved_count;
}

static bool
fb_wal_xact_fill_visitor(XLogReaderState *reader, void *arg)
{
	FbWalXactScanVisitorState *state = (FbWalXactScanVisitorState *) arg;
	uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	TransactionId xid = fb_record_xid(reader);
	const FbUnsafeXidEntry *unsafe_entry;

	if (state == NULL || XLogRecGetRmid(reader) != RM_XACT_ID)
		return true;

	unsafe_entry = fb_wal_xact_unsafe_lookup(xid,
											 state->unsafe_entries,
											 state->unsafe_count);

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
				if (fb_wal_xact_touched_lookup(xid,
											   state->touched_xids,
											   state->touched_count))
				{
					FbXidStatusEntry entry;

					MemSet(&entry, 0, sizeof(entry));
					entry.xid = xid;
					entry.status = FB_WAL_XID_COMMITTED;
					entry.commit_ts = commit_ts;
					entry.commit_lsn = reader->EndRecPtr;
					fb_spool_log_append(state->status_log, &entry, sizeof(entry));
					state->task->status_count++;
					if (commit_ts > state->task->target_ts &&
						commit_ts <= state->task->query_now_ts)
						state->task->target_commit_count++;
				}

				for (i = 0; i < parsed.nsubxacts; i++)
				{
					TransactionId subxid = parsed.subxacts[i];

					if (fb_wal_xact_touched_lookup(subxid,
												   state->touched_xids,
												   state->touched_count))
					{
						FbXidStatusEntry entry;

						MemSet(&entry, 0, sizeof(entry));
						entry.xid = subxid;
						entry.status = FB_WAL_XID_COMMITTED;
						entry.commit_ts = commit_ts;
						entry.commit_lsn = reader->EndRecPtr;
						fb_spool_log_append(state->status_log, &entry, sizeof(entry));
						state->task->status_count++;
						if (commit_ts > state->task->target_ts &&
							commit_ts <= state->task->query_now_ts)
							state->task->target_commit_count++;
					}
				}

				if (commit_ts > state->task->target_ts &&
					commit_ts <= state->task->query_now_ts &&
					fb_unsafe_entry_requires_reject(unsafe_entry) &&
					(!state->task->unsafe_found ||
					 reader->ReadRecPtr < state->task->unsafe_trigger_lsn))
				{
					state->task->unsafe_found = true;
					state->task->unsafe_xid = xid;
					state->task->unsafe_commit_ts = commit_ts;
					state->task->unsafe_reason = unsafe_entry->reason;
					state->task->unsafe_scope = unsafe_entry->scope;
					state->task->unsafe_storage_op = unsafe_entry->storage_op;
					state->task->unsafe_record_lsn = unsafe_entry->lsn;
					state->task->unsafe_trigger_lsn = reader->ReadRecPtr;
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
				if (fb_wal_xact_touched_lookup(xid,
											   state->touched_xids,
											   state->touched_count))
				{
					FbXidStatusEntry entry;

					MemSet(&entry, 0, sizeof(entry));
					entry.xid = xid;
					entry.status = FB_WAL_XID_ABORTED;
					entry.commit_ts = abort_ts;
					entry.commit_lsn = reader->EndRecPtr;
					fb_spool_log_append(state->status_log, &entry, sizeof(entry));
					state->task->status_count++;
					if (abort_ts > state->task->target_ts &&
						abort_ts <= state->task->query_now_ts)
						state->task->target_abort_count++;
				}

				for (i = 0; i < parsed.nsubxacts; i++)
				{
					TransactionId subxid = parsed.subxacts[i];

					if (fb_wal_xact_touched_lookup(subxid,
												   state->touched_xids,
												   state->touched_count))
					{
						FbXidStatusEntry entry;

						MemSet(&entry, 0, sizeof(entry));
						entry.xid = subxid;
						entry.status = FB_WAL_XID_ABORTED;
						entry.commit_ts = abort_ts;
						entry.commit_lsn = reader->EndRecPtr;
						fb_spool_log_append(state->status_log, &entry, sizeof(entry));
						state->task->status_count++;
						if (abort_ts > state->task->target_ts &&
							abort_ts <= state->task->query_now_ts)
							state->task->target_abort_count++;
					}
				}
				break;
			}
		default:
			break;
	}

	return true;
}

static bool
fb_wal_serial_xact_fill_visitor(XLogReaderState *reader, void *arg)
{
	FbWalSerialXactVisitorState *state = (FbWalSerialXactVisitorState *) arg;
	uint32 resolved_now;

	if (state == NULL || XLogRecGetRmid(reader) != RM_XACT_ID)
		return true;

	resolved_now =
		fb_note_xact_status_for_touched(reader,
										state->touched_xids,
										state->unsafe_xids,
										state->index,
										state->ctx);
	if (resolved_now > 0 && state->remaining_xids > resolved_now)
		state->remaining_xids -= resolved_now;
	else if (resolved_now > 0)
		state->remaining_xids = 0;
	if (!state->ctx->unsafe && state->remaining_xids == 0)
		return false;
	return !state->ctx->unsafe;
}

static bool
fb_summary_fill_xact_statuses(const FbRelationInfo *info,
							  FbWalScanContext *ctx,
							  FbWalRecordIndex *index,
							  const FbWalVisitWindow *windows,
							  uint32 window_count,
							  HTAB *touched_xids,
							  HTAB *unsafe_xids)
{
	FbWalSegmentEntry *segments;
	FbWalVisitWindow full_window;
	const FbWalVisitWindow *source_windows;
	uint32 source_window_count;
	bool *segment_seen;
	HASH_SEQ_STATUS status;
	HASH_SEQ_STATUS unsafe_status;
	FbTouchedXidEntry *touched_entry;
	FbUnsafeXidEntry *unsafe_entry_iter;
	uint32 hits = 0;
	uint32 unresolved = 0;
	uint32 i;

	if (info == NULL || ctx == NULL || index == NULL || touched_xids == NULL)
		return false;
	if (hash_get_num_entries(touched_xids) <= 0 &&
		(unsafe_xids == NULL || hash_get_num_entries(unsafe_xids) <= 0))
	{
		ctx->summary_xid_hits = 0;
		ctx->summary_xid_fallback = 0;
		return true;
	}

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	if (ctx->summary_cache == NULL)
		ctx->summary_cache = fb_summary_query_cache_create(CurrentMemoryContext);

	if (window_count == 0 || windows == NULL)
	{
		full_window.segments = segments;
		full_window.segment_count = ctx->resolved_segment_count;
		full_window.start_lsn = ctx->start_lsn;
		full_window.end_lsn = ctx->end_lsn;
		full_window.read_end_lsn = ctx->end_lsn;
		source_windows = &full_window;
		source_window_count = 1;
	}
	else
	{
		source_windows = windows;
		source_window_count = window_count;
	}

	segment_seen = palloc0(sizeof(bool) * ctx->resolved_segment_count);
	for (i = 0; i < source_window_count; i++)
	{
		const FbWalVisitWindow *window = &source_windows[i];
		uint32 j;

		for (j = 0; j < window->segment_count; j++)
		{
			uint32 segment_index = (uint32) ((window->segments - segments) + j);
			FbWalSegmentEntry *segment = &window->segments[j];
			const FbSummaryXidOutcome *outcomes = NULL;
			uint32 outcome_count = 0;
			uint32 outcome_index;

			if (segment_index >= ctx->resolved_segment_count)
				elog(ERROR, "summary xid fill segment index is out of bounds");
			if (segment_seen[segment_index])
				continue;
			segment_seen[segment_index] = true;
			ctx->summary_xid_segments_read++;

			if (!fb_summary_segment_lookup_xid_outcome_slice_cached(segment->path,
																   segment->bytes,
																   segment->timeline_id,
																   segment->segno,
																   ctx->wal_seg_size,
																   segment->source_kind,
																   ctx->summary_cache,
																   &outcomes,
																   &outcome_count))
				continue;

			for (outcome_index = 0; outcome_index < outcome_count; outcome_index++)
			{
				hits += fb_apply_summary_xid_outcome(ctx,
												 index,
												 touched_xids,
												 unsafe_xids,
												 &outcomes[outcome_index]);
			}
		}
	}
	pfree(segment_seen);

	hash_seq_init(&status, touched_xids);
	while ((touched_entry = (FbTouchedXidEntry *) hash_seq_search(&status)) != NULL)
	{
		FbXidStatusEntry *entry;

		entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
												 &touched_entry->xid,
												 HASH_FIND,
												 NULL);
		if (entry == NULL || entry->status == FB_WAL_XID_UNKNOWN)
			unresolved++;
	}
	if (unsafe_xids != NULL)
	{
		hash_seq_init(&unsafe_status, unsafe_xids);
		while ((unsafe_entry_iter = (FbUnsafeXidEntry *) hash_seq_search(&unsafe_status)) != NULL)
		{
			FbXidStatusEntry *entry;

			entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													 &unsafe_entry_iter->xid,
													 HASH_FIND,
													 NULL);
			if (entry == NULL || entry->status == FB_WAL_XID_UNKNOWN)
				unresolved++;
		}
	}

	ctx->summary_xid_hits = hits;
	ctx->summary_xid_fallback = unresolved;
	return unresolved == 0;
}

static uint32
fb_apply_summary_xid_outcome(FbWalScanContext *ctx,
							 FbWalRecordIndex *index,
							 HTAB *touched_xids,
							 HTAB *unsafe_xids,
							 const FbSummaryXidOutcome *outcome)
{
	TransactionId xid;
	FbUnsafeXidEntry *unsafe_entry;
	FbXidStatusEntry *status_entry;
	bool is_touched;
	bool is_unsafe;
	bool found = false;
	bool already_resolved;

	if (ctx == NULL || index == NULL || outcome == NULL)
		return 0;

	xid = outcome->xid;
	is_touched = fb_hash_has_xid(touched_xids, xid);
	is_unsafe = (unsafe_xids != NULL &&
				 fb_find_unsafe_xid(unsafe_xids, xid) != NULL);
	if (!is_touched && !is_unsafe)
		return 0;

	status_entry = fb_get_xid_status_entry(index->xid_statuses, xid, &found);
	already_resolved = (found && status_entry->status != FB_WAL_XID_UNKNOWN);
	fb_note_xid_status(index->xid_statuses,
					   xid,
					   (FbWalXidStatus) outcome->status,
					   outcome->commit_ts,
					   outcome->commit_lsn);
	if (!already_resolved)
	{
		if (is_touched &&
			outcome->status == FB_WAL_XID_COMMITTED &&
			outcome->commit_ts > ctx->target_ts &&
			outcome->commit_ts <= ctx->query_now_ts)
			index->target_commit_count++;
		else if (is_touched &&
				 outcome->status == FB_WAL_XID_ABORTED &&
				 outcome->commit_ts > ctx->target_ts &&
				 outcome->commit_ts <= ctx->query_now_ts)
			index->target_abort_count++;
	}

	unsafe_entry = fb_find_unsafe_xid(unsafe_xids, xid);
	if (unsafe_entry != NULL &&
		outcome->status == FB_WAL_XID_COMMITTED &&
		outcome->commit_ts > ctx->target_ts &&
		outcome->commit_ts <= ctx->query_now_ts &&
		fb_unsafe_entry_requires_reject(unsafe_entry))
	{
		fb_capture_unsafe_context(ctx, unsafe_entry, xid, outcome->commit_ts);
		fb_mark_unsafe(ctx, unsafe_entry->reason);
	}

	return already_resolved ? 0 : 1;
}

static bool
fb_summary_fill_exact_xact_statuses(FbWalScanContext *ctx,
									FbWalRecordIndex *index,
									const FbWalVisitWindow *windows,
									uint32 window_count,
									HTAB *touched_xids,
									HTAB *unsafe_xids)
{
	FbWalSegmentEntry *segments;
	FbWalVisitWindow full_window;
	const FbWalVisitWindow *source_windows;
	uint32 source_window_count;
	bool *segment_seen;
	TransactionId *xids = NULL;
	const FbSummaryXidOutcome *outcomes = NULL;
	HASH_SEQ_STATUS hash_status;
	FbTouchedXidEntry *touched_entry;
	FbUnsafeXidEntry *unsafe_entry;
	uint32 xid_count = 0;
	uint32 unresolved = 0;
	uint32 hits = 0;
	uint32 i;

	if (ctx == NULL || index == NULL)
		return false;
	if ((touched_xids == NULL || hash_get_num_entries(touched_xids) <= 0) &&
		(unsafe_xids == NULL || hash_get_num_entries(unsafe_xids) <= 0))
		return true;

	if (touched_xids != NULL)
		xid_count += (uint32) hash_get_num_entries(touched_xids);
	if (unsafe_xids != NULL)
	{
		hash_seq_init(&hash_status, unsafe_xids);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&hash_status)) != NULL)
		{
			if (touched_xids != NULL && fb_hash_has_xid(touched_xids, unsafe_entry->xid))
				continue;
			xid_count++;
		}
	}
	if (xid_count == 0)
		return true;

	xids = palloc(sizeof(*xids) * xid_count);
	xid_count = 0;
	if (touched_xids != NULL)
	{
		hash_seq_init(&hash_status, touched_xids);
		while ((touched_entry = (FbTouchedXidEntry *) hash_seq_search(&hash_status)) != NULL)
			xids[xid_count++] = touched_entry->xid;
	}
	if (unsafe_xids != NULL)
	{
		hash_seq_init(&hash_status, unsafe_xids);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&hash_status)) != NULL)
		{
			if (touched_xids != NULL && fb_hash_has_xid(touched_xids, unsafe_entry->xid))
				continue;
			xids[xid_count++] = unsafe_entry->xid;
		}
	}
	if (xid_count > 1)
		qsort(xids, xid_count, sizeof(*xids), fb_transactionid_cmp);

	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	if (window_count == 0 || windows == NULL)
	{
		full_window.segments = segments;
		full_window.segment_count = ctx->resolved_segment_count;
		full_window.start_lsn = ctx->start_lsn;
		full_window.end_lsn = ctx->end_lsn;
		full_window.read_end_lsn = ctx->end_lsn;
		source_windows = &full_window;
		source_window_count = 1;
	}
	else
	{
		source_windows = windows;
		source_window_count = window_count;
	}
	segment_seen = palloc0(sizeof(bool) * ctx->resolved_segment_count);
	for (i = 0; i < source_window_count; i++)
	{
		const FbWalVisitWindow *window = &source_windows[i];
		uint32 j;

		for (j = 0; j < window->segment_count; j++)
		{
			FbWalSegmentEntry *segment = &window->segments[j];
			uint32 segment_index = (uint32) ((segment - segments));
			uint32 xid_index = 0;
			uint32 outcome_count = 0;
			uint32 outcome_index = 0;

			if (segment_index >= ctx->resolved_segment_count)
				elog(ERROR, "summary exact xid fill segment index is out of bounds");
			if (segment_seen[segment_index])
				continue;
			segment_seen[segment_index] = true;

			if (!fb_summary_segment_lookup_xid_outcome_slice_cached(segment->path,
																   segment->bytes,
																   segment->timeline_id,
																   segment->segno,
																   ctx->wal_seg_size,
																   segment->source_kind,
																   ctx->summary_cache,
																   &outcomes,
																   &outcome_count))
				continue;
			ctx->summary_xid_exact_segments_read++;
			if (outcome_count == 0)
				continue;

			while (xid_index < xid_count && outcome_index < outcome_count)
			{
				if (xids[xid_index] < outcomes[outcome_index].xid)
				{
					xid_index++;
					continue;
				}
				if (xids[xid_index] > outcomes[outcome_index].xid)
				{
					outcome_index++;
					continue;
				}

				hits += fb_apply_summary_xid_outcome(ctx,
												 index,
												 touched_xids,
												 unsafe_xids,
												 &outcomes[outcome_index]);
				xid_index++;
				outcome_index++;
			}

			if (ctx->unsafe)
				break;
		}
		if (ctx->unsafe)
			break;
	}

	pfree(segment_seen);
	if (xids != NULL)
		pfree(xids);

	if (touched_xids != NULL)
	{
		hash_seq_init(&hash_status, touched_xids);
		while ((touched_entry = (FbTouchedXidEntry *) hash_seq_search(&hash_status)) != NULL)
		{
			FbXidStatusEntry *entry;

			entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													 &touched_entry->xid,
													 HASH_FIND,
													 NULL);
			if (entry == NULL || entry->status == FB_WAL_XID_UNKNOWN)
				unresolved++;
		}
	}
	if (unsafe_xids != NULL)
	{
		hash_seq_init(&hash_status, unsafe_xids);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&hash_status)) != NULL)
		{
			FbXidStatusEntry *entry;

			entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													 &unsafe_entry->xid,
													 HASH_FIND,
													 NULL);
			if (entry == NULL || entry->status == FB_WAL_XID_UNKNOWN)
				unresolved++;
		}
	}

	ctx->summary_xid_exact_hits = hits;
	ctx->summary_xid_fallback = unresolved;
	return ctx->unsafe || unresolved == 0;
}

static void
fb_build_unresolved_xid_fallback_sets(FbWalRecordIndex *index,
									  HTAB *touched_xids,
									  HTAB *unsafe_xids,
									  HTAB **fallback_touched_out,
									  HTAB **fallback_unsafe_out)
{
	HASH_SEQ_STATUS status;
	FbTouchedXidEntry *touched_entry;
	FbUnsafeXidEntry *unsafe_entry;
	HTAB *fallback_touched = NULL;
	HTAB *fallback_unsafe = NULL;

	if (fallback_touched_out != NULL)
		*fallback_touched_out = NULL;
	if (fallback_unsafe_out != NULL)
		*fallback_unsafe_out = NULL;
	if (index == NULL)
		return;

	if (touched_xids != NULL)
	{
		hash_seq_init(&status, touched_xids);
		while ((touched_entry = (FbTouchedXidEntry *) hash_seq_search(&status)) != NULL)
		{
			FbXidStatusEntry *entry;

			entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													 &touched_entry->xid,
													 HASH_FIND,
													 NULL);
			if (entry != NULL && entry->status != FB_WAL_XID_UNKNOWN)
				continue;
			if (fallback_touched == NULL)
				fallback_touched = fb_create_touched_xid_hash();
			hash_search(fallback_touched, &touched_entry->xid, HASH_ENTER, NULL);
		}
	}

	if (unsafe_xids != NULL)
	{
		hash_seq_init(&status, unsafe_xids);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&status)) != NULL)
		{
			FbXidStatusEntry *entry;
			bool found = false;
			FbUnsafeXidEntry *dst;

			entry = (FbXidStatusEntry *) hash_search(index->xid_statuses,
													 &unsafe_entry->xid,
													 HASH_FIND,
													 &found);
			if (found && entry->status != FB_WAL_XID_UNKNOWN)
				continue;
			if (fallback_unsafe == NULL)
				fallback_unsafe = fb_create_unsafe_xid_hash();
			dst = (FbUnsafeXidEntry *) hash_search(fallback_unsafe,
												   &unsafe_entry->xid,
												   HASH_ENTER,
												   &found);
			if (!found)
				*dst = *unsafe_entry;
		}
	}

	if (fallback_touched_out != NULL)
		*fallback_touched_out = fallback_touched;
	else if (fallback_touched != NULL)
		hash_destroy(fallback_touched);

	if (fallback_unsafe_out != NULL)
		*fallback_unsafe_out = fallback_unsafe;
	else if (fallback_unsafe != NULL)
		hash_destroy(fallback_unsafe);
}

static void
fb_wal_fill_xact_statuses_serial(const FbRelationInfo *info,
								 FbWalScanContext *ctx,
								 FbWalRecordIndex *index,
								 const FbWalVisitWindow *windows,
								 uint32 window_count,
								 HTAB *touched_xids,
								 HTAB *unsafe_xids)
{
	FbWalSerialXactVisitorState state;
	FbWalVisitWindow *fallback_windows = NULL;
	HTAB *fallback_touched = NULL;
	HTAB *fallback_unsafe = NULL;
	HASH_SEQ_STATUS hash_status;
	FbUnsafeXidEntry *unsafe_entry;
	uint32 fallback_window_count = 0;
	uint32 i;

	if (ctx == NULL || index == NULL || touched_xids == NULL || unsafe_xids == NULL)
		return;
	if (hash_get_num_entries(touched_xids) <= 0 &&
		hash_get_num_entries(unsafe_xids) <= 0)
		return;
	ctx->xact_summary_spool_hits =
		fb_wal_apply_xact_summary_log(ctx, index, touched_xids, unsafe_xids);
	if (ctx->unsafe)
		return;

	fb_build_unresolved_xid_fallback_sets(index,
										  touched_xids,
										  unsafe_xids,
										  &fallback_touched,
										  &fallback_unsafe);
	if ((fallback_touched == NULL || hash_get_num_entries(fallback_touched) <= 0) &&
		(fallback_unsafe == NULL || hash_get_num_entries(fallback_unsafe) <= 0))
	{
		ctx->summary_xid_fallback = 0;
		goto cleanup;
	}
	if (fb_summary_fill_xact_statuses(info,
									  ctx,
									  index,
									  windows,
									  window_count,
									  (fallback_touched != NULL) ? fallback_touched : touched_xids,
									  (fallback_unsafe != NULL) ? fallback_unsafe : unsafe_xids))
		goto cleanup;
	if (fallback_touched != NULL)
	{
		hash_destroy(fallback_touched);
		fallback_touched = NULL;
	}
	if (fallback_unsafe != NULL)
	{
		hash_destroy(fallback_unsafe);
		fallback_unsafe = NULL;
	}
	fb_build_unresolved_xid_fallback_sets(index,
										  touched_xids,
										  unsafe_xids,
										  &fallback_touched,
										  &fallback_unsafe);
	if ((fallback_touched == NULL || hash_get_num_entries(fallback_touched) <= 0) &&
		(fallback_unsafe == NULL || hash_get_num_entries(fallback_unsafe) <= 0))
		goto cleanup;

	fallback_window_count =
		fb_build_xact_fallback_visit_windows(ctx, windows, window_count, &fallback_windows);
	if (fb_summary_fill_exact_xact_statuses(ctx,
											index,
											fallback_windows,
											fallback_window_count,
											fallback_touched,
											fallback_unsafe))
		goto cleanup;
	if (fallback_touched != NULL)
	{
		hash_destroy(fallback_touched);
		fallback_touched = NULL;
	}
	if (fallback_unsafe != NULL)
	{
		hash_destroy(fallback_unsafe);
		fallback_unsafe = NULL;
	}
	fb_build_unresolved_xid_fallback_sets(index,
										  touched_xids,
										  unsafe_xids,
										  &fallback_touched,
										  &fallback_unsafe);
	if ((fallback_touched == NULL || hash_get_num_entries(fallback_touched) <= 0) &&
		(fallback_unsafe == NULL || hash_get_num_entries(fallback_unsafe) <= 0))
		goto cleanup;

	if (fb_summary_fill_exact_xact_statuses(ctx,
											index,
											NULL,
											0,
											fallback_touched,
											fallback_unsafe))
		goto cleanup;
	if (fallback_touched != NULL)
	{
		hash_destroy(fallback_touched);
		fallback_touched = NULL;
	}
	if (fallback_unsafe != NULL)
	{
		hash_destroy(fallback_unsafe);
		fallback_unsafe = NULL;
	}
	fb_build_unresolved_xid_fallback_sets(index,
										  touched_xids,
										  unsafe_xids,
										  &fallback_touched,
										  &fallback_unsafe);
	if ((fallback_touched == NULL || hash_get_num_entries(fallback_touched) <= 0) &&
		(fallback_unsafe == NULL || hash_get_num_entries(fallback_unsafe) <= 0))
		goto cleanup;
	ctx->xact_fallback_windows = fallback_window_count;
	ctx->xact_fallback_covered_segments =
		fb_wal_count_window_segments(fallback_windows, fallback_window_count);

	MemSet(&state, 0, sizeof(state));
	state.ctx = ctx;
	state.index = index;
	state.touched_xids = (fallback_touched != NULL) ? fallback_touched : touched_xids;
	state.unsafe_xids = (fallback_unsafe != NULL) ? fallback_unsafe : unsafe_xids;
	if (state.touched_xids != NULL)
		state.remaining_xids += (uint32) hash_get_num_entries(state.touched_xids);
	if (state.unsafe_xids != NULL)
	{
		hash_seq_init(&hash_status, state.unsafe_xids);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&hash_status)) != NULL)
		{
			if (state.touched_xids != NULL &&
				fb_hash_has_xid(state.touched_xids, unsafe_entry->xid))
				continue;
			state.remaining_xids++;
		}
	}

	if (fallback_window_count == 0 || fallback_windows == NULL)
	{
		fb_wal_visit_records(ctx, fb_wal_serial_xact_fill_visitor, &state);
		goto cleanup;
	}

	for (i = 0; i < fallback_window_count; i++)
	{
		fb_wal_visit_window(ctx, &fallback_windows[i],
							fb_wal_serial_xact_fill_visitor,
							&state);
		if (ctx->unsafe || state.remaining_xids == 0)
			break;
	}

cleanup:
	if (fallback_windows != NULL)
		pfree(fallback_windows);
	if (fallback_touched != NULL)
		hash_destroy(fallback_touched);
	if (fallback_unsafe != NULL)
		hash_destroy(fallback_unsafe);
}

/*
 * fb_scan_record_visitor
 *    WAL helper.
 */

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

	if (ctx->segment_prefilter_used && !ctx->current_segment_may_hit)
	{
		if (rmid == RM_HEAP_ID)
		{
			uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

			if (info_code == XLOG_HEAP_TRUNCATE &&
				fb_heap_truncate_matches_relation(reader, info))
				fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
								   FB_WAL_UNSAFE_TRUNCATE,
								   FB_WAL_UNSAFE_SCOPE_NONE,
								   FB_WAL_STORAGE_CHANGE_UNKNOWN,
								   reader->ReadRecPtr,
								   ctx);
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
								   FB_WAL_UNSAFE_REWRITE,
								   FB_WAL_UNSAFE_SCOPE_NONE,
								   FB_WAL_STORAGE_CHANGE_UNKNOWN,
								   reader->ReadRecPtr,
								   ctx);
			}
		}
		else if (rmid == RM_SMGR_ID)
		{
			FbWalUnsafeScope scope;
			FbWalStorageChangeOp op;

			if (fb_smgr_record_matches_relation(reader, info, &scope, &op))
				fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
								   FB_WAL_UNSAFE_STORAGE_CHANGE,
								   scope,
								   op,
								   reader->ReadRecPtr,
								   ctx);
		}
		else if (rmid == RM_STANDBY_ID)
		{
			TransactionId lock_xid = InvalidTransactionId;
			FbWalUnsafeScope scope;

			if (fb_standby_record_matches_relation(reader, info, &lock_xid, &scope))
				fb_mark_xid_unsafe(unsafe_xids, lock_xid,
								   FB_WAL_UNSAFE_STORAGE_CHANGE,
								   scope,
								   FB_WAL_STORAGE_CHANGE_STANDBY_LOCK,
								   reader->ReadRecPtr,
								   ctx);
		}

		if (rmid == RM_XACT_ID)
			fb_note_xact_record(reader, touched_xids, unsafe_xids, ctx);
		return !ctx->unsafe;
	}

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_HEAP_TRUNCATE &&
			fb_heap_truncate_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_TRUNCATE,
							   FB_WAL_UNSAFE_SCOPE_NONE,
							   FB_WAL_STORAGE_CHANGE_UNKNOWN,
							   reader->ReadRecPtr,
							   ctx);
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
							   FB_WAL_UNSAFE_REWRITE,
							   FB_WAL_UNSAFE_SCOPE_NONE,
							   FB_WAL_STORAGE_CHANGE_UNKNOWN,
							   reader->ReadRecPtr,
							   ctx);
		}
	}
	else if (rmid == RM_XLOG_ID)
	{
		/* checkpoint records are already handled above; nothing else to do here */
	}
	else if (rmid == RM_SMGR_ID)
	{
		FbWalUnsafeScope scope;
		FbWalStorageChangeOp op;

		if (fb_smgr_record_matches_relation(reader, info, &scope, &op))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_STORAGE_CHANGE,
							   scope,
							   op,
							   reader->ReadRecPtr,
							   ctx);
	}
	else if (rmid == RM_STANDBY_ID)
	{
		TransactionId lock_xid = InvalidTransactionId;
		FbWalUnsafeScope scope;

		if (fb_standby_record_matches_relation(reader, info, &lock_xid, &scope))
			fb_mark_xid_unsafe(unsafe_xids, lock_xid,
							   FB_WAL_UNSAFE_STORAGE_CHANGE,
							   scope,
							   FB_WAL_STORAGE_CHANGE_STANDBY_LOCK,
							   reader->ReadRecPtr,
							   ctx);
	}

	if (fb_record_touches_relation(reader, info))
		fb_mark_record_xids_touched(reader, touched_xids, ctx);

	if (rmid == RM_XACT_ID)
		fb_note_xact_record(reader, touched_xids, unsafe_xids, ctx);

	return !ctx->unsafe;
}

/*
 * fb_index_record_visitor
 *    WAL helper.
 */

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
	bool payload_emit_visible = true;
	bool payload_locator_visible = false;

	if ((state->capture_payload || state->count_only_capture) &&
		state->payload_locator_count > 0)
	{
		if (state->payload_locator_index < state->payload_locator_count)
		{
			XLogRecPtr locator_lsn =
				state->payload_locators[state->payload_locator_index].record_start_lsn;

			if (locator_lsn < reader->ReadRecPtr)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("summary payload locator skipped during batched visit"),
						 errdetail("Expected locator at %X/%08X before reader advanced to %X/%08X.",
								   LSN_FORMAT_ARGS(locator_lsn),
								   LSN_FORMAT_ARGS(reader->ReadRecPtr))));
			payload_locator_visible = (locator_lsn == reader->ReadRecPtr);
		}
		payload_emit_visible = payload_locator_visible;
	}
	else if ((state->capture_payload || state->count_only_capture) &&
		!XLogRecPtrIsInvalid(state->payload_emit_end_lsn))
		payload_emit_visible =
			(reader->EndRecPtr >= state->payload_emit_start_lsn &&
			 reader->ReadRecPtr < state->payload_emit_end_lsn);
	if ((state->capture_payload || state->count_only_capture) &&
		reader->ReadRecPtr >= index->anchor_redo_lsn &&
		payload_emit_visible)
		index->payload_scanned_record_count++;

	if (state->collect_metadata)
	{
		fb_note_checkpoint_record(reader, ctx);
		fb_maybe_activate_tail_payload_capture(reader, state);
	}

	if (state->collect_metadata &&
		ctx->segment_prefilter_used && !ctx->current_segment_may_hit)
	{
		if (rmid == RM_HEAP_ID)
		{
			uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

			if (info_code == XLOG_HEAP_TRUNCATE &&
				fb_heap_truncate_matches_relation(reader, info))
				fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
								   FB_WAL_UNSAFE_TRUNCATE,
								   FB_WAL_UNSAFE_SCOPE_NONE,
								   FB_WAL_STORAGE_CHANGE_UNKNOWN,
								   reader->ReadRecPtr,
								   ctx);
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
								   FB_WAL_UNSAFE_REWRITE,
								   FB_WAL_UNSAFE_SCOPE_NONE,
								   FB_WAL_STORAGE_CHANGE_UNKNOWN,
								   reader->ReadRecPtr,
								   ctx);
			}
		}
		else if (rmid == RM_SMGR_ID)
		{
			FbWalUnsafeScope scope;
			FbWalStorageChangeOp op;

			if (fb_smgr_record_matches_relation(reader, info, &scope, &op))
				fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
								   FB_WAL_UNSAFE_STORAGE_CHANGE,
								   scope,
								   op,
								   reader->ReadRecPtr,
								   ctx);
		}
		else if (rmid == RM_STANDBY_ID)
		{
			TransactionId lock_xid = InvalidTransactionId;
			FbWalUnsafeScope scope;

			if (fb_standby_record_matches_relation(reader, info, &lock_xid, &scope))
				fb_mark_xid_unsafe(unsafe_xids, lock_xid,
								   FB_WAL_UNSAFE_STORAGE_CHANGE,
								   scope,
								   FB_WAL_STORAGE_CHANGE_STANDBY_LOCK,
								   reader->ReadRecPtr,
								   ctx);
		}

		if (rmid == RM_XACT_ID)
			fb_note_xact_status_for_touched(reader, touched_xids, unsafe_xids,
											index, ctx);
		return !ctx->unsafe;
	}

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (state->collect_metadata &&
			info_code == XLOG_HEAP_TRUNCATE &&
			fb_heap_truncate_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_TRUNCATE,
							   FB_WAL_UNSAFE_SCOPE_NONE,
							   FB_WAL_STORAGE_CHANGE_UNKNOWN,
							   reader->ReadRecPtr,
							   ctx);

		if (fb_heap_record_matches_target(reader, info))
		{
			uint8 heap_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

			switch (heap_code)
			{
				case XLOG_HEAP_INSERT:
					if (state->collect_metadata)
						fb_mark_record_xids_touched(reader, touched_xids, ctx);
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->collect_metadata)
						fb_count_only_note_metadata_record(state, reader,
														   FB_WAL_RECORD_HEAP_INSERT);
					if (state->count_only_capture &&
						payload_emit_visible)
						fb_index_note_count_only_record(reader, info,
														FB_WAL_RECORD_HEAP_INSERT,
														index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_INSERT, state);
					break;
				case XLOG_HEAP_DELETE:
					if (state->collect_metadata)
						fb_mark_record_xids_touched(reader, touched_xids, ctx);
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->collect_metadata)
						fb_count_only_note_metadata_record(state, reader,
														   FB_WAL_RECORD_HEAP_DELETE);
					if (state->count_only_capture &&
						payload_emit_visible)
						fb_index_note_count_only_record(reader, info,
														FB_WAL_RECORD_HEAP_DELETE,
														index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_DELETE, state);
					break;
				case XLOG_HEAP_UPDATE:
					if (state->collect_metadata)
						fb_mark_record_xids_touched(reader, touched_xids, ctx);
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->collect_metadata)
						fb_count_only_note_metadata_record(state, reader,
														   FB_WAL_RECORD_HEAP_UPDATE);
					if (state->count_only_capture &&
						payload_emit_visible)
						fb_index_note_count_only_record(reader, info,
														FB_WAL_RECORD_HEAP_UPDATE,
														index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_UPDATE, state);
					break;
				case XLOG_HEAP_HOT_UPDATE:
					if (state->collect_metadata)
						fb_mark_record_xids_touched(reader, touched_xids, ctx);
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->collect_metadata)
						fb_count_only_note_metadata_record(state, reader,
														   FB_WAL_RECORD_HEAP_HOT_UPDATE);
					if (state->count_only_capture &&
						payload_emit_visible)
						fb_index_note_count_only_record(reader, info,
														FB_WAL_RECORD_HEAP_HOT_UPDATE,
														index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_HOT_UPDATE, state);
					break;
				case XLOG_HEAP_CONFIRM:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible &&
						fb_wal_payload_kind_enabled(FB_WAL_RECORD_HEAP_CONFIRM))
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_CONFIRM, state);
					break;
				case XLOG_HEAP_LOCK:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible &&
						fb_wal_payload_kind_enabled(FB_WAL_RECORD_HEAP_LOCK))
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_LOCK, state);
					break;
				case XLOG_HEAP_INPLACE:
					if (state->collect_metadata)
						fb_mark_record_xids_touched(reader, touched_xids, ctx);
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn &&
						payload_emit_visible)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_INPLACE, state);
					break;
				default:
					break;
			}
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (state->collect_metadata &&
			info_code == XLOG_HEAP2_REWRITE &&
			fb_heap_rewrite_matches_relation(reader, info))
		{
			xl_heap_rewrite_mapping *xlrec;

			xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);
			fb_mark_xid_unsafe(unsafe_xids, xlrec->mapped_xid,
							   FB_WAL_UNSAFE_REWRITE,
							   FB_WAL_UNSAFE_SCOPE_NONE,
							   FB_WAL_STORAGE_CHANGE_UNKNOWN,
							   reader->ReadRecPtr,
							   ctx);
		}
		else if (
#if PG_VERSION_NUM >= 170000
				 (info_code == XLOG_HEAP2_PRUNE_ON_ACCESS ||
				  info_code == XLOG_HEAP2_PRUNE_VACUUM_SCAN ||
				  info_code == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP) &&
#elif PG_VERSION_NUM >= 140000
				 info_code == XLOG_HEAP2_PRUNE &&
#else
				 info_code == XLOG_HEAP2_CLEAN &&
#endif
				 fb_record_touches_relation(reader, info))
		{
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn &&
				payload_emit_visible)
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_PRUNE, state);
		}
		else if (info_code == XLOG_HEAP2_VISIBLE &&
				 fb_record_touches_relation(reader, info))
		{
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn &&
				payload_emit_visible &&
				fb_wal_payload_kind_enabled(FB_WAL_RECORD_HEAP2_VISIBLE))
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_VISIBLE, state);
		}
		else if (info_code == XLOG_HEAP2_MULTI_INSERT &&
				 fb_record_touches_relation(reader, info))
		{
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->collect_metadata)
				fb_mark_record_xids_touched(reader, touched_xids, ctx);
			if (state->collect_metadata)
				fb_count_only_note_metadata_record(state, reader,
												   FB_WAL_RECORD_HEAP2_MULTI_INSERT);
			if (state->count_only_capture &&
				payload_emit_visible)
				fb_index_note_count_only_record(reader, info,
												FB_WAL_RECORD_HEAP2_MULTI_INSERT,
												index);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn &&
				payload_emit_visible)
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_MULTI_INSERT, state);
		}
		else if (info_code == XLOG_HEAP2_LOCK_UPDATED &&
				 fb_record_touches_relation(reader, info))
		{
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn &&
				payload_emit_visible &&
				fb_wal_payload_kind_enabled(FB_WAL_RECORD_HEAP2_LOCK_UPDATED))
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_LOCK_UPDATED, state);
		}
		else if (info_code == XLOG_HEAP2_NEW_CID &&
				 fb_record_touches_relation(reader, info))
		{
			/*
			 * PG18 redo treats NEW_CID as a logical-decoding-only record.
			 * Recognize it explicitly, but keep it out of page replay.
			 */
		}
	}
	else if (rmid == RM_XLOG_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if ((info_code == XLOG_FPI || info_code == XLOG_FPI_FOR_HINT) &&
			fb_record_touches_relation(reader, info))
		{
			uint8		payload_kind = (info_code == XLOG_FPI) ?
				FB_WAL_RECORD_XLOG_FPI :
				FB_WAL_RECORD_XLOG_FPI_FOR_HINT;

			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn &&
				payload_emit_visible &&
				fb_wal_payload_kind_enabled(payload_kind))
				fb_copy_xlog_fpi_record_ref(reader, info, payload_kind, state);
		}
	}
	else if (state->collect_metadata && rmid == RM_SMGR_ID)
	{
		FbWalUnsafeScope scope;
		FbWalStorageChangeOp op;

		if (fb_smgr_record_matches_relation(reader, info, &scope, &op))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_STORAGE_CHANGE,
							   scope,
							   op,
							   reader->ReadRecPtr,
							   ctx);
	}
	else if (state->collect_metadata && rmid == RM_STANDBY_ID)
	{
		TransactionId lock_xid = InvalidTransactionId;
		FbWalUnsafeScope scope;

		if (fb_standby_record_matches_relation(reader, info, &lock_xid, &scope))
			fb_mark_xid_unsafe(unsafe_xids, lock_xid,
							   FB_WAL_UNSAFE_STORAGE_CHANGE,
							   scope,
							   FB_WAL_STORAGE_CHANGE_STANDBY_LOCK,
							   reader->ReadRecPtr,
							   ctx);
	}

	if (state->collect_metadata && rmid == RM_XACT_ID)
		fb_append_xact_summary(reader, state);
	if (state->collect_metadata && state->collect_xact_statuses &&
		rmid == RM_XACT_ID)
		fb_note_xact_status_for_touched(reader, touched_xids, unsafe_xids,
										index, ctx);
	if (payload_locator_visible)
		state->payload_locator_index++;

	return !ctx->unsafe;
}

/*
 * fb_wal_visit_records
 *    WAL entry point.
 */

void
fb_wal_visit_records(FbWalScanContext *ctx, FbWalRecordVisitor visitor, void *arg)
{
	FbWalVisitWindow window;

	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	window.segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	window.segment_count = ctx->resolved_segment_count;
	window.start_lsn = ctx->start_lsn;
	window.end_lsn = ctx->end_lsn;

	fb_wal_visit_window(ctx, &window, visitor, arg);
}

void
fb_wal_visit_resolved_records(const FbWalRecordIndex *index,
							 XLogRecPtr start_lsn,
							 XLogRecPtr end_lsn,
							 FbWalRecordVisitor visitor,
							 void *arg)
{
	FbWalScanContext ctx;
	XLogRecPtr fallback_end_lsn;

	if (index == NULL ||
		index->resolved_segments == NULL ||
		index->resolved_segment_count == 0)
		return;

	MemSet(&ctx, 0, sizeof(ctx));
	ctx.timeline_id = index->resolved_segments[0].timeline_id;
	ctx.wal_seg_size = index->wal_seg_size;
	ctx.resolved_segments = (void *) index->resolved_segments;
	ctx.resolved_segment_count = index->resolved_segment_count;
	ctx.current_segment_may_hit = true;
	ctx.start_lsn = start_lsn;
	if (XLogRecPtrIsInvalid(ctx.start_lsn))
		XLogSegNoOffsetToRecPtr(index->resolved_segments[0].segno, 0,
								index->wal_seg_size, ctx.start_lsn);
	if (XLogRecPtrIsInvalid(end_lsn))
	{
		XLogSegNoOffsetToRecPtr(index->resolved_segments[index->resolved_segment_count - 1].segno,
								0,
								index->wal_seg_size,
								fallback_end_lsn);
		ctx.end_lsn = fallback_end_lsn + index->wal_seg_size;
	}
	else
		ctx.end_lsn = end_lsn;

	fb_wal_visit_records(&ctx, visitor, arg);
}

/*
 * fb_wal_visit_window
 *    WAL helper.
 */

static void
fb_wal_visit_window(FbWalScanContext *ctx, const FbWalVisitWindow *window,
					FbWalRecordVisitor visitor, void *arg)
{
	FbWalReaderPrivate private;
	XLogReaderState *reader;
	XLogRecPtr first_record;
	void *saved_segments;
	uint32 saved_segment_count;
	bool *saved_hit_map;
	bool saved_prefilter_used;
	bool saved_current_segment_may_hit;

	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (window == NULL || window->segments == NULL || window->segment_count == 0)
		return;

	saved_segments = ctx->resolved_segments;
	saved_segment_count = ctx->resolved_segment_count;
	saved_hit_map = ctx->segment_hit_map;
	saved_prefilter_used = ctx->segment_prefilter_used;
	saved_current_segment_may_hit = ctx->current_segment_may_hit;

	ctx->resolved_segments = window->segments;
	ctx->resolved_segment_count = window->segment_count;
	ctx->segment_hit_map = NULL;
	ctx->segment_prefilter_used = false;
	ctx->current_segment_may_hit = true;
	ctx->visited_segment_count += window->segment_count;

	MemSet(&private, 0, sizeof(private));
	private.timeline_id = ctx->timeline_id;
	private.endptr = window->end_lsn;
	private.current_file = -1;
	private.open_pattern = FB_WAL_OPEN_SEQUENTIAL;
	private.ctx = ctx;

#if PG_VERSION_NUM < 130000
	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_wal_read_page, &private);
#else
	{
		char *archive_dir;

		archive_dir = fb_get_effective_archive_dir();
		reader = XLogReaderAllocate(ctx->wal_seg_size, archive_dir,
									XL_ROUTINE(.page_read = fb_wal_read_page,
											   .segment_open = fb_wal_open_segment,
											   .segment_close = fb_wal_close_segment),
									&private);
		pfree(archive_dir);
	}
#endif
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

#if PG_VERSION_NUM < 130000
	first_record = XLogFindNextRecord(reader, window->start_lsn);
#else
	first_record = XLogFindNextRecord(reader, window->start_lsn);
#endif
	if (XLogRecPtrIsInvalid(first_record))
	{
		XLogReaderFree(reader);
		fb_wal_close_private_file(&private);
		goto done;
	}

	if (XLogRecPtrIsInvalid(ctx->first_record_lsn))
		ctx->first_record_lsn = first_record;

#if PG_VERSION_NUM >= 130000
	XLogBeginRead(reader, first_record);
#endif
	while (true)
	{
		XLogRecord *record;
		char *errormsg = NULL;

#if PG_VERSION_NUM < 130000
		record = XLogReadRecord(reader, InvalidXLogRecPtr, &errormsg);
#else
		record = XLogReadRecord(reader, &errormsg);
#endif
		if (record == NULL)
			break;

		ctx->records_scanned++;
		ctx->last_record_lsn = reader->EndRecPtr;
		if (visitor != NULL && !visitor(reader, arg))
			break;
	}

	XLogReaderFree(reader);
	fb_wal_close_private_file(&private);

done:
	ctx->resolved_segments = saved_segments;
	ctx->resolved_segment_count = saved_segment_count;
	ctx->segment_hit_map = saved_hit_map;
	ctx->segment_prefilter_used = saved_prefilter_used;
	ctx->current_segment_may_hit = saved_current_segment_may_hit;
}

static uint32
fb_wal_visit_payload_locators(FbWalScanContext *ctx,
							  const FbSummaryPayloadLocator *locators,
							  uint32 locator_count,
							  FbWalIndexBuildState *state)
{
	FbWalReaderPrivate private;
	XLogReaderState *reader;
	FbWalVisitWindow *visit_windows = NULL;
	bool *saved_hit_map;
	bool saved_prefilter_used;
	bool saved_current_segment_may_hit;
	uint32 visit_window_count = 0;
	uint32 i;

	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (state == NULL)
		elog(ERROR, "FbWalIndexBuildState must not be NULL");
	if (locators == NULL || locator_count == 0)
		return 0;

	visit_window_count =
		fb_build_payload_locator_visit_windows(ctx, locators, locator_count, &visit_windows);
	if (visit_window_count > 0 && visit_window_count < locator_count)
	{
		if (state->index != NULL &&
			fb_wal_materialize_payload_locators_parallel(state->info,
														 ctx,
														 state->index,
														 visit_windows,
														 visit_window_count,
														 locators,
														 locator_count))
		{
			if (visit_windows != NULL)
				pfree(visit_windows);
			return visit_window_count;
		}
		fb_wal_enable_incremental_payload_stats(state->index, state);
		state->payload_emit_start_lsn = InvalidXLogRecPtr;
		state->payload_emit_end_lsn = InvalidXLogRecPtr;
		state->payload_locators = locators;
		state->payload_locator_count = locator_count;
		state->payload_locator_index = 0;
		fb_wal_visit_window_batch(ctx, visit_windows, visit_window_count, state);
		if (state->payload_locator_index != locator_count)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("summary payload locator visit ended early"),
					 errdetail("Consumed %u of %u payload locators during batched visit.",
							   state->payload_locator_index,
							   locator_count)));
		state->payload_locators = NULL;
		state->payload_locator_count = 0;
		state->payload_locator_index = 0;
		if (visit_windows != NULL)
			pfree(visit_windows);
		return visit_window_count;
	}
	if (visit_windows != NULL)
		pfree(visit_windows);

	fb_wal_enable_incremental_payload_stats(state->index, state);
	saved_hit_map = ctx->segment_hit_map;
	saved_prefilter_used = ctx->segment_prefilter_used;
	saved_current_segment_may_hit = ctx->current_segment_may_hit;

	ctx->segment_hit_map = NULL;
	ctx->segment_prefilter_used = false;
	ctx->current_segment_may_hit = true;

	MemSet(&private, 0, sizeof(private));
	private.timeline_id = ctx->timeline_id;
	private.endptr = ctx->end_lsn;
	private.current_file = -1;
	private.open_pattern = FB_WAL_OPEN_SPARSE;
	private.ctx = ctx;

#if PG_VERSION_NUM < 130000
	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_wal_read_page, &private);
#else
	{
		char *archive_dir;

		archive_dir = fb_get_effective_archive_dir();
		reader = XLogReaderAllocate(ctx->wal_seg_size, archive_dir,
									XL_ROUTINE(.page_read = fb_wal_read_page,
											   .segment_open = fb_wal_open_segment,
											   .segment_close = fb_wal_close_segment),
									&private);
		pfree(archive_dir);
	}
#endif
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	for (i = 0; i < locator_count; i++)
	{
		XLogRecord *record;
		char *errormsg = NULL;

		state->payload_emit_start_lsn = locators[i].record_start_lsn;
		state->payload_emit_end_lsn = locators[i].record_start_lsn + 1;

#if PG_VERSION_NUM < 130000
		record = XLogReadRecord(reader, locators[i].record_start_lsn, &errormsg);
#else
		XLogBeginRead(reader, locators[i].record_start_lsn);
		record = XLogReadRecord(reader, &errormsg);
#endif
		if (record == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("summary payload locator read failed at %X/%08X",
							LSN_FORMAT_ARGS(locators[i].record_start_lsn)),
					 errdetail("%s", errormsg != NULL ? errormsg :
							   "WAL reader returned no record for a summary payload locator.")));
		if (reader->ReadRecPtr != locators[i].record_start_lsn)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("summary payload locator desynchronized at %X/%08X",
							LSN_FORMAT_ARGS(locators[i].record_start_lsn)),
					 errdetail("Expected record start LSN %X/%08X but reader returned %X/%08X.",
							   LSN_FORMAT_ARGS(locators[i].record_start_lsn),
							   LSN_FORMAT_ARGS(reader->ReadRecPtr))));

		ctx->records_scanned++;
		ctx->last_record_lsn = reader->EndRecPtr;
		if (XLogRecPtrIsInvalid(ctx->first_record_lsn))
			ctx->first_record_lsn = reader->ReadRecPtr;
		if (!fb_index_record_visitor(reader, state))
			break;
	}

	XLogReaderFree(reader);
	fb_wal_close_private_file(&private);
	ctx->segment_hit_map = saved_hit_map;
	ctx->segment_prefilter_used = saved_prefilter_used;
	ctx->current_segment_may_hit = saved_current_segment_may_hit;
	return locator_count;
}

static void
fb_wal_visit_window_batch(FbWalScanContext *ctx,
						  const FbWalVisitWindow *windows,
						  uint32 window_count,
						  FbWalIndexBuildState *state)
{
	FbWalReaderPrivate private;
	XLogReaderState *reader;
	void *saved_segments;
	uint32 saved_segment_count;
	bool *saved_hit_map;
	bool saved_prefilter_used;
	bool saved_current_segment_may_hit;
	FbWalVisitWindow previous_read_window;
	bool previous_read_valid = false;
	uint32 i;
	bool keep_scanning = true;

	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (state == NULL)
		elog(ERROR, "FbWalIndexBuildState must not be NULL");
	if (windows == NULL || window_count == 0)
		return;

	saved_segments = ctx->resolved_segments;
	saved_segment_count = ctx->resolved_segment_count;
	saved_hit_map = ctx->segment_hit_map;
	saved_prefilter_used = ctx->segment_prefilter_used;
	saved_current_segment_may_hit = ctx->current_segment_may_hit;

	ctx->segment_hit_map = NULL;
	ctx->segment_prefilter_used = false;
	ctx->current_segment_may_hit = true;

	MemSet(&private, 0, sizeof(private));
	private.timeline_id = ctx->timeline_id;
	private.endptr = InvalidXLogRecPtr;
	private.current_file = -1;
	private.open_pattern = FB_WAL_OPEN_SEQUENTIAL;
	private.ctx = ctx;

#if PG_VERSION_NUM < 130000
	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_wal_read_page, &private);
#else
	{
		char *archive_dir;

		archive_dir = fb_get_effective_archive_dir();
		reader = XLogReaderAllocate(ctx->wal_seg_size, archive_dir,
									XL_ROUTINE(.page_read = fb_wal_read_page,
											   .segment_open = fb_wal_open_segment,
											   .segment_close = fb_wal_close_segment),
									&private);
		pfree(archive_dir);
	}
#endif
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	for (i = 0; i < window_count && keep_scanning; i++)
	{
		const FbWalVisitWindow *window = &windows[i];
		FbWalVisitWindow read_window;
		XLogRecPtr first_record;

		if (window->segments == NULL || window->segment_count == 0 ||
			window->start_lsn >= window->end_lsn)
			continue;

		fb_wal_prepare_payload_read_window(ctx,
										   window,
										   (FbWalSegmentEntry *) saved_segments,
										   saved_segment_count,
										   &read_window);

		if (!previous_read_valid ||
			!fb_wal_visit_windows_share_segment_slice(&previous_read_window,
													 &read_window))
		{
			ctx->resolved_segments = read_window.segments;
			ctx->resolved_segment_count = read_window.segment_count;
			fb_wal_close_private_file(&private);
			private.last_open_segno_valid = false;
			private.last_open_entry = NULL;
			private.current_file = -1;
			private.current_file_segno_valid = false;
			ctx->payload_sparse_reader_resets++;
		}
		else
			ctx->payload_sparse_reader_reuses++;
		state->payload_emit_start_lsn = window->start_lsn;
		state->payload_emit_end_lsn = window->end_lsn;
		private.endptr = read_window.read_end_lsn;
		private.endptr_reached = false;

		first_record = XLogFindNextRecord(reader, read_window.start_lsn);
		if (XLogRecPtrIsInvalid(first_record) || first_record >= read_window.read_end_lsn)
		{
			previous_read_window = read_window;
			previous_read_valid = true;
			continue;
		}

		if (XLogRecPtrIsInvalid(ctx->first_record_lsn))
			ctx->first_record_lsn = first_record;

#if PG_VERSION_NUM >= 130000
		XLogBeginRead(reader, first_record);
#endif
		while (keep_scanning)
		{
			XLogRecord *record;
			char *errormsg = NULL;

#if PG_VERSION_NUM < 130000
			record = XLogReadRecord(reader, InvalidXLogRecPtr, &errormsg);
#else
			record = XLogReadRecord(reader, &errormsg);
#endif
			if (record == NULL)
				break;

			ctx->records_scanned++;
			ctx->last_record_lsn = reader->EndRecPtr;
			if (!fb_index_record_visitor(reader, state))
				keep_scanning = false;
		}

		previous_read_window = read_window;
		previous_read_valid = true;
	}

	XLogReaderFree(reader);
	fb_wal_close_private_file(&private);
	ctx->resolved_segments = saved_segments;
	ctx->resolved_segment_count = saved_segment_count;
	ctx->segment_hit_map = saved_hit_map;
	ctx->segment_prefilter_used = saved_prefilter_used;
	ctx->current_segment_may_hit = saved_current_segment_may_hit;
}

static void
fb_wal_visit_sparse_windows(FbWalScanContext *ctx,
							const FbWalVisitWindow *windows,
							uint32 window_count,
							FbWalIndexBuildState *state)
{
	fb_wal_visit_window_batch(ctx, windows, window_count, state);
}

void
fb_wal_payload_worker_main(Datum main_arg)
{
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg;
	FbWalPayloadShared *shared;
	FbWalPayloadTask *task;
	FbWalParallelWindow *windows;
	FbWalSegmentEntry *segments;
	FbSummaryPayloadLocator *locators;
	FbRelationInfo info;
	FbWalScanContext ctx;
	FbWalRecordIndex index;
	FbWalIndexBuildState state;
	FbSpoolLog *log = NULL;
	FbWalVisitWindow *task_windows = NULL;
	uint32 task_window_count = 0;
	int task_index;
	int i;

	memcpy(&task_index, MyBgworkerEntry->bgw_extra, sizeof(task_index));

	BackgroundWorkerUnblockSignals();
	seg = dsm_attach(handle);
	if (seg == NULL)
		proc_exit(1);

	shared = (FbWalPayloadShared *) dsm_segment_address(seg);
	task = &fb_wal_payload_tasks(shared)[task_index];
	windows = fb_wal_payload_windows(shared);
	segments = fb_wal_payload_segments(shared);
	locators = fb_wal_payload_locators_shared(shared);
	task->status = FB_WAL_PAYLOAD_TASK_RUNNING;

	fb_wal_payload_apply_gucs(task);

	PG_TRY();
	{
		info = task->info;
		info.mode_name = (info.mode == FB_APPLY_KEYED) ? "keyed" : "bag";
		MemSet(&ctx, 0, sizeof(ctx));
		ctx.target_ts = task->target_ts;
		ctx.query_now_ts = task->target_ts;
		ctx.timeline_id = shared->timeline_id;
		ctx.wal_seg_size = shared->wal_seg_size;
		ctx.resolved_segments = segments;
		ctx.resolved_segment_count = shared->resolved_segment_count;
		ctx.current_segment_may_hit = true;

		MemSet(&index, 0, sizeof(index));
		index.anchor_redo_lsn = task->anchor_redo_lsn;
		log = fb_spool_log_create_path(task->spool_path);

		MemSet(&state, 0, sizeof(state));
		state.info = &info;
		state.ctx = &ctx;
		state.index = &index;
		state.collect_metadata = false;
		state.capture_payload = true;
		state.tail_capture_allowed = false;
		state.payload_log = &log;
		state.payload_label = "wal-records-worker";
		if (task->locator_count > 0)
		{
			state.payload_emit_start_lsn = InvalidXLogRecPtr;
			state.payload_emit_end_lsn = InvalidXLogRecPtr;
			state.payload_locators = locators + task->locator_start;
			state.payload_locator_count = task->locator_count;
			state.payload_locator_index = 0;
		}

		task_window_count = (uint32) (task->window_end - task->window_start);
		task_windows = palloc0(sizeof(*task_windows) * task_window_count);
		for (i = task->window_start; i < task->window_end; i++)
		{
			FbWalParallelWindow *shared_window = &windows[i];
			FbWalVisitWindow *window = &task_windows[i - task->window_start];

			if (shared_window->segment_start_index + shared_window->segment_count >
				ctx.resolved_segment_count)
				elog(ERROR, "payload worker window segment range is out of bounds");

			window->segments = &segments[shared_window->segment_start_index];
			window->segment_count = shared_window->segment_count;
			window->start_lsn = shared_window->start_lsn;
			window->end_lsn = shared_window->end_lsn;
			window->read_end_lsn = shared_window->read_end_lsn;
		}
		fb_wal_visit_window_batch(&ctx,
								  task_windows,
								  task_window_count,
								  &state);
		if (task->locator_count > 0 &&
			state.payload_locator_index != task->locator_count)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("payload locator worker visit ended early"),
					 errdetail("Consumed %u of %u payload locators in worker %d.",
							   state.payload_locator_index,
							   task->locator_count,
							   task_index + 1)));

		task->record_count = index.record_count;
		task->scanned_record_count = index.payload_scanned_record_count;
		task->reader_reset_count = ctx.payload_sparse_reader_resets;
		task->reader_reuse_count = ctx.payload_sparse_reader_reuses;
		fb_spool_log_close(log);
		log = NULL;
		if (task_windows != NULL)
		{
			pfree(task_windows);
			task_windows = NULL;
		}

		task->status = FB_WAL_PAYLOAD_TASK_DONE;
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();

		FlushErrorState();
		if (log != NULL)
			fb_spool_log_close(log);
		if (task_windows != NULL)
			pfree(task_windows);
		strlcpy(task->errmsg,
				edata->message != NULL ? edata->message : "payload worker failed",
				sizeof(task->errmsg));
		task->status = FB_WAL_PAYLOAD_TASK_ERROR;
	}
	PG_END_TRY();

	dsm_detach(seg);
	proc_exit(0);
}

static bool
fb_wal_payload_launch_worker(dsm_handle handle,
							 int task_index,
							 BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker worker;
	pid_t pid;
	BgwHandleStatus status;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback wal payload %d", task_index + 1);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback wal payload");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_wal_payload_worker_main");
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
fb_wal_payload_wait_worker(BackgroundWorkerHandle *handle,
						   const FbWalPayloadTask *task)
{
	BgwHandleStatus status;

	status = WaitForBackgroundWorkerShutdown(handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("postmaster died while waiting for pg_flashback WAL payload worker")));
	if (task->status != FB_WAL_PAYLOAD_TASK_DONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_flashback WAL payload worker failed"),
				 errdetail_internal("%s",
									task->errmsg[0] != '\0' ? task->errmsg : "worker exited without success")));
}

void
fb_wal_metadata_collect_worker_main(Datum main_arg)
{
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg;
	FbWalMetadataCollectShared *shared;
	FbWalMetadataCollectTask *task;
	FbWalParallelWindow *windows;
	FbWalSegmentEntry *segments;
	FbRelationInfo info;
	FbWalScanContext ctx;
	FbWalRecordIndex index;
	FbWalIndexBuildState state;
	int task_index;
	int i;

	memcpy(&task_index, MyBgworkerEntry->bgw_extra, sizeof(task_index));

	BackgroundWorkerUnblockSignals();
	seg = dsm_attach(handle);
	if (seg == NULL)
		proc_exit(1);

	shared = (FbWalMetadataCollectShared *) dsm_segment_address(seg);
	task = &fb_wal_metadata_collect_tasks(shared)[task_index];
	windows = fb_wal_metadata_collect_windows(shared);
	segments = fb_wal_metadata_collect_segments(shared);
	task->status = FB_WAL_PAYLOAD_TASK_RUNNING;

	BackgroundWorkerInitializeConnectionByOid(task->dboid, task->useroid, 0);
	fb_wal_metadata_collect_apply_gucs(task);

	PG_TRY();
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		fb_catalog_load_relation_info(task->relid, &info);
		MemSet(&ctx, 0, sizeof(ctx));
		ctx.target_ts = task->target_ts;
		ctx.query_now_ts = task->query_now_ts;
		ctx.timeline_id = shared->timeline_id;
		ctx.wal_seg_size = shared->wal_seg_size;
		ctx.resolved_segments = segments;
		ctx.resolved_segment_count = shared->resolved_segment_count;
		ctx.current_segment_may_hit = true;

		MemSet(&index, 0, sizeof(index));

		MemSet(&state, 0, sizeof(state));
		state.info = &info;
		state.ctx = &ctx;
		state.index = &index;
		state.touched_xids = fb_create_touched_xid_hash();
		state.unsafe_xids = fb_create_unsafe_xid_hash();
		state.collect_metadata = true;
		state.collect_xact_statuses = false;
		state.capture_payload = false;
		state.tail_capture_allowed = false;
		state.xact_summary_log = NULL;
		state.xact_summary_label = NULL;

		for (i = task->window_start; i < task->window_end; i++)
		{
			FbWalVisitWindow window;
			FbWalParallelWindow *shared_window = &windows[i];

			window.segments = &segments[shared_window->segment_start_index];
			window.segment_count = shared_window->segment_count;
			window.start_lsn = shared_window->start_lsn;
			window.end_lsn = shared_window->end_lsn;
			window.read_end_lsn = shared_window->read_end_lsn;
			fb_wal_visit_window(&ctx, &window, fb_index_record_visitor, &state);
			if (ctx.unsafe && !TransactionIdIsValid(ctx.unsafe_xid))
				break;
		}

		fb_wal_metadata_dump_touched_xids(state.touched_xids,
										 task->touched_path,
										 &task->touched_count);
		fb_wal_metadata_dump_unsafe_xids(state.unsafe_xids,
										task->unsafe_path,
										&task->unsafe_count);
		task->xact_summary_count = 0;
		task->total_record_count = index.total_record_count;
		task->anchor_found = ctx.anchor_found;
		task->anchor_checkpoint_lsn = ctx.anchor_checkpoint_lsn;
		task->anchor_redo_lsn = ctx.anchor_redo_lsn;
		task->anchor_time = ctx.anchor_time;
		if (ctx.unsafe && !TransactionIdIsValid(ctx.unsafe_xid))
		{
			task->immediate_unsafe_found = true;
			task->immediate_unsafe_reason = ctx.unsafe_reason;
			task->immediate_unsafe_scope = ctx.unsafe_scope;
			task->immediate_unsafe_storage_op = ctx.unsafe_storage_op;
			task->immediate_unsafe_record_lsn = ctx.unsafe_record_lsn;
			task->immediate_unsafe_trigger_lsn = ctx.unsafe_record_lsn;
		}

		hash_destroy(state.touched_xids);
		hash_destroy(state.unsafe_xids);

		PopActiveSnapshot();
		CommitTransactionCommand();
		task->status = FB_WAL_PAYLOAD_TASK_DONE;
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();

		FlushErrorState();
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		if (IsTransactionState())
			AbortCurrentTransaction();
		strlcpy(task->errmsg,
				edata->message != NULL ? edata->message : "metadata collect worker failed",
				sizeof(task->errmsg));
		task->status = FB_WAL_PAYLOAD_TASK_ERROR;
	}
	PG_END_TRY();

	dsm_detach(seg);
	proc_exit(0);
}

static void
fb_wal_metadata_collect_launch_worker(dsm_handle handle,
									  int task_index,
									  BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker worker;
	pid_t pid;
	BgwHandleStatus status;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback wal metadata %d", task_index + 1);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback wal metadata");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_wal_metadata_collect_worker_main");
	worker.bgw_main_arg = UInt32GetDatum(handle);
	memcpy(worker.bgw_extra, &task_index, sizeof(task_index));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, handle_out))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register pg_flashback WAL metadata worker")));

	status = WaitForBackgroundWorkerStartup(*handle_out, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("pg_flashback WAL metadata worker failed to start")));
}

static void
fb_wal_metadata_collect_wait_worker(BackgroundWorkerHandle *handle,
									const FbWalMetadataCollectTask *task)
{
	BgwHandleStatus status;

	status = WaitForBackgroundWorkerShutdown(handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("postmaster died while waiting for pg_flashback WAL metadata worker")));
	if (task->status != FB_WAL_PAYLOAD_TASK_DONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_flashback WAL metadata worker failed"),
				 errdetail_internal("%s",
									task->errmsg[0] != '\0' ? task->errmsg : "worker exited without success")));
}

void
fb_wal_xact_fill_worker_main(Datum main_arg)
{
	dsm_handle handle = DatumGetUInt32(main_arg);
	dsm_segment *seg;
	FbWalXactFillShared *shared;
	FbWalXactFillTask *task;
	FbWalParallelWindow *windows;
	FbWalSegmentEntry *segments;
	FbWalXactScanVisitorState state;
	FbWalScanContext ctx;
	FbSpoolLog *status_log = NULL;
	int task_index;
	int i;

	memcpy(&task_index, MyBgworkerEntry->bgw_extra, sizeof(task_index));

	BackgroundWorkerUnblockSignals();
	seg = dsm_attach(handle);
	if (seg == NULL)
		proc_exit(1);

	shared = (FbWalXactFillShared *) dsm_segment_address(seg);
	task = &fb_wal_xact_fill_tasks(shared)[task_index];
	windows = fb_wal_xact_fill_windows(shared);
	segments = fb_wal_xact_fill_segments(shared);
	task->status = FB_WAL_PAYLOAD_TASK_RUNNING;

	BackgroundWorkerInitializeConnectionByOid(task->dboid, task->useroid, 0);
	fb_wal_xact_fill_apply_gucs(task);

	PG_TRY();
	{
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		MemSet(&ctx, 0, sizeof(ctx));
		ctx.target_ts = task->target_ts;
		ctx.query_now_ts = task->query_now_ts;
		ctx.timeline_id = shared->timeline_id;
		ctx.wal_seg_size = shared->wal_seg_size;
		ctx.resolved_segments = segments;
		ctx.resolved_segment_count = shared->resolved_segment_count;
		ctx.current_segment_may_hit = true;

		status_log = fb_spool_log_create_path(task->status_path);
		MemSet(&state, 0, sizeof(state));
		state.touched_xids = fb_wal_xact_fill_touched_xids(shared);
		state.touched_count = shared->touched_xid_count;
		state.unsafe_entries = fb_wal_xact_fill_unsafe_entries(shared);
		state.unsafe_count = shared->unsafe_count;
		state.status_log = status_log;
		state.task = task;

		for (i = task->window_start; i < task->window_end; i++)
		{
			FbWalVisitWindow window;
			FbWalParallelWindow *shared_window = &windows[i];

			window.segments = &segments[shared_window->segment_start_index];
			window.segment_count = shared_window->segment_count;
			window.start_lsn = shared_window->start_lsn;
			window.end_lsn = shared_window->end_lsn;
			window.read_end_lsn = shared_window->read_end_lsn;
			fb_wal_visit_window(&ctx, &window, fb_wal_xact_fill_visitor, &state);
		}

		fb_spool_log_close(status_log);
		status_log = NULL;

		PopActiveSnapshot();
		CommitTransactionCommand();
		task->status = FB_WAL_PAYLOAD_TASK_DONE;
	}
	PG_CATCH();
	{
		ErrorData *edata = CopyErrorData();

		FlushErrorState();
		if (status_log != NULL)
			fb_spool_log_close(status_log);
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		if (IsTransactionState())
			AbortCurrentTransaction();
		strlcpy(task->errmsg,
				edata->message != NULL ? edata->message : "xact fill worker failed",
				sizeof(task->errmsg));
		task->status = FB_WAL_PAYLOAD_TASK_ERROR;
	}
	PG_END_TRY();

	dsm_detach(seg);
	proc_exit(0);
}

static bool
fb_wal_xact_fill_launch_worker(dsm_handle handle,
							   int task_index,
							   BackgroundWorkerHandle **handle_out)
{
	BackgroundWorker worker;
	pid_t pid;
	BgwHandleStatus status;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_flashback wal xact %d", task_index + 1);
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_flashback wal xact");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "pg_flashback");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "fb_wal_xact_fill_worker_main");
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
fb_wal_xact_fill_wait_worker(BackgroundWorkerHandle *handle,
							 const FbWalXactFillTask *task)
{
	BgwHandleStatus status;

	status = WaitForBackgroundWorkerShutdown(handle);
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("postmaster died while waiting for pg_flashback WAL xact worker")));
	if (task->status != FB_WAL_PAYLOAD_TASK_DONE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_flashback WAL xact worker failed"),
				 errdetail_internal("%s",
									task->errmsg[0] != '\0' ? task->errmsg : "worker exited without success")));
}

static void
fb_wal_payload_merge_log(FbWalRecordIndex *index,
						 const char *path,
						 uint32 item_count)
{
	FbSpoolLog *dest;

	if (index == NULL || path == NULL || item_count == 0)
		return;

	dest = fb_index_ensure_record_log(index, &index->record_log, "wal-records");
	fb_spool_log_append_file(dest, path, item_count);
	index->record_count += item_count;
	unlink(path);
}

static bool
fb_wal_collect_metadata_parallel(const FbRelationInfo *info,
								 FbWalScanContext *ctx,
								 FbWalRecordIndex *index,
								 FbWalIndexBuildState *state,
								 const FbWalVisitWindow *windows,
								 uint32 window_count)
{
	/*
	 * Metadata parallel fanout is currently slower than the original serial
	 * metadata pass on the main live case because it adds one full extra
	 * `RM_XACT_ID` fill pass on top of worker startup and merge overhead.
	 * Keep it disabled until we have a design that beats the old 3/9 baseline.
	 */
	return false;

	const char *session_dir;
	Size tasks_size;
	Size windows_size;
	Size segments_size;
	Size shared_size;
	dsm_segment *seg;
	dsm_handle handle;
	FbWalMetadataCollectShared *shared;
	FbWalMetadataCollectTask *tasks;
	FbWalParallelWindow *shared_windows;
	BackgroundWorkerHandle *worker_handles[FB_WAL_PARALLEL_MAX_WORKERS];
	int worker_count;
	int chunk_size;
	int i;

	if (info == NULL || ctx == NULL || index == NULL || state == NULL ||
		windows == NULL || window_count <= 1 || ctx->parallel_workers <= 1 ||
		index->spool_session == NULL)
		return false;

	worker_count = fb_parallel_worker_count((int) window_count);
	if (worker_count <= 1)
		return false;

	session_dir = fb_spool_session_dir(index->spool_session);
	if (session_dir == NULL)
		return false;

	tasks_size = MAXALIGN(sizeof(FbWalMetadataCollectTask) * worker_count);
	windows_size = MAXALIGN(sizeof(FbWalParallelWindow) * window_count);
	segments_size = MAXALIGN(sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);
	shared_size = MAXALIGN(sizeof(FbWalMetadataCollectShared)) +
		tasks_size + windows_size + segments_size;
	seg = dsm_create(shared_size, 0);
	handle = dsm_segment_handle(seg);
	shared = (FbWalMetadataCollectShared *) dsm_segment_address(seg);
	MemSet(shared, 0, shared_size);
	shared->task_count = worker_count;
	shared->window_count = (int) window_count;
	shared->timeline_id = ctx->timeline_id;
	shared->wal_seg_size = ctx->wal_seg_size;
	shared->resolved_segment_count = ctx->resolved_segment_count;
	shared->tasks_offset = MAXALIGN(sizeof(FbWalMetadataCollectShared));
	shared->windows_offset = shared->tasks_offset + tasks_size;
	shared->segments_offset = shared->windows_offset + windows_size;
	tasks = fb_wal_metadata_collect_tasks(shared);
	shared_windows = fb_wal_metadata_collect_windows(shared);
	memcpy(fb_wal_metadata_collect_segments(shared),
		   ctx->resolved_segments,
		   sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);

	for (i = 0; i < (int) window_count; i++)
	{
		const FbWalVisitWindow *window = &windows[i];
		FbWalSegmentEntry *base_segments = (FbWalSegmentEntry *) ctx->resolved_segments;

		shared_windows[i].segment_start_index = (uint32) (window->segments - base_segments);
		shared_windows[i].segment_count = window->segment_count;
		shared_windows[i].start_lsn = window->start_lsn;
		shared_windows[i].end_lsn = window->end_lsn;
		shared_windows[i].read_end_lsn = window->read_end_lsn;
	}

	chunk_size = ((int) window_count + worker_count - 1) / worker_count;
	for (i = 0; i < worker_count; i++)
	{
		char touched_path[MAXPGPATH];
		char unsafe_path[MAXPGPATH];
		char xact_summary_path[MAXPGPATH];
		int window_start = i * chunk_size;
		int window_end = Min(window_start + chunk_size, (int) window_count);

		if (window_start >= window_end)
			break;

		snprintf(touched_path, sizeof(touched_path),
				 "%s/wal-metadata-touched-%d.bin",
				 session_dir, i + 1);
		snprintf(unsafe_path, sizeof(unsafe_path),
				 "%s/wal-metadata-unsafe-%d.bin",
				 session_dir, i + 1);
		snprintf(xact_summary_path, sizeof(xact_summary_path),
				 "%s/wal-metadata-xact-%d.bin",
				 session_dir, i + 1);
		fb_wal_metadata_collect_fill_task(&tasks[i],
										  info,
										  ctx->target_ts,
										  ctx->query_now_ts,
										  window_start,
										  window_end,
										  touched_path,
										  unsafe_path,
										  xact_summary_path);
		fb_wal_metadata_collect_launch_worker(handle, i, &worker_handles[i]);
	}
	worker_count = i;

	for (i = 0; i < worker_count; i++)
		fb_wal_metadata_collect_wait_worker(worker_handles[i], &tasks[i]);

	for (i = 0; i < worker_count; i++)
	{
		index->total_record_count += tasks[i].total_record_count;
		if (tasks[i].anchor_found &&
			(!ctx->anchor_found ||
			 tasks[i].anchor_time > ctx->anchor_time ||
			 (tasks[i].anchor_time == ctx->anchor_time &&
			  tasks[i].anchor_redo_lsn >= ctx->anchor_redo_lsn)))
		{
			ctx->anchor_found = true;
			ctx->anchor_checkpoint_lsn = tasks[i].anchor_checkpoint_lsn;
			ctx->anchor_redo_lsn = tasks[i].anchor_redo_lsn;
			ctx->anchor_time = tasks[i].anchor_time;
		}
		fb_wal_metadata_merge_touched_xids(ctx,
										   state->touched_xids,
										   tasks[i].touched_path,
										   tasks[i].touched_count);
		fb_wal_metadata_merge_unsafe_xids(ctx,
										  state->unsafe_xids,
										  tasks[i].unsafe_path,
										  tasks[i].unsafe_count);
		if (tasks[i].immediate_unsafe_found && !ctx->unsafe)
		{
			ctx->unsafe = false;
			ctx->unsafe_reason = tasks[i].immediate_unsafe_reason;
			ctx->unsafe_xid = InvalidTransactionId;
			ctx->unsafe_commit_ts = 0;
			ctx->unsafe_record_lsn = tasks[i].immediate_unsafe_record_lsn;
			ctx->unsafe_scope = tasks[i].immediate_unsafe_scope;
			ctx->unsafe_storage_op = tasks[i].immediate_unsafe_storage_op;
			fb_mark_unsafe(ctx, tasks[i].immediate_unsafe_reason);
		}
	}

	dsm_detach(seg);
	return true;
}

static bool
fb_wal_fill_xact_statuses_parallel(const FbRelationInfo *info,
								   FbWalScanContext *ctx,
								   FbWalRecordIndex *index,
								   const FbWalVisitWindow *windows,
								   uint32 window_count,
								   HTAB *touched_xids,
								   HTAB *unsafe_xids)
{
	const char *session_dir;
	long touched_entries;
	long unsafe_entries;
	TransactionId *touched_array;
	FbUnsafeXidEntry *unsafe_array;
	HASH_SEQ_STATUS status;
	FbTouchedXidEntry *touched_entry;
	FbUnsafeXidEntry *unsafe_entry;
	Size tasks_size;
	Size windows_size;
	Size segments_size;
	Size touched_size;
	Size unsafe_size;
	Size shared_size;
	dsm_segment *seg;
	dsm_handle handle;
	FbWalXactFillShared *shared;
	FbWalXactFillTask *tasks;
	FbWalParallelWindow *shared_windows;
	BackgroundWorkerHandle *worker_handles[FB_WAL_PARALLEL_MAX_WORKERS];
	int worker_count;
	int chunk_size;
	long index_pos;
	int i;

	return false;

	if (info == NULL || ctx == NULL || index == NULL || windows == NULL ||
		window_count <= 1 || ctx->parallel_workers <= 1 || touched_xids == NULL ||
		unsafe_xids == NULL || index->spool_session == NULL)
		return false;

	touched_entries = hash_get_num_entries(touched_xids);
	if (touched_entries <= 0)
		return true;

	worker_count = fb_parallel_worker_count((int) window_count);
	if (worker_count <= 1)
		return false;

	session_dir = fb_spool_session_dir(index->spool_session);
	if (session_dir == NULL)
		return false;

	touched_array = palloc(sizeof(TransactionId) * touched_entries);
	index_pos = 0;
	hash_seq_init(&status, touched_xids);
	while ((touched_entry = (FbTouchedXidEntry *) hash_seq_search(&status)) != NULL)
		touched_array[index_pos++] = touched_entry->xid;
	qsort(touched_array, touched_entries, sizeof(TransactionId), fb_transactionid_cmp);

	unsafe_entries = hash_get_num_entries(unsafe_xids);
	unsafe_array = NULL;
	if (unsafe_entries > 0)
	{
		unsafe_array = palloc(sizeof(FbUnsafeXidEntry) * unsafe_entries);
		index_pos = 0;
		hash_seq_init(&status, unsafe_xids);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&status)) != NULL)
			unsafe_array[index_pos++] = *unsafe_entry;
		qsort(unsafe_array, unsafe_entries, sizeof(FbUnsafeXidEntry), fb_unsafe_xid_entry_cmp);
	}

	tasks_size = MAXALIGN(sizeof(FbWalXactFillTask) * worker_count);
	windows_size = MAXALIGN(sizeof(FbWalParallelWindow) * window_count);
	segments_size = MAXALIGN(sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);
	touched_size = MAXALIGN(sizeof(TransactionId) * touched_entries);
	unsafe_size = MAXALIGN(sizeof(FbUnsafeXidEntry) * Max(unsafe_entries, 1L));
	shared_size = MAXALIGN(sizeof(FbWalXactFillShared)) +
		tasks_size + windows_size + segments_size + touched_size + unsafe_size;
	seg = dsm_create(shared_size, 0);
	handle = dsm_segment_handle(seg);
	shared = (FbWalXactFillShared *) dsm_segment_address(seg);
	MemSet(shared, 0, shared_size);
	shared->task_count = worker_count;
	shared->window_count = (int) window_count;
	shared->timeline_id = ctx->timeline_id;
	shared->wal_seg_size = ctx->wal_seg_size;
	shared->resolved_segment_count = ctx->resolved_segment_count;
	shared->touched_xid_count = (uint32) touched_entries;
	shared->unsafe_count = (uint32) unsafe_entries;
	shared->tasks_offset = MAXALIGN(sizeof(FbWalXactFillShared));
	shared->windows_offset = shared->tasks_offset + tasks_size;
	shared->segments_offset = shared->windows_offset + windows_size;
	shared->touched_xids_offset = shared->segments_offset + segments_size;
	shared->unsafe_entries_offset = shared->touched_xids_offset + touched_size;
	tasks = fb_wal_xact_fill_tasks(shared);
	shared_windows = fb_wal_xact_fill_windows(shared);
	memcpy(fb_wal_xact_fill_segments(shared),
		   ctx->resolved_segments,
		   sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);
	memcpy(fb_wal_xact_fill_touched_xids(shared),
		   touched_array,
		   sizeof(TransactionId) * touched_entries);
	if (unsafe_entries > 0)
		memcpy(fb_wal_xact_fill_unsafe_entries(shared),
			   unsafe_array,
			   sizeof(FbUnsafeXidEntry) * unsafe_entries);

	for (i = 0; i < (int) window_count; i++)
	{
		const FbWalVisitWindow *window = &windows[i];
		FbWalSegmentEntry *base_segments = (FbWalSegmentEntry *) ctx->resolved_segments;

		shared_windows[i].segment_start_index = (uint32) (window->segments - base_segments);
		shared_windows[i].segment_count = window->segment_count;
		shared_windows[i].start_lsn = window->start_lsn;
		shared_windows[i].end_lsn = window->end_lsn;
		shared_windows[i].read_end_lsn = window->read_end_lsn;
	}

	chunk_size = ((int) window_count + worker_count - 1) / worker_count;
	for (i = 0; i < worker_count; i++)
	{
		char status_path[MAXPGPATH];
		int window_start = i * chunk_size;
		int window_end = Min(window_start + chunk_size, (int) window_count);

		if (window_start >= window_end)
			break;

		snprintf(status_path, sizeof(status_path),
				 "%s/wal-xact-status-%d.bin",
				 session_dir, i + 1);
		fb_wal_xact_fill_fill_task(&tasks[i],
								   info,
								   ctx->target_ts,
								   ctx->query_now_ts,
								   window_start,
								   window_end,
								   status_path);
		if (!fb_wal_xact_fill_launch_worker(handle, i, &worker_handles[i]))
			break;
	}
	worker_count = i;
	if (worker_count <= 1)
	{
		for (i = 0; i < worker_count; i++)
		{
			TerminateBackgroundWorker(worker_handles[i]);
			(void) WaitForBackgroundWorkerShutdown(worker_handles[i]);
			if (tasks[i].status_path[0] != '\0')
				unlink(tasks[i].status_path);
		}
		dsm_detach(seg);
		if (unsafe_array != NULL)
			pfree(unsafe_array);
		pfree(touched_array);
		return false;
	}

	for (i = 0; i < worker_count; i++)
		fb_wal_xact_fill_wait_worker(worker_handles[i], &tasks[i]);

	for (i = 0; i < worker_count; i++)
	{
		index->target_commit_count += tasks[i].target_commit_count;
		index->target_abort_count += tasks[i].target_abort_count;
		fb_wal_xact_merge_statuses(index, tasks[i].status_path, tasks[i].status_count);
		if (tasks[i].unsafe_found && !ctx->unsafe)
		{
			ctx->unsafe = false;
			ctx->unsafe_reason = tasks[i].unsafe_reason;
			ctx->unsafe_xid = tasks[i].unsafe_xid;
			ctx->unsafe_commit_ts = tasks[i].unsafe_commit_ts;
			ctx->unsafe_record_lsn = tasks[i].unsafe_record_lsn;
			ctx->unsafe_scope = tasks[i].unsafe_scope;
			ctx->unsafe_storage_op = tasks[i].unsafe_storage_op;
			fb_mark_unsafe(ctx, tasks[i].unsafe_reason);
		}
	}

	pfree(touched_array);
	if (unsafe_array != NULL)
		pfree(unsafe_array);
	dsm_detach(seg);
	return true;
}

static bool
fb_wal_materialize_payload_parallel(const FbRelationInfo *info,
									FbWalScanContext *ctx,
									FbWalRecordIndex *index,
									const FbWalVisitWindow *windows,
									uint32 window_count)
{
	const char *session_dir;
	Size tasks_size;
	Size windows_size;
	Size segments_size;
	Size shared_size;
	dsm_segment *seg;
	dsm_handle handle;
	FbWalPayloadShared *shared;
	FbWalPayloadTask *tasks;
	FbWalParallelWindow *shared_windows;
	BackgroundWorkerHandle *worker_handles[FB_WAL_PARALLEL_MAX_WORKERS];
	int desired_workers;
	int chunk_size;
	int i;

	if (info == NULL || ctx == NULL || index == NULL || windows == NULL ||
		window_count <= 1 || ctx->parallel_workers <= 1 || index->spool_session == NULL)
		return false;

	desired_workers = fb_parallel_worker_count((int) window_count);
	if (desired_workers <= 1)
		return false;

	session_dir = fb_spool_session_dir(index->spool_session);
	if (session_dir == NULL)
		return false;

	for (; desired_workers > 1; desired_workers--)
	{
		int launched = 0;

		tasks_size = MAXALIGN(sizeof(FbWalPayloadTask) * desired_workers);
		windows_size = MAXALIGN(sizeof(FbWalParallelWindow) * window_count);
		segments_size = MAXALIGN(sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);
		shared_size = MAXALIGN(sizeof(FbWalPayloadShared)) + tasks_size + windows_size + segments_size;
		seg = dsm_create(shared_size, 0);
		handle = dsm_segment_handle(seg);
		shared = (FbWalPayloadShared *) dsm_segment_address(seg);
		MemSet(shared, 0, shared_size);
		shared->task_count = desired_workers;
		shared->window_count = (int) window_count;
		shared->timeline_id = ctx->timeline_id;
		shared->wal_seg_size = ctx->wal_seg_size;
		shared->resolved_segment_count = ctx->resolved_segment_count;
		shared->tasks_offset = MAXALIGN(sizeof(FbWalPayloadShared));
		shared->windows_offset = shared->tasks_offset + tasks_size;
		shared->segments_offset = shared->windows_offset + windows_size;
		tasks = fb_wal_payload_tasks(shared);
		shared_windows = fb_wal_payload_windows(shared);
		memcpy(fb_wal_payload_segments(shared),
			   ctx->resolved_segments,
			   sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);

		for (i = 0; i < (int) window_count; i++)
		{
			const FbWalVisitWindow *window = &windows[i];
			FbWalSegmentEntry *base_segments = (FbWalSegmentEntry *) ctx->resolved_segments;

			shared_windows[i].segment_start_index = (uint32) (window->segments - base_segments);
			shared_windows[i].segment_count = window->segment_count;
			shared_windows[i].start_lsn = window->start_lsn;
			shared_windows[i].end_lsn = window->end_lsn;
			shared_windows[i].read_end_lsn = window->read_end_lsn;
		}

		chunk_size = ((int) window_count + desired_workers - 1) / desired_workers;
		for (i = 0; i < desired_workers; i++)
		{
			char spool_path[MAXPGPATH];
			int window_start = i * chunk_size;
			int window_end = Min(window_start + chunk_size, (int) window_count);

			if (window_start >= window_end)
				break;

			snprintf(spool_path, sizeof(spool_path), "%s/wal-payload-worker-%d.bin",
					 session_dir, i + 1);
			fb_wal_payload_fill_task(&tasks[i],
									 info,
									 ctx->target_ts,
									 index->anchor_redo_lsn,
									 window_start,
									 window_end,
									 0,
									 0,
									 spool_path);
			if (!fb_wal_payload_launch_worker(handle, i, &worker_handles[i]))
				break;
			launched++;
		}

		if (launched != desired_workers)
		{
			for (i = 0; i < launched; i++)
				TerminateBackgroundWorker(worker_handles[i]);
			for (i = 0; i < launched; i++)
			{
				BgwHandleStatus status = WaitForBackgroundWorkerShutdown(worker_handles[i]);

				if (status == BGWH_POSTMASTER_DIED)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("postmaster died while stopping pg_flashback WAL payload workers")));
				if (tasks[i].spool_path[0] != '\0')
					unlink(tasks[i].spool_path);
			}
			dsm_detach(seg);
			continue;
		}

		for (i = 0; i < desired_workers; i++)
			fb_wal_payload_wait_worker(worker_handles[i], &tasks[i]);
		for (i = 0; i < desired_workers; i++)
		{
			fb_wal_payload_merge_log(index, tasks[i].spool_path, tasks[i].record_count);
			index->payload_scanned_record_count += tasks[i].scanned_record_count;
			index->payload_sparse_reader_resets += tasks[i].reader_reset_count;
			index->payload_sparse_reader_reuses += tasks[i].reader_reuse_count;
		}
		index->payload_window_count = window_count;
		index->payload_parallel_workers = desired_workers;
		dsm_detach(seg);
		return true;
	}

	return false;
}

static bool
fb_wal_materialize_payload_locators_parallel(const FbRelationInfo *info,
											 FbWalScanContext *ctx,
											 FbWalRecordIndex *index,
											 const FbWalVisitWindow *windows,
											 uint32 window_count,
											 const FbSummaryPayloadLocator *locators,
											 uint32 locator_count)
{
	const char *session_dir;
	uint32 *window_locator_offsets = NULL;
	Size tasks_size;
	Size windows_size;
	Size segments_size;
	Size locators_size;
	Size shared_size;
	dsm_segment *seg;
	dsm_handle handle;
	FbWalPayloadShared *shared;
	FbWalPayloadTask *tasks;
	FbWalParallelWindow *shared_windows;
	BackgroundWorkerHandle *worker_handles[FB_WAL_PARALLEL_MAX_WORKERS];
	int desired_workers;
	int chunk_size;
	uint32 locator_index = 0;
	int i;

	if (info == NULL || ctx == NULL || index == NULL || windows == NULL ||
		window_count <= 1 || locator_count == 0 || locators == NULL ||
		ctx->parallel_workers <= 1 || index->spool_session == NULL)
		return false;

	desired_workers = fb_parallel_worker_count((int) window_count);
	if (desired_workers <= 1)
		return false;

	session_dir = fb_spool_session_dir(index->spool_session);
	if (session_dir == NULL)
		return false;

	window_locator_offsets = palloc0(sizeof(*window_locator_offsets) * (window_count + 1));
	for (i = 0; i < (int) window_count; i++)
	{
		window_locator_offsets[i] = locator_index;
		while (locator_index < locator_count &&
			   locators[locator_index].record_start_lsn < windows[i].end_lsn)
			locator_index++;
	}
	window_locator_offsets[window_count] = locator_index;

	for (; desired_workers > 1; desired_workers--)
	{
		int launched = 0;

		tasks_size = MAXALIGN(sizeof(FbWalPayloadTask) * desired_workers);
		windows_size = MAXALIGN(sizeof(FbWalParallelWindow) * window_count);
		segments_size = MAXALIGN(sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);
		locators_size = MAXALIGN(sizeof(FbSummaryPayloadLocator) * locator_count);
		shared_size = MAXALIGN(sizeof(FbWalPayloadShared)) +
			tasks_size + windows_size + segments_size + locators_size;
		seg = dsm_create(shared_size, 0);
		handle = dsm_segment_handle(seg);
		shared = (FbWalPayloadShared *) dsm_segment_address(seg);
		MemSet(shared, 0, shared_size);
		shared->task_count = desired_workers;
		shared->window_count = (int) window_count;
		shared->locator_count = locator_count;
		shared->timeline_id = ctx->timeline_id;
		shared->wal_seg_size = ctx->wal_seg_size;
		shared->resolved_segment_count = ctx->resolved_segment_count;
		shared->tasks_offset = MAXALIGN(sizeof(FbWalPayloadShared));
		shared->windows_offset = shared->tasks_offset + tasks_size;
		shared->segments_offset = shared->windows_offset + windows_size;
		shared->locators_offset = shared->segments_offset + segments_size;
		tasks = fb_wal_payload_tasks(shared);
		shared_windows = fb_wal_payload_windows(shared);
		memcpy(fb_wal_payload_segments(shared),
			   ctx->resolved_segments,
			   sizeof(FbWalSegmentEntry) * ctx->resolved_segment_count);
		memcpy(fb_wal_payload_locators_shared(shared),
			   locators,
			   sizeof(FbSummaryPayloadLocator) * locator_count);

		for (i = 0; i < (int) window_count; i++)
		{
			const FbWalVisitWindow *window = &windows[i];
			FbWalSegmentEntry *base_segments = (FbWalSegmentEntry *) ctx->resolved_segments;

			shared_windows[i].segment_start_index = (uint32) (window->segments - base_segments);
			shared_windows[i].segment_count = window->segment_count;
			shared_windows[i].start_lsn = window->start_lsn;
			shared_windows[i].end_lsn = window->end_lsn;
			shared_windows[i].read_end_lsn = window->read_end_lsn;
		}

		chunk_size = ((int) window_count + desired_workers - 1) / desired_workers;
		for (i = 0; i < desired_workers; i++)
		{
			char spool_path[MAXPGPATH];
			int window_start = i * chunk_size;
			int window_end = Min(window_start + chunk_size, (int) window_count);
			uint32 task_locator_start;
			uint32 task_locator_count;

			if (window_start >= window_end)
				break;

			task_locator_start = window_locator_offsets[window_start];
			task_locator_count =
				window_locator_offsets[window_end] - task_locator_start;
			snprintf(spool_path, sizeof(spool_path), "%s/wal-payload-worker-%d.bin",
					 session_dir, i + 1);
			fb_wal_payload_fill_task(&tasks[i],
									 info,
									 ctx->target_ts,
									 index->anchor_redo_lsn,
									 window_start,
									 window_end,
									 task_locator_start,
									 task_locator_count,
									 spool_path);
			if (!fb_wal_payload_launch_worker(handle, i, &worker_handles[i]))
				break;
			launched++;
		}

		if (launched != desired_workers)
		{
			for (i = 0; i < launched; i++)
				TerminateBackgroundWorker(worker_handles[i]);
			for (i = 0; i < launched; i++)
			{
				BgwHandleStatus status = WaitForBackgroundWorkerShutdown(worker_handles[i]);

				if (status == BGWH_POSTMASTER_DIED)
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("postmaster died while stopping pg_flashback WAL payload locator workers")));
				if (tasks[i].spool_path[0] != '\0')
					unlink(tasks[i].spool_path);
			}
			dsm_detach(seg);
			continue;
		}

		for (i = 0; i < desired_workers; i++)
			fb_wal_payload_wait_worker(worker_handles[i], &tasks[i]);
		for (i = 0; i < desired_workers; i++)
		{
			fb_wal_payload_merge_log(index, tasks[i].spool_path, tasks[i].record_count);
			index->payload_scanned_record_count += tasks[i].scanned_record_count;
			index->payload_sparse_reader_resets += tasks[i].reader_reset_count;
			index->payload_sparse_reader_reuses += tasks[i].reader_reuse_count;
		}
		index->payload_window_count = window_count;
		index->payload_parallel_workers = desired_workers;
		dsm_detach(seg);
		pfree(window_locator_offsets);
		return true;
	}

	pfree(window_locator_offsets);
	return false;
}

/*
 * fb_require_archive_has_wal_segments
 *    WAL entry point.
 */

void
fb_require_archive_has_wal_segments(void)
{
	FbWalScanContext ctx;

	MemSet(&ctx, 0, sizeof(ctx));
	fb_collect_archive_segments(&ctx);
}

/*
 * fb_wal_prepare_scan_context
 *    WAL entry point.
 */

void
fb_wal_prepare_scan_context(TimestampTz target_ts,
							FbSpoolSession *spool_session,
							FbWalScanContext *ctx)
{
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	MemSet(ctx, 0, sizeof(*ctx));
	ctx->target_ts = target_ts;
	ctx->query_now_ts = GetCurrentTimestamp();
	ctx->parallel_workers = fb_parallel_workers();
	ctx->current_segment_may_hit = true;
	ctx->spool_session = spool_session;

	fb_collect_archive_segments(ctx);
	ctx->original_start_lsn = ctx->start_lsn;
	fb_maybe_seed_anchor_hint(ctx);
	fb_wal_probe_anchor_coverage(ctx);
}

/*
 * fb_wal_scan_relation_window
 *    WAL entry point.
 */

void
fb_wal_scan_relation_window(const FbRelationInfo *info, FbWalScanContext *ctx)
{
	FbWalScanVisitorState state;
	FbWalVisitWindow *windows = NULL;
	uint32 window_count = 0;
	uint32 i;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.ctx = ctx;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();
	ctx->summary_span_windows = 0;
	ctx->summary_span_segments_read = 0;
	ctx->summary_span_covered_segments = 0;
	ctx->summary_span_fallback_segments = 0;
	ctx->summary_span_public_builds = 0;
	ctx->summary_xid_hits = 0;
	ctx->summary_xid_exact_hits = 0;
	ctx->summary_xid_fallback = 0;
	ctx->summary_xid_segments_read = 0;
	ctx->summary_xid_exact_segments_read = 0;

	fb_prepare_segment_prefilter(info, ctx);
	window_count = fb_build_prefilter_visit_windows(ctx, &windows);
	if (window_count == 0)
		fb_wal_visit_records(ctx, fb_scan_record_visitor, &state);
	else
	{
		for (i = 0; i < window_count; i++)
		{
			fb_wal_visit_window(ctx, &windows[i], fb_scan_record_visitor, &state);
			if (ctx->unsafe)
				break;
		}
		pfree(windows);
	}

	hash_destroy(state.touched_xids);
	hash_destroy(state.unsafe_xids);
}

/*
 * fb_wal_build_record_index
 *    WAL entry point.
 */

void
fb_wal_build_record_index(const FbRelationInfo *info,
						  FbWalScanContext *ctx,
						  FbWalRecordIndex *index,
						  FbWalBuildMode build_mode)
{
	FbWalIndexBuildState state;
	FbWalScanVisitorState anchor_state;
	FbWalVisitWindow *windows = NULL;
	FbWalVisitWindow *span_windows = NULL;
	FbWalVisitWindow *payload_base_windows = NULL;
	FbWalVisitWindow *metadata_windows = NULL;
	FbWalVisitWindow *payload_locator_fallback_base_windows = NULL;
	FbWalVisitWindow *payload_windows = NULL;
	FbWalVisitWindow *payload_sparse_windows = NULL;
	FbWalVisitWindow *payload_windowed_windows = NULL;
	FbWalVisitWindow *payload_parallel_windows = NULL;
	FbSummaryPayloadLocator *payload_locators = NULL;
	uint32		window_count = 0;
	uint32		span_window_count = 0;
	uint32		payload_base_window_count = 0;
	uint32		metadata_window_count = 0;
	uint32		payload_locator_count = 0;
	uint32		payload_locator_fallback_base_count = 0;
	uint32		payload_window_count = 0;
	uint32		payload_sparse_count = 0;
	uint32		payload_windowed_count = 0;
	uint32		payload_parallel_count = 0;
	uint32		scan_segment_total = 0;
	uint32		payload_locator_segment_count = 0;
	bool		metadata_parallel_done = false;
	bool		payload_use_sparse_scan = false;
	bool		payload_parallel_done = false;
	bool		count_only_mode = (build_mode == FB_WAL_BUILD_COUNT_ONLY);
	uint32 i;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (index == NULL)
		elog(ERROR, "FbWalRecordIndex must not be NULL");

	fb_progress_enter_stage(FB_PROGRESS_STAGE_BUILD_INDEX, "prefilter");

	MemSet(index, 0, sizeof(*index));
	index->target_ts = ctx->target_ts;
	index->query_now_ts = ctx->query_now_ts;
	index->memory_limit_bytes = fb_get_memory_limit_bytes();
	index->xid_statuses = fb_create_xid_status_hash();
	index->spool_session = ctx->spool_session;
	index->target_locator = info->locator;
	index->toast_locator = info->toast_locator;
	index->has_toast_locator = info->has_toast_locator;
	index->resolved_segments = (const FbWalResolvedSegment *) ctx->resolved_segments;
	index->resolved_segment_count = ctx->resolved_segment_count;
	index->wal_seg_size = ctx->wal_seg_size;
	index->summary_cache = ctx->summary_cache;

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.ctx = ctx;
	state.index = index;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();
	state.count_only_xids = count_only_mode ? fb_create_count_only_xid_hash() : NULL;
	state.collect_metadata = true;
	state.collect_xact_statuses = false;
	state.capture_payload = false;
	state.count_only_capture = false;
	state.tail_capture_allowed = false;
	state.defer_payload_body = !count_only_mode;
	state.locator_only_payload_capture = false;
	state.payload_log = NULL;
	state.payload_label = NULL;
	state.xact_summary_log = &index->xact_summary_log;
	state.xact_summary_label = "wal-xact-summary";
	ctx->summary_span_windows = 0;
	ctx->summary_span_segments_read = 0;
	ctx->summary_span_covered_segments = 0;
	ctx->summary_span_fallback_segments = 0;
	ctx->summary_span_public_builds = 0;
	ctx->summary_xid_hits = 0;
	ctx->summary_xid_exact_hits = 0;
	ctx->summary_xid_fallback = 0;
	ctx->summary_xid_segments_read = 0;
	ctx->summary_xid_exact_segments_read = 0;
	ctx->xact_summary_spool_hits = 0;
	ctx->xact_fallback_windows = 0;
	ctx->xact_fallback_covered_segments = 0;
	ctx->summary_unsafe_hits = 0;
	ctx->metadata_fallback_windows = 0;
	ctx->payload_sparse_reader_resets = 0;
	ctx->payload_sparse_reader_reuses = 0;
	if (ctx->summary_cache == NULL)
		ctx->summary_cache = fb_summary_query_cache_create(CurrentMemoryContext);
	index->summary_cache = ctx->summary_cache;

	fb_prepare_segment_prefilter(info, ctx);
	window_count = fb_build_prefilter_visit_windows(ctx, &windows);
	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX,
							   fb_progress_map_subrange(0, 10, 1, 1),
							   "summary-span");
	span_window_count =
		fb_build_summary_span_visit_windows(info, ctx, windows, window_count, &span_windows);
	payload_base_window_count = span_window_count;
	payload_base_windows = fb_copy_visit_windows(span_windows, span_window_count);
	span_window_count = fb_merge_visit_windows(ctx, &span_windows, span_window_count);
	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX,
							   fb_progress_map_subrange(10, 20, 1, 1),
							   "metadata");
	metadata_window_count =
		fb_summary_seed_metadata_from_summary(info,
											  ctx,
											  (span_window_count > 0) ? span_windows : windows,
											  (span_window_count > 0) ? span_window_count : window_count,
											  state.touched_xids,
											  state.unsafe_xids,
											  &metadata_windows);
	ctx->visited_segment_count = 0;
	if (window_count == 0)
		scan_segment_total = ctx->resolved_segment_count;
	else
	{
		for (i = 0; i < window_count; i++)
			scan_segment_total += windows[i].segment_count;
	}
	ctx->progress_segment_total = Max((uint32) 1, scan_segment_total * 2);
	metadata_parallel_done =
		(!count_only_mode &&
		 metadata_window_count > 0 &&
		 fb_wal_collect_metadata_parallel(info, ctx, index, &state,
											 metadata_windows, metadata_window_count));
	if (!metadata_parallel_done)
	{
		if (metadata_window_count > 0)
		{
			for (i = 0; i < metadata_window_count; i++)
			{
				fb_wal_visit_window(ctx, &metadata_windows[i], fb_index_record_visitor, &state);
				if (ctx->unsafe)
					break;
			}
		}
	}
	if (!ctx->anchor_found)
	{
		if (window_count == 0)
			fb_wal_visit_records(ctx, fb_index_record_visitor, &state);
		else
		{
			for (i = 0; i < window_count; i++)
			{
				fb_wal_visit_window(ctx, &windows[i], fb_index_record_visitor, &state);
				if (ctx->unsafe)
					break;
			}
		}
	}
	if (!ctx->anchor_found && window_count > 0 && !ctx->unsafe)
	{
		MemSet(&anchor_state, 0, sizeof(anchor_state));
		anchor_state.info = info;
		anchor_state.ctx = ctx;
		anchor_state.touched_xids = state.touched_xids;
		anchor_state.unsafe_xids = state.unsafe_xids;
		fb_wal_visit_records(ctx, fb_scan_record_visitor, &anchor_state);
	}

	if (!ctx->unsafe)
		fb_wal_fill_xact_statuses_serial(info, ctx, index,
										 (span_window_count > 0) ? span_windows : windows,
										 (span_window_count > 0) ? span_window_count : window_count,
										 state.touched_xids,
										 state.unsafe_xids);
	if (!ctx->unsafe && count_only_mode)
		fb_count_only_finalize_index(&state);
	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX,
							   fb_progress_map_subrange(30, 25, 1, 1),
							   "xact-status");

	if (!ctx->anchor_found)
		fb_wal_raise_missing_anchor_error(ctx, false, 0);

	index->anchor_found = ctx->anchor_found;
	index->anchor_checkpoint_lsn = ctx->anchor_checkpoint_lsn;
	index->anchor_redo_lsn = ctx->anchor_redo_lsn;
	index->anchor_time = ctx->anchor_time;

	if (!ctx->unsafe)
	{
		uint32		payload_covered_segment_count = 0;
		uint64		payload_scanned_record_count = 0;
		uint64		payload_kept_record_count = 0;
		bool		payload_capture_anchor_context = false;
		const FbWalVisitWindow *payload_source_windows;
		uint32		payload_source_window_count;

		fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX,
								   fb_progress_map_subrange(55, 15, 1, 1),
								   "payload");
		payload_source_windows =
			(payload_base_window_count > 0) ? payload_base_windows :
			(span_window_count > 0) ? span_windows : windows;
		payload_source_window_count =
			(payload_base_window_count > 0) ? payload_base_window_count :
			(span_window_count > 0) ? span_window_count : window_count;
			payload_locator_count =
				fb_build_summary_payload_locator_plan(info,
													  ctx,
													  payload_source_windows,
													  payload_source_window_count,
												  ctx->anchor_redo_lsn,
												  index->tail_inline_payload ?
												  index->tail_cutover_lsn :
												  ctx->end_lsn,
													  &payload_locators,
													  &payload_locator_fallback_base_windows,
													  &payload_locator_fallback_base_count,
													  &index->summary_payload_locator_fallback_segments,
													  &index->summary_payload_locator_segments_read,
													  NULL);
		payload_locator_segment_count =
			fb_count_payload_locator_segments(ctx,
											  payload_locators,
											  payload_locator_count);
		if (payload_locator_fallback_base_count > 0)
		{
			payload_window_count =
				fb_build_materialize_visit_windows(ctx,
												   payload_locator_fallback_base_windows,
												   payload_locator_fallback_base_count,
												   ctx->anchor_redo_lsn,
												   index->tail_inline_payload ?
												   index->tail_cutover_lsn :
												   ctx->end_lsn,
												   &payload_windows);
			payload_sparse_windows = fb_copy_visit_windows(payload_windows,
														   payload_window_count);
			payload_sparse_count =
				fb_merge_visit_windows(ctx, &payload_sparse_windows, payload_window_count);
			payload_windowed_windows = fb_copy_visit_windows(payload_sparse_windows,
															 payload_sparse_count);
			payload_windowed_count =
				fb_coalesce_payload_visit_windows(ctx,
												  &payload_windowed_windows,
												  payload_sparse_count);
			payload_parallel_windows =
				fb_copy_visit_windows(payload_windowed_windows, payload_windowed_count);
			payload_parallel_count = fb_split_parallel_materialize_windows(ctx,
																		  &payload_parallel_windows,
																		  payload_windowed_count);
			payload_covered_segment_count =
				fb_wal_count_window_segments(payload_windowed_windows,
											 payload_windowed_count);
			payload_use_sparse_scan =
				fb_should_use_sparse_payload_scan(payload_sparse_count,
												  (payload_parallel_count > 0) ?
												  payload_parallel_count :
												  payload_windowed_count,
												  payload_covered_segment_count);
		}
		payload_covered_segment_count += payload_locator_segment_count;
		state.collect_metadata = false;
		state.capture_payload = !count_only_mode;
		state.count_only_capture = count_only_mode;
		state.tail_capture_allowed = false;
		state.payload_log = &index->record_log;
		state.payload_label = "wal-records";
		index->summary_payload_locator_records = payload_locator_count;
		index->summary_payload_locator_public_builds =
			fb_summary_query_cache_payload_locator_public_builds(ctx->summary_cache);
		index->payload_scan_mode = (payload_locator_count > 0) ?
			FB_WAL_PAYLOAD_SCAN_LOCATOR :
			(payload_use_sparse_scan ?
			 FB_WAL_PAYLOAD_SCAN_SPARSE :
			 FB_WAL_PAYLOAD_SCAN_WINDOWED);
		index->payload_window_count = (payload_locator_count > 0) ?
			payload_locator_count :
			(payload_use_sparse_scan ? payload_sparse_count : payload_windowed_count);
		index->payload_parallel_workers = 0;
		index->payload_covered_segment_count = payload_covered_segment_count;
		ctx->visited_segment_count = scan_segment_total;
		if (payload_use_sparse_scan)
			qsort(payload_sparse_windows, payload_sparse_count, sizeof(*payload_sparse_windows),
				  fb_visit_window_cmp);
		if (payload_locator_count > 0 &&
			ctx->parallel_workers > 1 &&
			!count_only_mode)
		{
			for (i = 0; i < payload_locator_count; i++)
				fb_index_append_locator_stub(index,
											 state.payload_log,
											 state.payload_label,
											 payload_locators[i].record_start_lsn);
			index->payload_scanned_record_count += payload_locator_count;
			index->payload_window_count = payload_locator_count;
			state.locator_only_payload_capture = true;
		}
		else if (payload_locator_count > 0)
			index->payload_window_count =
				fb_wal_visit_payload_locators(ctx, payload_locators, payload_locator_count, &state);
		if ((payload_use_sparse_scan && payload_sparse_count > 0) ||
			(!payload_use_sparse_scan && payload_windowed_count > 0))
		{
			XLogRecPtr payload_emit_floor = InvalidXLogRecPtr;

			if (count_only_mode)
			{
				if (payload_use_sparse_scan)
				{
					qsort(payload_sparse_windows, payload_sparse_count, sizeof(*payload_sparse_windows),
						  fb_visit_window_cmp);
					fb_wal_visit_sparse_windows(ctx,
												payload_sparse_windows,
												payload_sparse_count,
												&state);
				}
				else
				{
					for (i = 0; i < payload_windowed_count; i++)
					{
						FbWalVisitWindow read_window;

						state.payload_emit_start_lsn = payload_windowed_windows[i].start_lsn;
						state.payload_emit_end_lsn = payload_windowed_windows[i].end_lsn;
						fb_wal_prepare_payload_read_window(ctx,
														   &payload_windowed_windows[i],
														   (FbWalSegmentEntry *) ctx->resolved_segments,
														   ctx->resolved_segment_count,
														   &read_window);
						fb_wal_visit_window(ctx, &read_window,
											fb_index_record_visitor, &state);
					}
				}
			}
			else if (payload_use_sparse_scan)
			{
				for (i = 0; i < payload_sparse_count; i++)
				{
					XLogRecPtr emit_start = payload_sparse_windows[i].start_lsn;

					if (!XLogRecPtrIsInvalid(payload_emit_floor) &&
						emit_start < payload_emit_floor)
						emit_start = payload_emit_floor;
					if (emit_start >= payload_sparse_windows[i].end_lsn)
						continue;
					payload_sparse_windows[i].start_lsn = emit_start;
					if (XLogRecPtrIsInvalid(payload_emit_floor) ||
						payload_sparse_windows[i].end_lsn > payload_emit_floor)
						payload_emit_floor = payload_sparse_windows[i].end_lsn;
				}
				state.payload_emit_start_lsn = InvalidXLogRecPtr;
				state.payload_emit_end_lsn = InvalidXLogRecPtr;
				payload_parallel_done =
					fb_wal_materialize_payload_parallel(info, ctx, index,
														payload_sparse_windows,
														payload_sparse_count);
				if (!payload_parallel_done)
				{
						fb_wal_enable_incremental_payload_stats(index, &state);
						fb_wal_visit_sparse_windows(ctx,
													payload_sparse_windows,
													payload_sparse_count,
													&state);
				}
			}
			else
			{
				if (payload_parallel_count > 0)
				{
					payload_parallel_done =
						fb_wal_materialize_payload_parallel(info, ctx, index,
															payload_parallel_windows,
															payload_parallel_count);
				}

				if (payload_parallel_done)
				{
					index->payload_window_count = payload_parallel_count;
				}
				else
				{
					fb_wal_enable_incremental_payload_stats(index, &state);
					for (i = 0; i < payload_windowed_count; i++)
					{
						FbWalVisitWindow read_window;
						XLogRecPtr emit_start = payload_windowed_windows[i].start_lsn;

						fb_wal_prepare_payload_read_window(ctx,
														   &payload_windowed_windows[i],
														   (FbWalSegmentEntry *) ctx->resolved_segments,
														   ctx->resolved_segment_count,
														   &read_window);
						if (payload_capture_anchor_context)
							emit_start = read_window.start_lsn;
						else if (!XLogRecPtrIsInvalid(payload_emit_floor) &&
							emit_start < payload_emit_floor)
							emit_start = payload_emit_floor;
						if (emit_start >= payload_windowed_windows[i].end_lsn)
							continue;
						state.payload_emit_start_lsn = emit_start;
						state.payload_emit_end_lsn = payload_capture_anchor_context ?
							read_window.end_lsn :
							payload_windowed_windows[i].end_lsn;
						fb_wal_visit_window(ctx, &read_window,
											fb_index_record_visitor, &state);
						if (!payload_capture_anchor_context &&
							(XLogRecPtrIsInvalid(payload_emit_floor) ||
							 payload_windowed_windows[i].end_lsn > payload_emit_floor))
							payload_emit_floor = payload_windowed_windows[i].end_lsn;
					}
				}
			}
		}
		if (payload_windows != NULL)
			pfree(payload_windows);
		if (payload_sparse_windows != NULL)
			pfree(payload_sparse_windows);
		if (payload_windowed_windows != NULL)
			pfree(payload_windowed_windows);
		if (payload_parallel_windows != NULL)
			pfree(payload_parallel_windows);
		if (payload_locator_fallback_base_windows != NULL)
		{
			pfree(payload_locator_fallback_base_windows);
			payload_locator_fallback_base_windows = NULL;
		}
		if (payload_locators != NULL)
		{
			pfree(payload_locators);
			payload_locators = NULL;
		}
		if (payload_base_windows != NULL)
		{
			pfree(payload_base_windows);
			payload_base_windows = NULL;
		}

			payload_scanned_record_count = index->payload_scanned_record_count;
			payload_kept_record_count = index->record_count;
			if (!count_only_mode && state.locator_only_payload_capture)
			{
				if (state.payload_block_states != NULL)
				{
					hash_destroy(state.payload_block_states);
					state.payload_block_states = NULL;
				}
				fb_wal_finalize_record_stats(index);
			}
			else if (!count_only_mode && state.payload_block_states != NULL)
			{
				hash_destroy(state.payload_block_states);
				state.payload_block_states = NULL;
				if (index->precomputed_missing_blocks != NULL)
					index->precomputed_missing_block_count =
						hash_get_num_entries(index->precomputed_missing_blocks);
			}
			else if (!count_only_mode)
				fb_wal_finalize_record_stats(index);
			index->payload_covered_segment_count = payload_covered_segment_count;
		index->payload_scanned_record_count = payload_scanned_record_count;
		index->payload_kept_record_count = payload_kept_record_count;
		index->payload_sparse_reader_resets += ctx->payload_sparse_reader_resets;
		index->payload_sparse_reader_reuses += ctx->payload_sparse_reader_reuses;
		fb_summary_service_report_query_summary_usage(GetCurrentTimestamp(),
													  ctx->summary_span_fallback_segments,
													  ctx->metadata_fallback_windows);
	}

	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX,
							   fb_progress_map_subrange(70, 30, 1, 1),
							   "payload");

	index->unsafe = ctx->unsafe;
	index->unsafe_reason = ctx->unsafe_reason;
	index->unsafe_xid = ctx->unsafe_xid;
	index->unsafe_commit_ts = ctx->unsafe_commit_ts;
	index->unsafe_record_lsn = ctx->unsafe_record_lsn;
	index->unsafe_scope = ctx->unsafe_scope;
	index->unsafe_storage_op = ctx->unsafe_storage_op;

	if (windows != NULL)
		pfree(windows);
	if (span_windows != NULL)
		pfree(span_windows);
	if (metadata_windows != NULL)
		pfree(metadata_windows);
	if (payload_base_windows != NULL)
		pfree(payload_base_windows);
	hash_destroy(state.touched_xids);
	hash_destroy(state.unsafe_xids);
	if (state.count_only_xids != NULL)
		hash_destroy(state.count_only_xids);
}

Datum
fb_wal_payload_window_contract_debug(PG_FUNCTION_ARGS)
{
	FbWalScanContext ctx;
	FbWalSegmentEntry segments[2];
	FbWalVisitWindow emit_window;
	FbWalVisitWindow read_window;
	XLogSegNo current_segno;

	MemSet(&ctx, 0, sizeof(ctx));
	MemSet(segments, 0, sizeof(segments));
	MemSet(&emit_window, 0, sizeof(emit_window));
	MemSet(&read_window, 0, sizeof(read_window));

	ctx.wal_seg_size = 16 * 1024 * 1024;
	ctx.resolved_segments = segments;
	ctx.resolved_segment_count = lengthof(segments);

	emit_window.segments = &segments[1];
	emit_window.segment_count = 1;
	emit_window.start_lsn = UINT64CONST(0x00000089F0001908);
	emit_window.end_lsn = UINT64CONST(0x00000089F0001A00);
	emit_window.read_end_lsn = emit_window.end_lsn;

	XLByteToSeg(emit_window.start_lsn, current_segno, ctx.wal_seg_size);
	segments[0].segno = current_segno - 1;
	segments[1].segno = current_segno;

	fb_wal_prepare_payload_read_window(&ctx,
									   &emit_window,
									   segments,
									   lengthof(segments),
									   &read_window);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"emit_start=%X/%08X read_start=%X/%08X read_segments=%u",
		LSN_FORMAT_ARGS(emit_window.start_lsn),
		LSN_FORMAT_ARGS(read_window.start_lsn),
		read_window.segment_count)));
}

Datum
fb_wal_nonapply_image_spool_contract_debug(PG_FUNCTION_ARGS)
{
	FbWalRecordIndex index;
	FbSpoolSession *session;
	FbWalRecordCursor *cursor;
	FbRecordRef input;
	FbRecordRef stored;
	FbRecordBlockRef *block_ref;
	char image_page[BLCKSZ];
	char *result;
	uint32 record_index = 0;
	bool read_ok;
	bool stored_has_image;
	bool stored_image_is_null;
	bool materializes;

	MemSet(&index, 0, sizeof(index));
	MemSet(&input, 0, sizeof(input));
	MemSet(&stored, 0, sizeof(stored));
	MemSet(image_page, 0x5A, sizeof(image_page));

	session = fb_spool_session_create();
	index.spool_session = session;
	index.anchor_redo_lsn = InvalidXLogRecPtr + 16;

	input.kind = FB_WAL_RECORD_XLOG_FPI_FOR_HINT;
	input.lsn = InvalidXLogRecPtr + 1;
	input.end_lsn = input.lsn + 1;
	input.block_count = 1;
	block_ref = &input.blocks[0];
	block_ref->in_use = true;
	block_ref->forknum = MAIN_FORKNUM;
	block_ref->blkno = 42;
	block_ref->has_image = true;
	block_ref->apply_image = false;
	block_ref->image = image_page;

	fb_index_append_record(&index, &index.record_log, "contract", &input, false);

	cursor = fb_wal_record_cursor_open(&index, FB_SPOOL_FORWARD);
	read_ok = fb_wal_record_cursor_read(cursor, &stored, &record_index);
	if (!read_ok)
	{
		fb_wal_record_cursor_close(cursor);
		fb_spool_session_destroy(session);
		elog(ERROR, "failed to read back non-apply image contract record");
	}
	stored_has_image = stored.blocks[0].has_image;
	stored_image_is_null = (stored.blocks[0].image == NULL);
	materializes = fb_wal_record_block_materializes_page(&stored, 0);
	fb_wal_record_cursor_close(cursor);

	result = psprintf(
		"stored_has_image=%s stored_image_is_null=%s spool_bytes=%llu materializes=%s",
		stored_has_image ? "true" : "false",
		stored_image_is_null ? "true" : "false",
		(unsigned long long) fb_spool_log_size(index.record_log),
		materializes ? "true" : "false");
	fb_spool_session_destroy(session);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
fb_wal_hint_fpi_payload_contract_debug(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"hint_fpi_payload_enabled=%s",
		fb_wal_payload_kind_enabled(FB_WAL_RECORD_XLOG_FPI_FOR_HINT) ?
		"true" : "false")));
}

Datum
fb_wal_heap2_visible_payload_contract_debug(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(psprintf(
		"heap2_visible_payload_enabled=%s",
		fb_wal_payload_kind_enabled(FB_WAL_RECORD_HEAP2_VISIBLE) ?
		"true" : "false")));
}

Datum
fb_summary_xid_resolution_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext ctx;
	FbWalRecordIndex index;
	FbWalIndexBuildState state;
	FbWalVisitWindow *windows = NULL;
	FbWalVisitWindow *span_windows = NULL;
	FbWalVisitWindow *metadata_windows = NULL;
	FbWalVisitWindow *fallback_windows = NULL;
	HTAB *fallback_touched = NULL;
	HTAB *fallback_unsafe = NULL;
	StringInfoData buf;
	uint32 window_count = 0;
	uint32 span_window_count = 0;
	uint32 metadata_window_count = 0;
	uint32 fallback_window_count = 0;
	uint32 unresolved_touched = 0;
	uint32 unresolved_unsafe = 0;
	HASH_SEQ_STATUS status;
	FbTouchedXidEntry *touched_entry;
	FbUnsafeXidEntry *unsafe_entry;

	if (target_ts > GetCurrentTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target timestamp must not be in the future")));

	fb_require_archive_has_wal_segments();
	fb_catalog_load_relation_info(relid, &info);
	fb_wal_prepare_scan_context(target_ts, NULL, &ctx);

	MemSet(&index, 0, sizeof(index));
	index.target_ts = ctx.target_ts;
	index.query_now_ts = ctx.query_now_ts;
	index.xid_statuses = fb_create_xid_status_hash();
	if (ctx.summary_cache == NULL)
		ctx.summary_cache = fb_summary_query_cache_create(CurrentMemoryContext);
	index.summary_cache = ctx.summary_cache;

	MemSet(&state, 0, sizeof(state));
	state.info = &info;
	state.ctx = &ctx;
	state.index = &index;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();
	state.collect_metadata = true;
	state.collect_xact_statuses = false;
	state.capture_payload = false;
	state.count_only_capture = false;
	state.tail_capture_allowed = false;
	state.defer_payload_body = true;
	state.locator_only_payload_capture = false;
	state.xact_summary_log = NULL;
	state.xact_summary_label = NULL;

	fb_prepare_segment_prefilter(&info, &ctx);
	window_count = fb_build_prefilter_visit_windows(&ctx, &windows);
	span_window_count =
		fb_build_summary_span_visit_windows(&info, &ctx, windows, window_count, &span_windows);
	if (span_window_count > 0)
		span_window_count = fb_merge_visit_windows(&ctx, &span_windows, span_window_count);

	metadata_window_count =
		fb_summary_seed_metadata_from_summary(&info,
											  &ctx,
											  (span_window_count > 0) ? span_windows : windows,
											  (span_window_count > 0) ? span_window_count : window_count,
											  state.touched_xids,
											  state.unsafe_xids,
											  &metadata_windows);
	if (metadata_window_count > 0)
	{
		uint32 i;

		for (i = 0; i < metadata_window_count; i++)
			fb_wal_visit_window(&ctx, &metadata_windows[i], fb_index_record_visitor, &state);
	}

	(void) fb_summary_fill_xact_statuses(&info,
										 &ctx,
										 &index,
										 (span_window_count > 0) ? span_windows : windows,
										 (span_window_count > 0) ? span_window_count : window_count,
										 state.touched_xids,
										 state.unsafe_xids);

	fb_build_unresolved_xid_fallback_sets(&index,
										  state.touched_xids,
										  state.unsafe_xids,
										  &fallback_touched,
										  &fallback_unsafe);
	if ((fallback_touched != NULL && hash_get_num_entries(fallback_touched) > 0) ||
		(fallback_unsafe != NULL && hash_get_num_entries(fallback_unsafe) > 0))
	{
		fallback_window_count =
			fb_build_xact_fallback_visit_windows(&ctx,
												 (span_window_count > 0) ? span_windows : windows,
												 (span_window_count > 0) ? span_window_count : window_count,
												 &fallback_windows);
		(void) fb_summary_fill_exact_xact_statuses(&ctx,
												   &index,
												   fallback_windows,
												   fallback_window_count,
												   fallback_touched,
												   fallback_unsafe);
	}

	if (fallback_touched != NULL)
	{
		hash_destroy(fallback_touched);
		fallback_touched = NULL;
	}
	if (fallback_unsafe != NULL)
	{
		hash_destroy(fallback_unsafe);
		fallback_unsafe = NULL;
	}

	fb_build_unresolved_xid_fallback_sets(&index,
										  state.touched_xids,
										  state.unsafe_xids,
										  &fallback_touched,
										  &fallback_unsafe);
	if ((fallback_touched != NULL && hash_get_num_entries(fallback_touched) > 0) ||
		(fallback_unsafe != NULL && hash_get_num_entries(fallback_unsafe) > 0))
	{
		(void) fb_summary_fill_exact_xact_statuses(&ctx,
												   &index,
												   NULL,
												   0,
												   fallback_touched,
												   fallback_unsafe);
	}

	if (fallback_touched != NULL)
	{
		hash_seq_init(&status, fallback_touched);
		while ((touched_entry = (FbTouchedXidEntry *) hash_seq_search(&status)) != NULL)
		{
			FbXidStatusEntry *entry;

			entry = fb_get_xid_status_entry(index.xid_statuses, touched_entry->xid, NULL);
			if (entry == NULL || entry->status == FB_WAL_XID_UNKNOWN)
				unresolved_touched++;
		}
	}
	if (fallback_unsafe != NULL)
	{
		hash_seq_init(&status, fallback_unsafe);
		while ((unsafe_entry = (FbUnsafeXidEntry *) hash_seq_search(&status)) != NULL)
		{
			FbXidStatusEntry *entry;

			entry = fb_get_xid_status_entry(index.xid_statuses, unsafe_entry->xid, NULL);
			if (entry == NULL || entry->status == FB_WAL_XID_UNKNOWN)
				unresolved_unsafe++;
		}
	}

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "summary_hits=%u summary_exact_hits=%u unresolved_touched=%u unresolved_unsafe=%u fallback_windows=%u touched_samples=",
					 ctx.summary_xid_hits,
					 ctx.summary_xid_exact_hits,
					 unresolved_touched,
					 unresolved_unsafe,
					 fallback_window_count);
	fb_append_touched_xid_hash_samples(&buf, fallback_touched, 8);

	if (fallback_windows != NULL)
		pfree(fallback_windows);
	if (metadata_windows != NULL)
		pfree(metadata_windows);
	if (span_windows != NULL)
		pfree(span_windows);
	if (windows != NULL)
		pfree(windows);
	if (fallback_touched != NULL)
		hash_destroy(fallback_touched);
	if (fallback_unsafe != NULL)
		hash_destroy(fallback_unsafe);
	if (state.touched_xids != NULL)
		hash_destroy(state.touched_xids);
	if (state.unsafe_xids != NULL)
		hash_destroy(state.unsafe_xids);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

Datum
fb_summary_payload_locator_plan_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	TimestampTz target_ts = PG_GETARG_TIMESTAMPTZ(1);
	FbRelationInfo info;
	FbWalScanContext ctx;
	FbWalRecordIndex index;
	FbWalIndexBuildState state;
	FbWalScanVisitorState anchor_state;
	FbWalVisitWindow *windows = NULL;
	FbWalVisitWindow *span_windows = NULL;
	FbWalVisitWindow *payload_base_windows = NULL;
	FbWalVisitWindow *metadata_windows = NULL;
	FbWalVisitWindow *payload_locator_fallback_base_windows = NULL;
	FbSummaryPayloadLocator *payload_locators = NULL;
	FbPayloadLocatorPlanDebug plan_debug;
	StringInfoData buf;
	const FbWalVisitWindow *payload_source_windows;
	uint32 window_count = 0;
	uint32 span_window_count = 0;
	uint32 payload_base_window_count = 0;
	uint32 metadata_window_count = 0;
	uint32 payload_source_window_count = 0;
	uint32 payload_locator_count = 0;
	uint32 payload_locator_fallback_base_count = 0;
	uint32 payload_locator_fallback_segments = 0;
	uint32 payload_locator_segments_read = 0;
	uint32 i;

	if (target_ts > GetCurrentTimestamp())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target timestamp must not be in the future")));

	fb_require_archive_has_wal_segments();
	fb_catalog_load_relation_info(relid, &info);
	fb_wal_prepare_scan_context(target_ts, NULL, &ctx);

	MemSet(&index, 0, sizeof(index));
	index.target_ts = ctx.target_ts;
	index.query_now_ts = ctx.query_now_ts;
	index.xid_statuses = fb_create_xid_status_hash();
	if (ctx.summary_cache == NULL)
		ctx.summary_cache = fb_summary_query_cache_create(CurrentMemoryContext);
	index.summary_cache = ctx.summary_cache;

	MemSet(&state, 0, sizeof(state));
	state.info = &info;
	state.ctx = &ctx;
	state.index = &index;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();
	state.collect_metadata = true;
	state.collect_xact_statuses = false;
	state.capture_payload = false;
	state.count_only_capture = false;
	state.tail_capture_allowed = false;
	state.defer_payload_body = true;
	state.locator_only_payload_capture = false;
	state.xact_summary_log = NULL;
	state.xact_summary_label = NULL;

	fb_prepare_segment_prefilter(&info, &ctx);
	window_count = fb_build_prefilter_visit_windows(&ctx, &windows);
	span_window_count =
		fb_build_summary_span_visit_windows(&info, &ctx, windows, window_count, &span_windows);
	payload_base_window_count = span_window_count;
	payload_base_windows = fb_copy_visit_windows(span_windows, span_window_count);
	span_window_count = fb_merge_visit_windows(&ctx, &span_windows, span_window_count);
	metadata_window_count =
		fb_summary_seed_metadata_from_summary(&info,
											  &ctx,
											  (span_window_count > 0) ? span_windows : windows,
											  (span_window_count > 0) ? span_window_count : window_count,
											  state.touched_xids,
											  state.unsafe_xids,
											  &metadata_windows);
	if (metadata_window_count > 0)
	{
		for (i = 0; i < metadata_window_count; i++)
		{
			fb_wal_visit_window(&ctx, &metadata_windows[i], fb_index_record_visitor, &state);
			if (ctx.unsafe)
				break;
		}
	}
	if (!ctx.anchor_found)
	{
		if (window_count == 0)
			fb_wal_visit_records(&ctx, fb_index_record_visitor, &state);
		else
		{
			for (i = 0; i < window_count; i++)
			{
				fb_wal_visit_window(&ctx, &windows[i], fb_index_record_visitor, &state);
				if (ctx.unsafe)
					break;
			}
		}
	}
	if (!ctx.anchor_found && window_count > 0 && !ctx.unsafe)
	{
		MemSet(&anchor_state, 0, sizeof(anchor_state));
		anchor_state.info = &info;
		anchor_state.ctx = &ctx;
		anchor_state.touched_xids = state.touched_xids;
		anchor_state.unsafe_xids = state.unsafe_xids;
		fb_wal_visit_records(&ctx, fb_scan_record_visitor, &anchor_state);
	}
	if (!ctx.anchor_found)
		fb_wal_raise_missing_anchor_error(&ctx, false, 0);
	index.anchor_found = ctx.anchor_found;
	index.anchor_redo_lsn = ctx.anchor_redo_lsn;
	index.tail_inline_payload = false;
	index.tail_cutover_lsn = InvalidXLogRecPtr;
	payload_source_windows =
		(payload_base_window_count > 0) ? payload_base_windows :
		(span_window_count > 0) ? span_windows : windows;
	payload_source_window_count =
		(payload_base_window_count > 0) ? payload_base_window_count :
		(span_window_count > 0) ? span_window_count : window_count;

	MemSet(&plan_debug, 0, sizeof(plan_debug));
	payload_locator_count =
		fb_build_summary_payload_locator_plan(&info,
											  &ctx,
											  payload_source_windows,
											  payload_source_window_count,
											  index.anchor_redo_lsn,
											  index.tail_inline_payload ?
											  index.tail_cutover_lsn :
											  ctx.end_lsn,
											  &payload_locators,
											  &payload_locator_fallback_base_windows,
											  &payload_locator_fallback_base_count,
											  &payload_locator_fallback_segments,
											  &payload_locator_segments_read,
											  &plan_debug);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "base_segments=%u success_segments=%u empty_success_segments=%u segments_with_locators=%u fallback_segments=%u locator_records=%u base_archive=%u base_pgwal=%u base_archive_dir=%u base_ckwal=%u success_archive=%u success_pgwal=%u success_archive_dir=%u success_ckwal=%u fallback_archive=%u fallback_pgwal=%u fallback_archive_dir=%u fallback_ckwal=%u locator_archive=%u locator_pgwal=%u locator_archive_dir=%u locator_ckwal=%u failed_segments=",
					 plan_debug.base_segments,
					 plan_debug.success_segments,
					 plan_debug.empty_success_segments,
					 plan_debug.segments_with_locators,
					 plan_debug.fallback_segments,
					 payload_locator_count,
					 plan_debug.base_by_source[FB_WAL_SOURCE_ARCHIVE_DEST],
					 plan_debug.base_by_source[FB_WAL_SOURCE_PG_WAL],
					 plan_debug.base_by_source[FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY],
					 plan_debug.base_by_source[FB_WAL_SOURCE_CKWAL],
					 plan_debug.success_by_source[FB_WAL_SOURCE_ARCHIVE_DEST],
					 plan_debug.success_by_source[FB_WAL_SOURCE_PG_WAL],
					 plan_debug.success_by_source[FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY],
					 plan_debug.success_by_source[FB_WAL_SOURCE_CKWAL],
					 plan_debug.fallback_by_source[FB_WAL_SOURCE_ARCHIVE_DEST],
					 plan_debug.fallback_by_source[FB_WAL_SOURCE_PG_WAL],
					 plan_debug.fallback_by_source[FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY],
					 plan_debug.fallback_by_source[FB_WAL_SOURCE_CKWAL],
					 plan_debug.locator_by_source[FB_WAL_SOURCE_ARCHIVE_DEST],
					 plan_debug.locator_by_source[FB_WAL_SOURCE_PG_WAL],
					 plan_debug.locator_by_source[FB_WAL_SOURCE_ARCHIVE_DIR_LEGACY],
					 plan_debug.locator_by_source[FB_WAL_SOURCE_CKWAL]);
	fb_append_payload_locator_failed_samples(&buf, &plan_debug);

	if (payload_locators != NULL)
		pfree(payload_locators);
	if (payload_locator_fallback_base_windows != NULL)
		pfree(payload_locator_fallback_base_windows);
	if (metadata_windows != NULL)
		pfree(metadata_windows);
	if (payload_base_windows != NULL)
		pfree(payload_base_windows);
	if (span_windows != NULL)
		pfree(span_windows);
	if (windows != NULL)
		pfree(windows);
	if (state.touched_xids != NULL)
		hash_destroy(state.touched_xids);
	if (state.unsafe_xids != NULL)
		hash_destroy(state.unsafe_xids);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * fb_wal_lookup_xid_status
 *    WAL entry point.
 */

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

/*
 * fb_wal_unsafe_reason_name
 *    WAL entry point.
 */

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

const char *
fb_wal_unsafe_scope_name(FbWalUnsafeScope scope)
{
	switch (scope)
	{
		case FB_WAL_UNSAFE_SCOPE_NONE:
			return "none";
		case FB_WAL_UNSAFE_SCOPE_MAIN:
			return "main";
		case FB_WAL_UNSAFE_SCOPE_TOAST:
			return "toast";
	}

	return "unknown";
}

const char *
fb_wal_storage_change_op_name(FbWalStorageChangeOp op)
{
	switch (op)
	{
		case FB_WAL_STORAGE_CHANGE_UNKNOWN:
			return "unknown";
		case FB_WAL_STORAGE_CHANGE_STANDBY_LOCK:
			return "standby_lock";
		case FB_WAL_STORAGE_CHANGE_SMGR_CREATE:
			return "smgr_create";
		case FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE:
			return "smgr_truncate";
	}

	return "unknown";
}

const char *
fb_wal_payload_scan_mode_name(FbWalPayloadScanMode mode)
{
	switch (mode)
	{
		case FB_WAL_PAYLOAD_SCAN_WINDOWED:
			return "windowed";
		case FB_WAL_PAYLOAD_SCAN_SPARSE:
			return "sparse";
		case FB_WAL_PAYLOAD_SCAN_LOCATOR:
			return "locator";
	}

	return "unknown";
}
