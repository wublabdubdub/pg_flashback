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
#include "storage/fd.h"
#include "storage/standbydefs.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "fb_guc.h"
#include "fb_ckwal.h"
#include "fb_progress.h"
#include "fb_runtime.h"
#include "fb_wal.h"

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
	bool collect_metadata;
	bool capture_payload;
	bool tail_capture_allowed;
	FbSpoolLog **payload_log;
	const char *payload_label;
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
	bool init_page;
	int32 block_count;
	uint32 main_data_len;
	FbSerializedRecordBlockRef blocks[FB_WAL_MAX_BLOCK_REFS];
} FbSerializedRecordHeader;

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
};

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
	FbWalPrefilterPattern patterns[4];
	int pattern_count;
	bool *hit_map;
	char meta_dir[MAXPGPATH];
	uint64 pattern_hash;
} FbWalSegmentPrefilterTask;

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
} FbWalVisitWindow;

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

/*
 * fb_mark_unsafe
 *    WAL helper.
 */

static void fb_mark_unsafe(FbWalScanContext *ctx, FbWalUnsafeReason reason);
static void fb_capture_unsafe_context(FbWalScanContext *ctx,
									  const FbUnsafeXidEntry *entry,
									  TransactionId xid,
									  TimestampTz commit_ts);
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
static void fb_maybe_activate_tail_payload_capture(XLogReaderState *reader,
												   FbWalIndexBuildState *state);
static void fb_index_note_materialized_record(FbWalRecordIndex *index,
											  FbRecordRef *record);
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
 * fb_convert_mismatched_pg_wal_entry
 *    WAL helper.
 */

static bool fb_convert_mismatched_pg_wal_entry(FbWalScanContext *ctx,
												   FbWalSegmentEntry *entry,
												   FbWalSegmentEntry *converted);
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
/*
 * fb_prepare_segment_prefilter
 *    WAL helper.
 */

static void fb_prepare_segment_prefilter(const FbRelationInfo *info,
										 FbWalScanContext *ctx);
static uint32 fb_build_materialize_visit_windows(FbWalScanContext *ctx,
												 const FbWalVisitWindow *base_windows,
												 uint32 base_window_count,
												 XLogRecPtr start_lsn,
												 XLogRecPtr end_lsn,
												 FbWalVisitWindow **windows_out);
/*
 * fb_wal_visit_window
 *    WAL helper.
 */

static void fb_wal_visit_window(FbWalScanContext *ctx,
								const FbWalVisitWindow *window,
								FbWalRecordVisitor visitor,
								void *arg);

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
fb_segment_path_matches_patterns(const char *path,
								 const char *meta_dir,
								 uint64 pattern_hash,
								 FbWalPrefilterPattern *patterns,
								 int pattern_count)
{
	int fd = -1;
	struct stat st;
	void *map = MAP_FAILED;
	char cache_path[MAXPGPATH];
	uint64 cache_hash;
	uint64 file_identity_hash = 0;
	bool cached_hit = false;
	int i;

	if (pattern_count <= 0)
		return true;

	cache_path[0] = '\0';

	if (meta_dir != NULL && meta_dir[0] != '\0' && stat(path, &st) == 0)
	{
		file_identity_hash = fb_sidecar_file_identity_hash(path, st.st_size,
														   st.st_mtime);
		cache_hash = UINT64CONST(1469598103934665603);
		cache_hash = fb_prefilter_hash_bytes(cache_hash, &file_identity_hash,
											 sizeof(file_identity_hash));
		cache_hash = fb_prefilter_hash_bytes(cache_hash, &pattern_hash, sizeof(pattern_hash));
		snprintf(cache_path, sizeof(cache_path), "%s/prefilter-%016llx.meta",
				 meta_dir, (unsigned long long) cache_hash);

		if (fb_prefilter_sidecar_load(cache_path,
									  file_identity_hash,
									  pattern_hash,
									  &cached_hit))
			return cached_hit;
	}

	fd = open(path, O_RDONLY | PG_BINARY, 0);
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
			if (cache_path[0] != '\0')
				fb_prefilter_sidecar_store(cache_path,
										   file_identity_hash,
										   pattern_hash,
										   true);
			munmap(map, st.st_size);
			return true;
		}
	}

	if (cache_path[0] != '\0')
		fb_prefilter_sidecar_store(cache_path,
								   file_identity_hash,
								   pattern_hash,
								   false);

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

		task->hit_map[i] = fb_segment_path_matches_patterns(task->segments[i].path,
															 task->meta_dir,
															 task->pattern_hash,
															 task->patterns,
															 task->pattern_count);
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
	pthread_t workers[16];
	bool worker_started[16];
	FbWalSegmentPrefilterTask tasks[16];
	FbWalPrefilterPattern patterns[4];
	char *meta_dir = NULL;
	int *pending_indexes = NULL;
	int pattern_count = 0;
	long cpu_count;
	int max_workers;
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

	if (!ctx->parallel_segment_scan_enabled || ctx->resolved_segment_count == 0)
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
	meta_dir = fb_runtime_meta_dir();
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

	cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
	if (cpu_count <= 0)
		cpu_count = 4;
	max_workers = Min((int) cpu_count, 16);
	worker_count = Min(max_workers, pending_count);
	if (worker_count <= 0)
		worker_count = 1;
	chunk_size = (pending_count + worker_count - 1) / worker_count;

	for (i = 0; i < worker_count; i++)
	{
		int start = i * chunk_size;
		int end = Min(start + chunk_size, pending_count);

		worker_started[i] = false;

		tasks[i].segments = segments;
		tasks[i].indexes = pending_indexes + start;
		tasks[i].index_count = end - start;
		memcpy(tasks[i].patterns, patterns, sizeof(patterns));
		tasks[i].pattern_count = pattern_count;
		tasks[i].hit_map = ctx->segment_hit_map;
		strlcpy(tasks[i].meta_dir, meta_dir, sizeof(tasks[i].meta_dir));
		tasks[i].pattern_hash = pattern_hash;

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
	if (meta_dir != NULL)
		pfree(meta_dir);
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

/*
 * fb_open_file_at_path
 *    WAL helper.
 */

static int
fb_open_file_at_path(const char *path)
{
	int fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

#ifdef USE_POSIX_FADVISE
	if (fd >= 0)
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
fb_validate_segment_identity(FbWalSegmentEntry *entry, int wal_seg_size)
{
	PGAlignedXLogBlock buf;
	FILE *fp;
	size_t bytes_read;
	XLogLongPageHeader longhdr;
	XLogRecPtr expected_pageaddr;

	if (entry == NULL)
		return false;

	fp = AllocateFile(entry->path, PG_BINARY_R);
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", entry->path)));

	bytes_read = fread(buf.data, 1, XLOG_BLCKSZ, fp);
	FreeFile(fp);
	if (bytes_read != XLOG_BLCKSZ)
		return false;

	longhdr = (XLogLongPageHeader) buf.data;
	if (!IsValidWalSegSize(longhdr->xlp_seg_size))
		return false;
	if (longhdr->xlp_seg_size != wal_seg_size)
		return false;

	XLogSegNoOffsetToRecPtr(entry->segno, 0, wal_seg_size, expected_pageaddr);
	if (longhdr->std.xlp_pageaddr != expected_pageaddr)
		return false;
	if (longhdr->std.xlp_tli != entry->timeline_id)
		return false;

	return true;
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

	if (!fb_ckwal_restore_segment(timeline_id, segno, wal_seg_size,
								  entry->path, sizeof(entry->path)))
		return false;
	XLogFileName(fname, timeline_id, segno, wal_seg_size);
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
 * fb_convert_mismatched_pg_wal_entry
 *    WAL helper.
 */

static bool
fb_convert_mismatched_pg_wal_entry(FbWalScanContext *ctx,
								   FbWalSegmentEntry *entry,
								   FbWalSegmentEntry *converted)
{
	TimeLineID actual_tli;
	XLogSegNo actual_segno;
	char recovered_path[MAXPGPATH];
	char fname[MAXPGPATH];
	struct stat st;

	if (ctx == NULL || entry == NULL || converted == NULL)
		elog(ERROR, "ckwal conversion inputs must not be NULL");

	if ((FbWalSourceKind) entry->source_kind != FB_WAL_SOURCE_PG_WAL ||
		!entry->mismatch)
		return false;

	MemSet(converted, 0, sizeof(*converted));
	if (!fb_ckwal_convert_mismatched_segment(entry->path,
											 ctx->wal_seg_size,
											 &actual_tli,
											 &actual_segno,
											 recovered_path,
											 sizeof(recovered_path)))
		return false;

	if (stat(recovered_path, &st) != 0)
		return false;

	XLogFileName(fname, actual_tli, actual_segno, ctx->wal_seg_size);
	strlcpy(converted->name, fname, sizeof(converted->name));
	strlcpy(converted->path, recovered_path, sizeof(converted->path));
	converted->timeline_id = actual_tli;
	converted->segno = actual_segno;
	converted->bytes = st.st_size;
	converted->partial = (st.st_size < ctx->wal_seg_size);
	converted->valid = true;
	converted->mismatch = false;
	converted->ignored = false;
	converted->source_kind = (int) FB_WAL_SOURCE_CKWAL;
	ctx->ckwal_invoked = true;

	entry->ignored = true;
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
	FbWalSegmentEntry *selected = NULL;
	const char *archive_dir;
	char *pg_wal_dir = NULL;
	int candidate_count = 0;
	int candidate_capacity = 0;
	int selected_count = 0;
	int selected_capacity = 0;
	int highest_tli = 0;
	int i;
	XLogRecPtr last_segment_start;
	off_t last_segment_bytes;

	archive_dir = fb_get_effective_archive_dir();
	ctx->using_archive_dest = !fb_using_legacy_archive_dir();
	ctx->using_legacy_archive_dir = fb_using_legacy_archive_dir();
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
						ctx->using_archive_dest ? "pg_flashback.archive_dest" :
						"pg_flashback.archive_dir",
						archive_dir)));

	for (i = 0; i < candidate_count; i++)
	{
		if (candidates[i].timeline_id > (TimeLineID) highest_tli)
			highest_tli = candidates[i].timeline_id;
	}

	ctx->timeline_id = (TimeLineID) highest_tli;

	for (i = 0; i < candidate_count; i++)
	{
		TimeLineID timeline_id = 0;
		FbWalSegmentEntry converted_entry;

		if (candidates[i].timeline_id != ctx->timeline_id)
			continue;
		if (ctx->wal_seg_size == 0)
			ctx->wal_seg_size = fb_read_wal_segment_size_from_path(candidates[i].path);

		XLogFromFileName(candidates[i].name, &timeline_id, &candidates[i].segno,
						 ctx->wal_seg_size);
		candidates[i].partial = (candidates[i].bytes < ctx->wal_seg_size);
		if ((FbWalSourceKind) candidates[i].source_kind == FB_WAL_SOURCE_PG_WAL &&
			!fb_validate_segment_identity(&candidates[i], ctx->wal_seg_size))
		{
			candidates[i].valid = false;
			candidates[i].mismatch = true;
			if (fb_convert_mismatched_pg_wal_entry(ctx, &candidates[i],
												   &converted_entry))
				fb_append_segment_candidate(&candidates, &candidate_count,
											&candidate_capacity,
											&converted_entry);
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
		FbWalSegmentEntry recovered_entry;
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

		if (archive_entry != NULL)
			chosen = archive_entry;
		else if (ckwal_entry != NULL)
			chosen = ckwal_entry;
		else if (pg_entry != NULL)
			chosen = pg_entry;
		else if (invalid_pg_entry != NULL &&
				 fb_try_ckwal_segment(ctx, ctx->timeline_id, segno,
									 ctx->wal_seg_size, &recovered_entry))
			chosen = &recovered_entry;
				else if (invalid_pg_entry != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("WAL not complete: pg_wal contains recycled or mismatched segments and internal recovery could not reconstruct a usable segment"),
							 errdetail("segment=%s source=%s",
									   invalid_pg_entry->name,
									   invalid_pg_entry->path)));
		else
			chosen = NULL;

		if (chosen == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("WAL not complete: could not resolve segment for segno %llu",
							(unsigned long long) segno)));

		fb_append_selected_segment(&selected, &selected_count, &selected_capacity,
								   chosen, ctx);

		i = j;
	}

	if (selected_count == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no WAL segments remain on selected timeline %u", ctx->timeline_id)));

	for (i = 1; i < selected_count; i++)
	{
		XLogSegNo expected_segno = selected[i - 1].segno + 1;

		while (expected_segno < selected[i].segno)
		{
			FbWalSegmentEntry recovered_entry;

			if (!fb_try_ckwal_segment(ctx, ctx->timeline_id, expected_segno,
									  ctx->wal_seg_size, &recovered_entry))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("WAL not complete: missing segment between %s and %s",
								selected[i - 1].name, selected[i].name)));

			fb_append_selected_segment(&selected, &selected_count, &selected_capacity,
									   &recovered_entry, ctx);
			memmove(&selected[i + 1], &selected[i],
					sizeof(FbWalSegmentEntry) * (selected_count - i - 1));
			selected[i] = recovered_entry;
			expected_segno++;
			i++;
		}
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
		fb_progress_update_fraction(FB_PROGRESS_STAGE_BUILD_INDEX,
									 ctx->visited_segment_count,
									 ctx->progress_segment_total,
									 NULL);
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
	state->seg.ws_file = fb_open_file_at_path(entry->path);
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
			private->current_file = fb_open_file_at_path(entry->path);
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
	if (record->main_data_len > 0)
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
		if (dst->has_image)
		{
			dst->image = (char *) ptr;
			ptr += BLCKSZ;
		}
		if (dst->has_data && dst->data_len > 0)
		{
			dst->data = (char *) ptr;
			ptr += dst->data_len;
		}
	}

	fb_wal_set_record_status(index, record);
}

FbWalRecordCursor *
fb_wal_record_cursor_open(const FbWalRecordIndex *index, FbSpoolDirection direction)
{
	FbWalRecordCursor *cursor;

	cursor = palloc0(sizeof(*cursor));
	cursor->index = index;
	cursor->direction = direction;
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
	uint32 item_index = 0;

	if (cursor == NULL)
		return false;

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

	fb_wal_deserialize_record(cursor->index,
							  cursor->raw.data,
							  cursor->raw.len,
							  &cursor->current);
	if (record != NULL)
		*record = cursor->current;
	if (record_index != NULL)
		*record_index = item_index;
	return true;
}

void
fb_wal_record_cursor_close(FbWalRecordCursor *cursor)
{
	if (cursor == NULL)
		return;

	if (cursor->head_cursor != NULL)
		fb_spool_cursor_close(cursor->head_cursor);
	if (cursor->tail_cursor != NULL)
		fb_spool_cursor_close(cursor->tail_cursor);
	if (cursor->raw.data != NULL)
		pfree(cursor->raw.data);
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

static void
fb_wal_finalize_record_stats(FbWalRecordIndex *index)
{
	FbWalRecordCursor *cursor;
	FbRecordRef record;

	if (index == NULL || index->record_count == 0)
		return;

	fb_wal_reset_record_stats(index);
	cursor = fb_wal_record_cursor_open(index, FB_SPOOL_FORWARD);
	while (fb_wal_record_cursor_read(cursor, &record, NULL))
		fb_index_note_materialized_record(index, &record);
	fb_wal_record_cursor_close(cursor);
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
	FbCheckpointSidecarEntry *entries = NULL;
	FbCheckpointSidecarEntry best_entry;
	char	   *meta_dir;
	uint32		best_segment_index = 0;
	uint32		entry_count = 0;
	uint32		i;
	uint32		j;
	bool		found = false;

	if (ctx == NULL || ctx->resolved_segment_count == 0 || ctx->resolved_segments == NULL)
		return;

	fb_runtime_ensure_initialized();
	meta_dir = fb_runtime_meta_dir();
	segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	ctx->checkpoint_sidecar_entries = 0;

	for (i = 0; i < ctx->resolved_segment_count; i++)
	{
		if (!fb_checkpoint_sidecar_load(&segments[i], meta_dir, &entries, &entry_count))
			continue;

		for (j = 0; j < entry_count; j++)
		{
			if (entries[j].checkpoint_ts > ctx->target_ts)
				continue;

			ctx->checkpoint_sidecar_entries++;
			if (!found ||
				entries[j].checkpoint_ts > best_entry.checkpoint_ts ||
				(entries[j].checkpoint_ts == best_entry.checkpoint_ts &&
				 entries[j].redo_lsn >= best_entry.redo_lsn))
			{
				best_entry = entries[j];
				best_segment_index = i;
				found = true;
			}
		}

		if (entries != NULL)
		{
			pfree(entries);
			entries = NULL;
		}
	}

	pfree(meta_dir);

	if (!found)
		return;

	ctx->anchor_hint_found = true;
	ctx->anchor_found = true;
	ctx->anchor_checkpoint_lsn = best_entry.checkpoint_lsn;
	ctx->anchor_redo_lsn = best_entry.redo_lsn;
	ctx->anchor_time = best_entry.checkpoint_ts;
	ctx->anchor_hint_segment_index = best_segment_index;
	if (best_entry.checkpoint_lsn > ctx->start_lsn)
	{
		ctx->start_lsn = best_entry.checkpoint_lsn;
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
					 errmsg("fb WAL record spool session is not initialized")));
		*log_ptr = fb_spool_log_create(index->spool_session, label);
	}

	return *log_ptr;
}

static void
fb_index_append_record(FbWalRecordIndex *index,
					   FbSpoolLog **log_ptr,
					   const char *label,
					   const FbRecordRef *record)
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
	hdr.init_page = record->init_page;
	hdr.block_count = record->block_count;
	hdr.main_data_len = record->main_data_len;
	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *src = &record->blocks[block_index];
		FbSerializedRecordBlockRef *dst = &hdr.blocks[block_index];

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
	}

	appendBinaryStringInfo(&buf, (const char *) &hdr, sizeof(hdr));
	if (record->main_data_len > 0 && record->main_data != NULL)
		appendBinaryStringInfo(&buf, record->main_data, record->main_data_len);
	for (block_index = 0; block_index < record->block_count; block_index++)
	{
		const FbRecordBlockRef *block_ref = &record->blocks[block_index];

		if (block_ref->has_image && block_ref->image != NULL)
			appendBinaryStringInfo(&buf, block_ref->image, BLCKSZ);
		if (block_ref->has_data && block_ref->data_len > 0 && block_ref->data != NULL)
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
fb_wal_reset_record_stats(FbWalRecordIndex *index)
{
	if (index == NULL)
		return;

	index->kept_record_count = 0;
	index->target_record_count = 0;
	index->target_insert_count = 0;
	index->target_delete_count = 0;
	index->target_update_count = 0;
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

/*
 * fb_record_block_copy_image
 *    WAL helper.
 */

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
	block_ref->image = fb_copy_bytes(page, BLCKSZ);
}

/*
 * fb_fill_record_block_ref
 *    WAL helper.
 */

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
			block_ref->data = fb_copy_bytes(data, datalen);
			block_ref->data_len = datalen;
		}
	}

	fb_record_block_copy_image(block_ref, reader, index);
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

		fb_fill_record_block_ref(&record.blocks[block_count], reader, block_id, info, index);
		block_count++;
	}

	record.block_count = block_count;
	fb_index_append_record(index, state->payload_log, state->payload_label, &record);
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

		fb_fill_record_block_ref(&record.blocks[block_count], reader, block_id, info, index);
		block_count++;
	}

	record.block_count = block_count;
	fb_index_append_record(index, state->payload_log, state->payload_label, &record);
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
					if (unsafe_entry != NULL)
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

static void
fb_note_xact_status_for_touched(XLogReaderState *reader,
								HTAB *touched_xids,
								HTAB *unsafe_xids,
								FbWalRecordIndex *index,
								FbWalScanContext *ctx)
{
	uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;
	TransactionId xid = fb_record_xid(reader);
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

			if (state->collect_metadata)
				fb_mark_record_xids_touched(reader, touched_xids, ctx);

			switch (heap_code)
			{
				case XLOG_HEAP_INSERT:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_INSERT, state);
					break;
				case XLOG_HEAP_DELETE:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_DELETE, state);
					break;
				case XLOG_HEAP_UPDATE:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_UPDATE, state);
					break;
				case XLOG_HEAP_HOT_UPDATE:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_HOT_UPDATE, state);
					break;
				case XLOG_HEAP_CONFIRM:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_CONFIRM, state);
					break;
				case XLOG_HEAP_LOCK:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
						fb_copy_heap_record_ref(reader, info,
											 FB_WAL_RECORD_HEAP_LOCK, state);
					break;
				case XLOG_HEAP_INPLACE:
					if (state->collect_metadata)
						fb_index_note_record_metadata(index);
					if (state->capture_payload &&
						reader->ReadRecPtr >= index->anchor_redo_lsn)
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
			if (state->collect_metadata)
				fb_mark_record_xids_touched(reader, touched_xids, ctx);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn)
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_PRUNE, state);
		}
		else if (info_code == XLOG_HEAP2_VISIBLE &&
				 fb_record_touches_relation(reader, info))
		{
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->collect_metadata)
				fb_mark_record_xids_touched(reader, touched_xids, ctx);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn)
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
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn)
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_MULTI_INSERT, state);
		}
		else if (info_code == XLOG_HEAP2_LOCK_UPDATED &&
				 fb_record_touches_relation(reader, info))
		{
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->collect_metadata)
				fb_mark_record_xids_touched(reader, touched_xids, ctx);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn)
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
			if (state->collect_metadata)
				fb_index_note_record_metadata(index);
			if (state->capture_payload &&
				reader->ReadRecPtr >= index->anchor_redo_lsn)
				fb_copy_xlog_fpi_record_ref(reader, info,
											info_code == XLOG_FPI ?
											FB_WAL_RECORD_XLOG_FPI :
											FB_WAL_RECORD_XLOG_FPI_FOR_HINT,
											state);
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
		fb_note_xact_status_for_touched(reader, touched_xids, unsafe_xids,
										index, ctx);

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
	private.ctx = ctx;

#if PG_VERSION_NUM < 130000
	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_wal_read_page, &private);
#else
	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_get_effective_archive_dir(),
								XL_ROUTINE(.page_read = fb_wal_read_page,
										   .segment_open = fb_wal_open_segment,
										   .segment_close = fb_wal_close_segment),
								&private);
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
	ctx->parallel_segment_scan_enabled = fb_parallel_segment_scan_enabled();
	ctx->current_segment_may_hit = true;
	ctx->spool_session = spool_session;

	fb_collect_archive_segments(ctx);
	ctx->original_start_lsn = ctx->start_lsn;
	fb_maybe_seed_anchor_hint(ctx);
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
						  FbWalRecordIndex *index)
{
	FbWalIndexBuildState state;
	FbWalVisitWindow *windows = NULL;
	FbWalVisitWindow *payload_windows = NULL;
	uint32		window_count = 0;
	uint32		payload_window_count = 0;
	uint32		scan_segment_total = 0;
	uint32 i;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (index == NULL)
		elog(ERROR, "FbWalRecordIndex must not be NULL");

	fb_progress_enter_stage(FB_PROGRESS_STAGE_BUILD_INDEX, NULL);

	MemSet(index, 0, sizeof(*index));
	index->target_ts = ctx->target_ts;
	index->query_now_ts = ctx->query_now_ts;
	index->memory_limit_bytes = fb_get_memory_limit_bytes();
	index->xid_statuses = fb_create_xid_status_hash();
	index->spool_session = ctx->spool_session;

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.ctx = ctx;
	state.index = index;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();
	state.collect_metadata = true;
	state.capture_payload = false;
	state.tail_capture_allowed = false;
	state.payload_log = NULL;
	state.payload_label = NULL;

	fb_prepare_segment_prefilter(info, ctx);
	window_count = fb_build_prefilter_visit_windows(ctx, &windows);
	ctx->visited_segment_count = 0;
	if (window_count == 0)
		scan_segment_total = ctx->resolved_segment_count;
	else
	{
		for (i = 0; i < window_count; i++)
			scan_segment_total += windows[i].segment_count;
	}
	ctx->progress_segment_total = Max((uint32) 1, scan_segment_total * 2);
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

	if (!ctx->anchor_found)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: no checkpoint before target timestamp")));

	index->anchor_found = ctx->anchor_found;
	index->anchor_checkpoint_lsn = ctx->anchor_checkpoint_lsn;
	index->anchor_redo_lsn = ctx->anchor_redo_lsn;
	index->anchor_time = ctx->anchor_time;

	if (!ctx->unsafe)
	{
		payload_window_count =
			fb_build_materialize_visit_windows(ctx,
											   windows,
											   window_count,
											   ctx->anchor_redo_lsn,
											   index->tail_inline_payload ?
											   index->tail_cutover_lsn :
											   ctx->end_lsn,
											   &payload_windows);
		state.collect_metadata = false;
		state.capture_payload = true;
		state.tail_capture_allowed = false;
		state.payload_log = &index->record_log;
		state.payload_label = "wal-records";
		ctx->visited_segment_count = scan_segment_total;
		if (payload_window_count > 0)
		{
			for (i = 0; i < payload_window_count; i++)
				fb_wal_visit_window(ctx, &payload_windows[i], fb_index_record_visitor, &state);
			pfree(payload_windows);
		}

		fb_wal_finalize_record_stats(index);
	}

	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX, 100, NULL);

	index->unsafe = ctx->unsafe;
	index->unsafe_reason = ctx->unsafe_reason;
	index->unsafe_xid = ctx->unsafe_xid;
	index->unsafe_commit_ts = ctx->unsafe_commit_ts;
	index->unsafe_record_lsn = ctx->unsafe_record_lsn;
	index->unsafe_scope = ctx->unsafe_scope;
	index->unsafe_storage_op = ctx->unsafe_storage_op;

	if (windows != NULL)
		pfree(windows);
	hash_destroy(state.touched_xids);
	hash_destroy(state.unsafe_xids);
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
