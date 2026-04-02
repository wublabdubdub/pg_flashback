/*
 * fb_summary.c
 *    WAL segment summary sidecars and debug helpers.
 */

#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/standbydefs.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "fb_ckwal.h"
#include "fb_catalog.h"
#include "fb_compat.h"
#include "fb_guc.h"
#include "fb_runtime.h"
#include "fb_summary.h"
#include "fb_wal.h"

PG_FUNCTION_INFO_V1(fb_summary_build_available_debug);
PG_FUNCTION_INFO_V1(fb_summary_block_anchor_debug);
PG_FUNCTION_INFO_V1(fb_summary_meta_stats_debug);

#define FB_SUMMARY_MAGIC ((uint32) 0x4642534d)
#define FB_SUMMARY_VERSION 6
#define FB_SUMMARY_LOCATOR_BLOOM_BYTES 64
#define FB_SUMMARY_RELTAG_BLOOM_BYTES 64
#define FB_SUMMARY_SECTION_COUNT 6
#define FB_SUMMARY_SPAN_MERGE_GAP XLOG_BLCKSZ

typedef enum FbSummarySourceKind
{
	FB_SUMMARY_SOURCE_PG_WAL = 1,
	FB_SUMMARY_SOURCE_ARCHIVE_DEST,
	FB_SUMMARY_SOURCE_ARCHIVE_DIR_LEGACY,
	FB_SUMMARY_SOURCE_CKWAL
} FbSummarySourceKind;

typedef struct FbSummarySegmentEntry
{
	char name[25];
	char path[MAXPGPATH];
	TimeLineID timeline_id;
	XLogSegNo segno;
	off_t bytes;
	bool valid;
	bool mismatch;
	bool ignored;
	int source_kind;
} FbSummarySegmentEntry;

typedef struct FbSummaryFileHeader
{
	uint32 magic;
	uint16 version;
	uint16 source_kind;
	uint64 file_identity_hash;
	TimeLineID timeline_id;
	uint32 wal_seg_size;
	XLogSegNo segno;
	TimestampTz built_at;
	TimestampTz oldest_xact_ts;
	TimestampTz newest_xact_ts;
	uint32 flags;
	uint32 relation_entry_count;
	uint32 relation_span_count;
	uint32 xid_outcome_count;
	uint32 touched_xid_count;
	uint32 unsafe_fact_count;
	uint32 block_anchor_count;
	uint32 section_count;
	struct
	{
		uint32 kind;
		uint32 offset;
		uint32 length;
		uint32 item_count;
	} sections[FB_SUMMARY_SECTION_COUNT];
	unsigned char locator_bloom[FB_SUMMARY_LOCATOR_BLOOM_BYTES];
	unsigned char reltag_bloom[FB_SUMMARY_RELTAG_BLOOM_BYTES];
} FbSummaryFileHeader;

typedef enum FbSummarySectionKind
{
	FB_SUMMARY_SECTION_RELATION_ENTRIES = 1,
	FB_SUMMARY_SECTION_RELATION_SPANS,
	FB_SUMMARY_SECTION_XID_OUTCOMES,
	FB_SUMMARY_SECTION_TOUCHED_XIDS,
	FB_SUMMARY_SECTION_UNSAFE_FACTS,
	FB_SUMMARY_SECTION_BLOCK_ANCHORS
} FbSummarySectionKind;

typedef enum FbSummaryUnsafeMatchKind
{
	FB_SUMMARY_UNSAFE_MATCH_LOCATOR = 1,
	FB_SUMMARY_UNSAFE_MATCH_RELTAG = 2
} FbSummaryUnsafeMatchKind;

typedef struct FbSummaryRelTag
{
	Oid db_oid;
	Oid rel_oid;
} FbSummaryRelTag;

typedef enum FbSummaryRelationKeyKind
{
	FB_SUMMARY_RELKEY_LOCATOR = 1,
	FB_SUMMARY_RELKEY_RELTAG = 2
} FbSummaryRelationKeyKind;

typedef struct FbSummaryDiskRelationEntry
{
	uint16 kind;
	uint16 slot;
	uint32 first_span;
	uint32 span_count;
	uint32 first_touched_xid;
	uint32 touched_xid_count;
	uint32 first_unsafe_fact;
	uint32 unsafe_fact_count;
	uint32 first_block_anchor;
	uint32 block_anchor_count;
	RelFileLocator locator;
	Oid db_oid;
	Oid rel_oid;
} FbSummaryDiskRelationEntry;

typedef struct FbSummaryDiskRelationSpan
{
	uint16 slot;
	uint16 flags;
	uint32 reserved;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
} FbSummaryDiskRelationSpan;

typedef struct FbSummaryDiskXidOutcome
{
	TransactionId xid;
	uint8 status;
	uint8 reserved[3];
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
} FbSummaryDiskXidOutcome;

typedef struct FbSummaryDiskTouchedXid
{
	uint16 slot;
	uint16 reserved;
	TransactionId xid;
} FbSummaryDiskTouchedXid;

typedef struct FbSummaryDiskUnsafeFact
{
	uint16 slot;
	uint8 reason;
	uint8 scope;
	uint8 storage_op;
	uint8 match_kind;
	uint16 reserved;
	TransactionId xid;
	XLogRecPtr record_lsn;
	RelFileLocator locator;
	Oid db_oid;
	Oid rel_oid;
} FbSummaryDiskUnsafeFact;

typedef struct FbSummaryDiskBlockAnchor
{
	uint16 slot;
	uint16 flags;
	uint32 blkno;
	XLogRecPtr anchor_lsn;
} FbSummaryDiskBlockAnchor;

typedef struct FbSummaryLocatorKey
{
	RelFileLocator locator;
} FbSummaryLocatorKey;

typedef struct FbSummaryRelTagKey
{
	Oid db_oid;
	Oid rel_oid;
} FbSummaryRelTagKey;

typedef struct FbSummaryLocatorHashEntry
{
	FbSummaryLocatorKey key;
	uint16 slot;
} FbSummaryLocatorHashEntry;

typedef struct FbSummaryRelTagHashEntry
{
	FbSummaryRelTagKey key;
	uint16 slot;
} FbSummaryRelTagHashEntry;

typedef struct FbSummaryTouchedXidHashEntry
{
	TransactionId xid;
} FbSummaryTouchedXidHashEntry;

typedef struct FbSummaryUnsafeFactHashEntry
{
	FbSummaryUnsafeFact fact;
} FbSummaryUnsafeFactHashEntry;

typedef struct FbSummaryBlockAnchorHashEntry
{
	BlockNumber blkno;
	uint32 index;
} FbSummaryBlockAnchorHashEntry;

typedef struct FbSummaryBuildRelationEntry
{
	uint16 kind;
	uint16 slot;
	RelFileLocator locator;
	Oid db_oid;
	Oid rel_oid;
	HTAB *touched_xid_set;
	HTAB *unsafe_fact_set;
	HTAB *block_anchor_map;
	FbSummarySpan *spans;
	uint32 span_count;
	uint32 span_capacity;
	TransactionId *touched_xids;
	uint32 touched_xid_count;
	uint32 touched_xid_capacity;
	FbSummaryUnsafeFact *unsafe_facts;
	uint32 unsafe_fact_count;
	uint32 unsafe_fact_capacity;
	FbSummaryBlockAnchor *block_anchors;
	uint32 block_anchor_count;
	uint32 block_anchor_capacity;
} FbSummaryBuildRelationEntry;

typedef struct FbSummaryBuildState
{
	FbSummaryFileHeader file;
	HTAB *locator_hash;
	HTAB *reltag_hash;
	HTAB *xid_outcomes;
	FbSummaryBuildRelationEntry *relations;
	uint32 relation_count;
	uint32 relation_capacity;
} FbSummaryBuildState;

typedef struct FbSummaryBuildXidOutcome
{
	TransactionId xid;
	FbSummaryDiskXidOutcome outcome;
} FbSummaryBuildXidOutcome;

typedef struct FbSummaryReaderPrivate
{
	const FbSummarySegmentEntry *segment;
	const FbSummarySegmentEntry *next_segment;
	XLogRecPtr endptr;
	bool endptr_reached;
	TimeLineID timeline_id;
#if PG_VERSION_NUM < 130000
	int current_file;
	bool current_file_open;
	XLogSegNo current_file_segno;
	bool current_file_segno_valid;
#endif
} FbSummaryReaderPrivate;

typedef struct FbSummaryMetaCounts
{
	uint32 summary_files;
	uint32 prefilter_files;
} FbSummaryMetaCounts;

typedef struct FbSummaryQueryCacheEntry
{
	uint64 file_identity_hash;
	bool loaded;
	bool valid;
	FbSummaryFileHeader file;
	FbSummaryDiskRelationEntry *relations;
	FbSummaryDiskRelationSpan *spans;
	FbSummaryDiskXidOutcome *xid_outcomes;
	FbSummaryDiskTouchedXid *touched_xids;
	FbSummaryDiskUnsafeFact *unsafe_facts;
	FbSummaryDiskBlockAnchor *block_anchors;
} FbSummaryQueryCacheEntry;

struct FbSummaryQueryCache
{
	HTAB *entries;
	MemoryContext mcxt;
};

static uint64 fb_summary_hash_bytes(uint64 seed, const void *data, Size len);
static TransactionId fb_summary_record_xid(XLogReaderState *reader);
static uint64 fb_summary_file_identity_hash(const char *path, const struct stat *st);
static bool fb_summary_file_path_into(char *summary_path,
									  Size summary_path_len,
									  const char *path,
									  const struct stat *st);
static char *fb_summary_file_path(const char *path, const struct stat *st);
static bool fb_summary_load_file(const char *path,
								 off_t bytes,
								 TimeLineID timeline_id,
								 XLogSegNo segno,
								 int wal_seg_size,
								 int source_kind,
								 FbSummaryFileHeader *file);
static bool fb_summary_open_validated_file(const char *path,
										   off_t bytes,
										   TimeLineID timeline_id,
										   XLogSegNo segno,
										   int wal_seg_size,
										   int source_kind,
										   FbSummaryFileHeader *file,
										   int *fd_out);
static void fb_summary_bloom_add(unsigned char *bits, Size bits_len,
								 const void *data, Size len);
static bool fb_summary_bloom_maybe_contains(const unsigned char *bits, Size bits_len,
											 const void *data, Size len);
static HTAB *fb_summary_create_locator_hash(void);
static HTAB *fb_summary_create_reltag_hash(void);
static HTAB *fb_summary_create_xid_outcome_hash(void);
static HTAB *fb_summary_create_touched_xid_hash(void);
static HTAB *fb_summary_create_unsafe_fact_hash(void);
static HTAB *fb_summary_create_block_anchor_hash(void);
static HTAB *fb_summary_create_query_cache_hash(MemoryContext mcxt);
static FbSummaryBuildRelationEntry *fb_summary_get_or_add_locator_relation(FbSummaryBuildState *state,
																		   const RelFileLocator *locator);
static FbSummaryBuildRelationEntry *fb_summary_get_or_add_reltag_relation(FbSummaryBuildState *state,
																		  Oid db_oid,
																		  Oid rel_oid);
static void fb_summary_relation_append_span(FbSummaryBuildRelationEntry *entry,
											XLogRecPtr start_lsn,
											XLogRecPtr end_lsn,
											uint16 flags);
static void fb_summary_relation_append_touched_xid(FbSummaryBuildRelationEntry *entry,
													TransactionId xid);
static void fb_summary_relation_append_block_anchor(FbSummaryBuildRelationEntry *entry,
													const RelFileLocator *locator,
													ForkNumber forknum,
													BlockNumber blkno,
													XLogRecPtr anchor_lsn,
													uint16 flags);
static void fb_summary_relation_append_unsafe_fact_locator(FbSummaryBuildRelationEntry *entry,
														   FbWalUnsafeReason reason,
														   FbWalUnsafeScope scope,
														   FbWalStorageChangeOp storage_op,
														   TransactionId xid,
														   XLogRecPtr record_lsn,
														   const RelFileLocator *locator);
static void fb_summary_relation_append_unsafe_fact_reltag(FbSummaryBuildRelationEntry *entry,
														  FbWalUnsafeReason reason,
														  FbWalUnsafeScope scope,
														  FbWalStorageChangeOp storage_op,
														  TransactionId xid,
														  XLogRecPtr record_lsn,
														  Oid db_oid,
														  Oid rel_oid);
static void fb_summary_add_locator(FbSummaryBuildState *state,
								   const RelFileLocator *locator);
static void fb_summary_add_reltag(FbSummaryBuildState *state,
								  Oid db_oid,
								  Oid rel_oid);
static void fb_summary_note_xid_outcome(FbSummaryBuildState *state,
										TransactionId xid,
										FbWalXidStatus status,
										TimestampTz commit_ts,
										XLogRecPtr commit_lsn);
static void fb_summary_note_xact_timestamp(FbSummaryBuildState *state,
										   TimestampTz ts);
static bool fb_summary_record_locator_from_smgr(XLogReaderState *reader,
												 RelFileLocator *locator_out);
static bool fb_summary_record_block_is_anchor(XLogReaderState *reader,
											  int block_id,
											  uint16 *flags_out);
static void fb_summary_note_record(XLogReaderState *reader,
								   FbSummaryBuildState *state);
static int fb_summary_open_file_at_path(const char *path);
static int fb_summary_read_wal_segment_size_from_path(const char *path);
static bool fb_summary_parse_timeline_id(const char *name, TimeLineID *timeline_id);
static int fb_summary_segment_name_cmp(const void *lhs, const void *rhs);
static void fb_summary_append_segment_entry(FbSummarySegmentEntry **segments,
											int *segment_count,
											int *segment_capacity,
											const char *directory,
											const char *name,
											TimeLineID timeline_id,
											int source_kind,
											off_t bytes);
static void fb_summary_collect_segments_from_directory(const char *directory,
													   int source_kind,
													   FbSummarySegmentEntry **segments,
													   int *segment_count,
													   int *segment_capacity);
static bool fb_summary_read_segment_identity_path(const char *path,
												  TimeLineID *timeline_id,
												  XLogSegNo *segno,
												  int wal_seg_size);
static bool fb_summary_validate_segment_identity(FbSummarySegmentEntry *entry,
												 int wal_seg_size);
static bool fb_summary_build_file_for_segment(const FbSummarySegmentEntry *entry,
											  const FbSummarySegmentEntry *next_entry,
											  int wal_seg_size);
static bool fb_summary_cache_get_or_load(const char *path,
										 off_t bytes,
										 TimeLineID timeline_id,
										 XLogSegNo segno,
										 int wal_seg_size,
										 int source_kind,
										 FbSummaryQueryCache *cache,
										 FbSummaryQueryCacheEntry **entry_out);
static void fb_summary_cache_load_sections(FbSummaryQueryCache *cache,
										   FbSummaryQueryCacheEntry *entry,
										   int fd);
static bool fb_summary_find_section(const FbSummaryFileHeader *file,
									uint32 kind,
									uint32 *offset_out,
									uint32 *length_out,
									uint32 *item_count_out);
static bool fb_summary_relation_entry_matches_info(const FbSummaryDiskRelationEntry *entry,
												   const FbRelationInfo *info);
static FbSummaryBuildRelationEntry *fb_summary_find_relation_by_slot(FbSummaryBuildState *state,
																	 uint16 slot);
static void fb_summary_collect_meta_counts(FbSummaryMetaCounts *counts);
static int fb_summary_collect_selected_segments(FbSummarySegmentEntry **selected_out,
												 int *wal_seg_size_out);
#if PG_VERSION_NUM >= 130000
static void fb_summary_open_segment(XLogReaderState *state, XLogSegNo next_segno,
									TimeLineID *timeline_id);
static void fb_summary_close_segment(XLogReaderState *state);
#endif
static int fb_summary_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr,
								int req_len, XLogRecPtr target_rec_ptr, char *read_buf
#if PG_VERSION_NUM < 130000
								, TimeLineID *page_tli
#endif
	);
static bool fb_summary_visit_record(XLogReaderState *reader, void *arg);

static uint64
fb_summary_hash_bytes(uint64 seed, const void *data, Size len)
{
	const unsigned char *ptr = (const unsigned char *) data;
	Size i;

	for (i = 0; i < len; i++)
	{
		seed ^= ptr[i];
		seed *= UINT64CONST(1099511628211);
	}

	return seed;
}

static TransactionId
fb_summary_record_xid(XLogReaderState *reader)
{
	TransactionId xid = FB_XLOGREC_GET_TOP_XID(reader);

	if (TransactionIdIsValid(xid))
		return xid;

	return XLogRecGetXid(reader);
}

static uint64
fb_summary_file_identity_hash(const char *path, const struct stat *st)
{
	uint64 hash = UINT64CONST(1469598103934665603);
	off_t bytes;
	time_t mtime_sec;
	long mtime_nsec = 0;
	time_t ctime_sec;
	long ctime_nsec = 0;

	if (st == NULL)
		return hash;

	bytes = st->st_size;
	mtime_sec = st->st_mtime;
	ctime_sec = st->st_ctime;
#if defined(__APPLE__)
	mtime_nsec = st->st_mtimespec.tv_nsec;
	ctime_nsec = st->st_ctimespec.tv_nsec;
#else
	mtime_nsec = st->st_mtim.tv_nsec;
	ctime_nsec = st->st_ctim.tv_nsec;
#endif
	hash = fb_summary_hash_bytes(hash, path, strlen(path));
	hash = fb_summary_hash_bytes(hash, &bytes, sizeof(bytes));
	hash = fb_summary_hash_bytes(hash, &mtime_sec, sizeof(mtime_sec));
	hash = fb_summary_hash_bytes(hash, &mtime_nsec, sizeof(mtime_nsec));
	hash = fb_summary_hash_bytes(hash, &ctime_sec, sizeof(ctime_sec));
	hash = fb_summary_hash_bytes(hash, &ctime_nsec, sizeof(ctime_nsec));
	return hash;
}

static bool
fb_summary_file_path_into(char *summary_path,
						  Size summary_path_len,
						  const char *path,
						  const struct stat *st)
{
	char summary_dir[MAXPGPATH];
	uint64 identity_hash;
	int written;

	if (summary_path == NULL || summary_path_len == 0 || path == NULL || st == NULL)
		return false;

	written = snprintf(summary_dir, sizeof(summary_dir),
					   "%s/pg_flashback/meta/summary", DataDir);
	if (written < 0 || written >= (int) sizeof(summary_dir))
		return false;

	identity_hash = fb_summary_file_identity_hash(path, st);
	written = snprintf(summary_path, summary_path_len,
					   "%s/summary-%016llx.meta",
					   summary_dir,
					   (unsigned long long) identity_hash);
	if (written < 0 || written >= (int) summary_path_len)
		return false;

	return true;
}

static char *
fb_summary_file_path(const char *path, const struct stat *st)
{
	char summary_path[MAXPGPATH];

	if (!fb_summary_file_path_into(summary_path, sizeof(summary_path), path, st))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("summary file path exceeds MAXPGPATH"),
				 errdetail("path=%s", path)));

	return pstrdup(summary_path);
}

static bool
fb_summary_open_validated_file(const char *path,
							   off_t bytes,
							   TimeLineID timeline_id,
							   XLogSegNo segno,
							   int wal_seg_size,
							   int source_kind,
							   FbSummaryFileHeader *file,
							   int *fd_out)
{
	char summary_path[MAXPGPATH];
	struct stat st;
	uint64 file_identity_hash;
	int fd;
	ssize_t nread;

	if (file != NULL)
		MemSet(file, 0, sizeof(*file));
	if (fd_out != NULL)
		*fd_out = -1;

	if (path == NULL || file == NULL || stat(path, &st) != 0)
		return false;

	file_identity_hash = fb_summary_file_identity_hash(path, &st);
	if (!fb_summary_file_path_into(summary_path, sizeof(summary_path), path, &st))
		return false;
	fd = open(summary_path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;

	nread = read(fd, file, sizeof(*file));
	if (nread != sizeof(*file))
	{
		close(fd);
		return false;
	}

	if (file->magic != FB_SUMMARY_MAGIC ||
		file->version != FB_SUMMARY_VERSION ||
		file->file_identity_hash != file_identity_hash ||
		file->timeline_id != timeline_id ||
		file->segno != segno ||
		file->wal_seg_size != (uint32) wal_seg_size ||
		file->source_kind != (uint16) source_kind)
	{
		close(fd);
		return false;
	}

	if (fd_out != NULL)
		*fd_out = fd;
	else
		close(fd);

	return true;
}

static bool
fb_summary_load_file(const char *path,
					 off_t bytes,
					 TimeLineID timeline_id,
					 XLogSegNo segno,
					 int wal_seg_size,
					 int source_kind,
					 FbSummaryFileHeader *file)
{
	int fd = -1;
	bool loaded;

	loaded = fb_summary_open_validated_file(path,
											 bytes,
											 timeline_id,
											 segno,
											 wal_seg_size,
											 source_kind,
											 file,
											 &fd);
	if (fd >= 0)
		close(fd);
	return loaded;
}

static int
fb_summary_collect_selected_segments(FbSummarySegmentEntry **selected_out,
									 int *wal_seg_size_out)
{
	FbSummarySegmentEntry *candidates = NULL;
	FbSummarySegmentEntry *selected = NULL;
	int candidate_count = 0;
	int candidate_capacity = 0;
	int selected_count = 0;
	int selected_capacity = 0;
	char *archive_dir;
	char *pg_wal_dir;
	char *recovered_dir;
	FbArchiveDirSource archive_source;
	const char *archive_setting_name;
	TimeLineID highest_tli = 0;
	int wal_seg_size = 0;
	int i;

	if (selected_out != NULL)
		*selected_out = NULL;
	if (wal_seg_size_out != NULL)
		*wal_seg_size_out = 0;

	fb_runtime_ensure_initialized();
	archive_dir = fb_try_resolve_archive_dir(&archive_source, &archive_setting_name);
	pg_wal_dir = fb_get_pg_wal_dir();
	recovered_dir = fb_runtime_recovered_wal_dir();

	fb_summary_collect_segments_from_directory(archive_dir,
											  archive_source == FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR ?
											  FB_SUMMARY_SOURCE_ARCHIVE_DIR_LEGACY :
											  FB_SUMMARY_SOURCE_ARCHIVE_DEST,
											  &candidates, &candidate_count, &candidate_capacity);
	fb_summary_collect_segments_from_directory(recovered_dir, FB_SUMMARY_SOURCE_CKWAL,
											  &candidates, &candidate_count, &candidate_capacity);
	fb_summary_collect_segments_from_directory(pg_wal_dir, FB_SUMMARY_SOURCE_PG_WAL,
											  &candidates, &candidate_count, &candidate_capacity);

	if (candidate_count == 0)
		goto done;

	for (i = 0; i < candidate_count; i++)
	{
		if (candidates[i].timeline_id > highest_tli)
			highest_tli = candidates[i].timeline_id;
	}

	for (i = 0; i < candidate_count; i++)
	{
		TimeLineID parsed_tli = 0;

		if (candidates[i].timeline_id != highest_tli)
			candidates[i].ignored = true;
		if (wal_seg_size == 0 && !candidates[i].ignored)
			wal_seg_size = fb_summary_read_wal_segment_size_from_path(candidates[i].path);
		if (!candidates[i].ignored)
			XLogFromFileName(candidates[i].name, &parsed_tli, &candidates[i].segno,
							 wal_seg_size);
		if (candidates[i].source_kind == FB_SUMMARY_SOURCE_PG_WAL &&
			!candidates[i].ignored &&
			!fb_summary_validate_segment_identity(&candidates[i], wal_seg_size))
		{
			candidates[i].valid = false;
			candidates[i].mismatch = true;
		}
	}

	qsort(candidates, candidate_count, sizeof(FbSummarySegmentEntry),
		  fb_summary_segment_name_cmp);

	for (i = 0; i < candidate_count;)
	{
		FbSummarySegmentEntry *chosen = NULL;
		FbSummarySegmentEntry *entry = &candidates[i];
		int group_end;
		int j;

		if (entry->ignored)
		{
			i++;
			continue;
		}

		group_end = i + 1;
		while (group_end < candidate_count &&
			   candidates[group_end].segno == entry->segno &&
			   candidates[group_end].timeline_id == entry->timeline_id)
			group_end++;

		for (j = i; j < group_end; j++)
		{
			if (candidates[j].ignored)
				continue;
			if (candidates[j].source_kind == FB_SUMMARY_SOURCE_ARCHIVE_DEST ||
				candidates[j].source_kind == FB_SUMMARY_SOURCE_ARCHIVE_DIR_LEGACY)
			{
				chosen = &candidates[j];
				break;
			}
			if (candidates[j].source_kind == FB_SUMMARY_SOURCE_CKWAL)
				chosen = &candidates[j];
			else if (candidates[j].source_kind == FB_SUMMARY_SOURCE_PG_WAL &&
					 candidates[j].valid && chosen == NULL)
				chosen = &candidates[j];
		}

		if (chosen != NULL)
		{
			if (selected_count == selected_capacity)
			{
				selected_capacity = (selected_capacity == 0) ? 32 : (selected_capacity * 2);
				if (selected == NULL)
					selected = palloc(sizeof(FbSummarySegmentEntry) * selected_capacity);
				else
					selected = repalloc(selected,
									   sizeof(FbSummarySegmentEntry) * selected_capacity);
			}
			selected[selected_count++] = *chosen;
		}

		i = group_end;
	}

done:
	pfree(archive_dir);
	pfree(pg_wal_dir);
	pfree(recovered_dir);
	if (candidates != NULL)
		pfree(candidates);

	if (selected_out != NULL)
		*selected_out = selected;
	else if (selected != NULL)
		pfree(selected);
	if (wal_seg_size_out != NULL)
		*wal_seg_size_out = wal_seg_size;
	return selected_count;
}

static HTAB *
fb_summary_create_locator_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbSummaryLocatorKey);
	ctl.entrysize = sizeof(FbSummaryLocatorHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	return hash_create("fb summary locator hash", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_summary_create_reltag_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbSummaryRelTagKey);
	ctl.entrysize = sizeof(FbSummaryRelTagHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	return hash_create("fb summary reltag hash", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_summary_create_xid_outcome_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(FbSummaryBuildXidOutcome);
	ctl.hcxt = CurrentMemoryContext;
	return hash_create("fb summary xid outcomes", 128, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_summary_create_touched_xid_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(FbSummaryTouchedXidHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	return hash_create("fb summary touched xids", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_summary_create_unsafe_fact_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(FbSummaryUnsafeFact);
	ctl.entrysize = sizeof(FbSummaryUnsafeFactHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	return hash_create("fb summary unsafe facts", 32, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_summary_create_block_anchor_hash(void)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BlockNumber);
	ctl.entrysize = sizeof(FbSummaryBlockAnchorHashEntry);
	ctl.hcxt = CurrentMemoryContext;
	return hash_create("fb summary block anchors", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static HTAB *
fb_summary_create_query_cache_hash(MemoryContext mcxt)
{
	HASHCTL ctl;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(FbSummaryQueryCacheEntry);
	ctl.hcxt = mcxt;
	return hash_create("fb summary query cache", 64, &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static FbSummaryBuildRelationEntry *
fb_summary_find_relation_by_slot(FbSummaryBuildState *state, uint16 slot)
{
	if (state == NULL || state->relations == NULL)
		return NULL;
	if (slot >= state->relation_count)
		return NULL;
	return &state->relations[slot];
}

static FbSummaryBuildRelationEntry *
fb_summary_append_relation_entry(FbSummaryBuildState *state)
{
	FbSummaryBuildRelationEntry *entry;

	if (state == NULL)
		return NULL;

	if (state->relation_count == state->relation_capacity)
	{
		uint32 old_capacity = state->relation_capacity;

		state->relation_capacity = (state->relation_capacity == 0) ? 16 : state->relation_capacity * 2;
		if (state->relations == NULL)
			state->relations = palloc0(sizeof(FbSummaryBuildRelationEntry) * state->relation_capacity);
		else
		{
			state->relations = repalloc(state->relations,
										sizeof(FbSummaryBuildRelationEntry) * state->relation_capacity);
			MemSet(state->relations + old_capacity, 0,
				   sizeof(FbSummaryBuildRelationEntry) * (state->relation_capacity - old_capacity));
		}
	}

	entry = &state->relations[state->relation_count];
	MemSet(entry, 0, sizeof(*entry));
	entry->slot = (uint16) state->relation_count;
	state->relation_count++;
	return entry;
}

static FbSummaryBuildRelationEntry *
fb_summary_get_or_add_locator_relation(FbSummaryBuildState *state,
									   const RelFileLocator *locator)
{
	FbSummaryLocatorHashEntry *hash_entry;
	FbSummaryBuildRelationEntry *entry;
	bool found = false;

	if (state == NULL || locator == NULL)
		return NULL;
	if (state->locator_hash == NULL)
		state->locator_hash = fb_summary_create_locator_hash();

	hash_entry = (FbSummaryLocatorHashEntry *) hash_search(state->locator_hash,
														   locator,
														   HASH_ENTER,
														   &found);
	if (found)
		return fb_summary_find_relation_by_slot(state, hash_entry->slot);

	entry = fb_summary_append_relation_entry(state);
	entry->kind = FB_SUMMARY_RELKEY_LOCATOR;
	entry->locator = *locator;
	hash_entry->key.locator = *locator;
	hash_entry->slot = entry->slot;
	return entry;
}

static FbSummaryBuildRelationEntry *
fb_summary_get_or_add_reltag_relation(FbSummaryBuildState *state,
									  Oid db_oid,
									  Oid rel_oid)
{
	FbSummaryRelTagHashEntry *hash_entry;
	FbSummaryRelTagKey key;
	FbSummaryBuildRelationEntry *entry;
	bool found = false;

	if (state == NULL || !OidIsValid(db_oid) || !OidIsValid(rel_oid))
		return NULL;
	if (state->reltag_hash == NULL)
		state->reltag_hash = fb_summary_create_reltag_hash();

	key.db_oid = db_oid;
	key.rel_oid = rel_oid;
	hash_entry = (FbSummaryRelTagHashEntry *) hash_search(state->reltag_hash,
														  &key,
														  HASH_ENTER,
														  &found);
	if (found)
		return fb_summary_find_relation_by_slot(state, hash_entry->slot);

	entry = fb_summary_append_relation_entry(state);
	entry->kind = FB_SUMMARY_RELKEY_RELTAG;
	entry->db_oid = db_oid;
	entry->rel_oid = rel_oid;
	hash_entry->key = key;
	hash_entry->slot = entry->slot;
	return entry;
}

static void
fb_summary_relation_append_span(FbSummaryBuildRelationEntry *entry,
								XLogRecPtr start_lsn,
								XLogRecPtr end_lsn,
								uint16 flags)
{
	FbSummarySpan *span;

	if (entry == NULL || XLogRecPtrIsInvalid(start_lsn) || XLogRecPtrIsInvalid(end_lsn) ||
		start_lsn >= end_lsn)
		return;

	if (entry->span_count > 0)
	{
		span = &entry->spans[entry->span_count - 1];
		if (start_lsn <= span->end_lsn + FB_SUMMARY_SPAN_MERGE_GAP)
		{
			if (end_lsn > span->end_lsn)
				span->end_lsn = end_lsn;
			span->flags |= flags;
			return;
		}
	}

	if (entry->span_count == entry->span_capacity)
	{
		entry->span_capacity = (entry->span_capacity == 0) ? 8 : entry->span_capacity * 2;
		if (entry->spans == NULL)
			entry->spans = palloc0(sizeof(FbSummarySpan) * entry->span_capacity);
		else
			entry->spans = repalloc(entry->spans,
									sizeof(FbSummarySpan) * entry->span_capacity);
	}

	span = &entry->spans[entry->span_count++];
	span->start_lsn = start_lsn;
	span->end_lsn = end_lsn;
	span->flags = flags;
}

static void
fb_summary_relation_append_touched_xid(FbSummaryBuildRelationEntry *entry,
									   TransactionId xid)
{
	bool found = false;

	if (entry == NULL || !TransactionIdIsValid(xid))
		return;
	if (entry->touched_xid_set == NULL)
		entry->touched_xid_set = fb_summary_create_touched_xid_hash();
	hash_search(entry->touched_xid_set, &xid, HASH_ENTER, &found);
	if (found)
		return;

	if (entry->touched_xid_count == entry->touched_xid_capacity)
	{
		entry->touched_xid_capacity =
			(entry->touched_xid_capacity == 0) ? 8 : entry->touched_xid_capacity * 2;
		if (entry->touched_xids == NULL)
			entry->touched_xids =
				palloc(sizeof(*entry->touched_xids) * entry->touched_xid_capacity);
		else
			entry->touched_xids =
				repalloc(entry->touched_xids,
						 sizeof(*entry->touched_xids) * entry->touched_xid_capacity);
	}

	entry->touched_xids[entry->touched_xid_count++] = xid;
}

static void
fb_summary_relation_append_block_anchor(FbSummaryBuildRelationEntry *entry,
										const RelFileLocator *locator,
										ForkNumber forknum,
										BlockNumber blkno,
										XLogRecPtr anchor_lsn,
										uint16 flags)
{
	FbSummaryBlockAnchor *anchor;
	FbSummaryBlockAnchorHashEntry *hash_entry;
	bool found = false;

	if (entry == NULL || locator == NULL || XLogRecPtrIsInvalid(anchor_lsn))
		return;
	if (entry->block_anchor_map == NULL)
		entry->block_anchor_map = fb_summary_create_block_anchor_hash();
	hash_entry = (FbSummaryBlockAnchorHashEntry *)
		hash_search(entry->block_anchor_map, &blkno, HASH_ENTER, &found);
	if (found)
	{
		anchor = &entry->block_anchors[hash_entry->index];
		if (anchor_lsn > anchor->anchor_lsn)
		{
			anchor->anchor_lsn = anchor_lsn;
			anchor->flags = flags;
		}
		else if (anchor_lsn == anchor->anchor_lsn)
			anchor->flags |= flags;
		return;
	}

	if (entry->block_anchor_count == entry->block_anchor_capacity)
	{
		entry->block_anchor_capacity =
			(entry->block_anchor_capacity == 0) ? 8 : entry->block_anchor_capacity * 2;
		if (entry->block_anchors == NULL)
			entry->block_anchors =
				palloc0(sizeof(*entry->block_anchors) * entry->block_anchor_capacity);
		else
			entry->block_anchors =
				repalloc(entry->block_anchors,
						 sizeof(*entry->block_anchors) * entry->block_anchor_capacity);
	}

	anchor = &entry->block_anchors[entry->block_anchor_count++];
	MemSet(anchor, 0, sizeof(*anchor));
	anchor->locator = *locator;
	anchor->forknum = forknum;
	anchor->blkno = blkno;
	anchor->anchor_lsn = anchor_lsn;
	anchor->flags = flags;
	hash_entry->index = entry->block_anchor_count - 1;
}

static void
fb_summary_relation_append_unsafe_fact_common(FbSummaryBuildRelationEntry *entry,
											  FbWalUnsafeReason reason,
											  FbWalUnsafeScope scope,
											  FbWalStorageChangeOp storage_op,
											  TransactionId xid,
											  XLogRecPtr record_lsn,
											  uint8 match_kind,
											  const RelFileLocator *locator,
											  Oid db_oid,
											  Oid rel_oid)
{
	FbSummaryUnsafeFact *fact;
	FbSummaryUnsafeFact unsafe_fact;
	bool found = false;

	if (entry == NULL)
		return;
	if (entry->unsafe_fact_set == NULL)
		entry->unsafe_fact_set = fb_summary_create_unsafe_fact_hash();

	MemSet(&unsafe_fact, 0, sizeof(unsafe_fact));
	unsafe_fact.reason = (uint8) reason;
	unsafe_fact.scope = (uint8) scope;
	unsafe_fact.storage_op = (uint8) storage_op;
	unsafe_fact.match_kind = match_kind;
	unsafe_fact.xid = xid;
	unsafe_fact.record_lsn = record_lsn;
	if (locator != NULL)
		unsafe_fact.locator = *locator;
	unsafe_fact.db_oid = db_oid;
	unsafe_fact.rel_oid = rel_oid;

	hash_search(entry->unsafe_fact_set, &unsafe_fact, HASH_ENTER, &found);
	if (found)
		return;

	if (entry->unsafe_fact_count == entry->unsafe_fact_capacity)
	{
		entry->unsafe_fact_capacity =
			(entry->unsafe_fact_capacity == 0) ? 4 : entry->unsafe_fact_capacity * 2;
		if (entry->unsafe_facts == NULL)
			entry->unsafe_facts =
				palloc0(sizeof(*entry->unsafe_facts) * entry->unsafe_fact_capacity);
		else
			entry->unsafe_facts =
				repalloc(entry->unsafe_facts,
						 sizeof(*entry->unsafe_facts) * entry->unsafe_fact_capacity);
	}

	fact = &entry->unsafe_facts[entry->unsafe_fact_count++];
	*fact = unsafe_fact;
}

static void
fb_summary_relation_append_unsafe_fact_locator(FbSummaryBuildRelationEntry *entry,
											   FbWalUnsafeReason reason,
											   FbWalUnsafeScope scope,
											   FbWalStorageChangeOp storage_op,
											   TransactionId xid,
											   XLogRecPtr record_lsn,
											   const RelFileLocator *locator)
{
	fb_summary_relation_append_unsafe_fact_common(entry,
												  reason,
												  scope,
												  storage_op,
												  xid,
												  record_lsn,
												  FB_SUMMARY_UNSAFE_MATCH_LOCATOR,
												  locator,
												  InvalidOid,
												  InvalidOid);
}

static void
fb_summary_relation_append_unsafe_fact_reltag(FbSummaryBuildRelationEntry *entry,
											  FbWalUnsafeReason reason,
											  FbWalUnsafeScope scope,
											  FbWalStorageChangeOp storage_op,
											  TransactionId xid,
											  XLogRecPtr record_lsn,
											  Oid db_oid,
											  Oid rel_oid)
{
	fb_summary_relation_append_unsafe_fact_common(entry,
												  reason,
												  scope,
												  storage_op,
												  xid,
												  record_lsn,
												  FB_SUMMARY_UNSAFE_MATCH_RELTAG,
												  NULL,
												  db_oid,
												  rel_oid);
}

static void
fb_summary_note_xid_outcome(FbSummaryBuildState *state,
							TransactionId xid,
							FbWalXidStatus status,
							TimestampTz commit_ts,
							XLogRecPtr commit_lsn)
{
	FbSummaryBuildXidOutcome *entry;
	bool found = false;

	if (state == NULL || !TransactionIdIsValid(xid))
		return;
	if (state->xid_outcomes == NULL)
		state->xid_outcomes = fb_summary_create_xid_outcome_hash();

	entry = (FbSummaryBuildXidOutcome *) hash_search(state->xid_outcomes,
													 &xid,
													 HASH_ENTER,
													 &found);
	entry->xid = xid;
	entry->outcome.xid = xid;
	entry->outcome.status = (uint8) status;
	entry->outcome.commit_ts = commit_ts;
	entry->outcome.commit_lsn = commit_lsn;
}

static bool
fb_summary_find_section(const FbSummaryFileHeader *file,
						uint32 kind,
						uint32 *offset_out,
						uint32 *length_out,
						uint32 *item_count_out)
{
	uint32 i;

	if (offset_out != NULL)
		*offset_out = 0;
	if (length_out != NULL)
		*length_out = 0;
	if (item_count_out != NULL)
		*item_count_out = 0;
	if (file == NULL)
		return false;

	for (i = 0; i < Min(file->section_count, (uint32) FB_SUMMARY_SECTION_COUNT); i++)
	{
		if (file->sections[i].kind != kind)
			continue;
		if (offset_out != NULL)
			*offset_out = file->sections[i].offset;
		if (length_out != NULL)
			*length_out = file->sections[i].length;
		if (item_count_out != NULL)
			*item_count_out = file->sections[i].item_count;
		return true;
	}

	return false;
}

static bool
fb_summary_relation_entry_matches_info(const FbSummaryDiskRelationEntry *entry,
									   const FbRelationInfo *info)
{
	FbSummaryRelTag reltag;

	if (entry == NULL || info == NULL)
		return false;

	reltag.db_oid = FB_LOCATOR_DBOID(info->locator);
	reltag.rel_oid = info->relid;

	if (entry->kind == FB_SUMMARY_RELKEY_LOCATOR &&
		memcmp(&entry->locator, &info->locator, sizeof(info->locator)) == 0)
		return true;
	if (entry->kind == FB_SUMMARY_RELKEY_LOCATOR &&
		info->has_toast_locator &&
		memcmp(&entry->locator, &info->toast_locator, sizeof(info->toast_locator)) == 0)
		return true;
	if (entry->kind == FB_SUMMARY_RELKEY_RELTAG &&
		entry->db_oid == reltag.db_oid &&
		entry->rel_oid == reltag.rel_oid)
		return true;
	if (entry->kind == FB_SUMMARY_RELKEY_RELTAG &&
		entry->db_oid == reltag.db_oid &&
		OidIsValid(info->toast_relid) &&
		entry->rel_oid == info->toast_relid)
		return true;

	return false;
}

static int
fb_summary_block_anchor_public_cmp(const void *lhs, const void *rhs)
{
	const FbSummaryBlockAnchor *left = (const FbSummaryBlockAnchor *) lhs;
	const FbSummaryBlockAnchor *right = (const FbSummaryBlockAnchor *) rhs;
	int locator_cmp;

	if (left->anchor_lsn < right->anchor_lsn)
		return -1;
	if (left->anchor_lsn > right->anchor_lsn)
		return 1;

	locator_cmp = memcmp(&left->locator, &right->locator, sizeof(left->locator));
	if (locator_cmp != 0)
		return locator_cmp;
	if (left->forknum < right->forknum)
		return -1;
	if (left->forknum > right->forknum)
		return 1;
	if (left->blkno < right->blkno)
		return -1;
	if (left->blkno > right->blkno)
		return 1;
	if (left->flags < right->flags)
		return -1;
	if (left->flags > right->flags)
		return 1;
	return 0;
}

static void
fb_summary_bloom_add(unsigned char *bits, Size bits_len, const void *data, Size len)
{
	uint64 hash1;
	uint64 hash2;
	uint64 bit_count;
	int i;

	hash1 = fb_summary_hash_bytes(UINT64CONST(1469598103934665603), data, len);
	hash2 = fb_summary_hash_bytes(UINT64CONST(1099511628211), data, len);
	bit_count = bits_len * 8;

	for (i = 0; i < 4; i++)
	{
		uint64 bit = (hash1 + ((uint64) i * hash2)) % bit_count;

		bits[bit / 8] |= (1U << (bit % 8));
	}
}

static bool
fb_summary_bloom_maybe_contains(const unsigned char *bits, Size bits_len,
								  const void *data, Size len)
{
	uint64 hash1;
	uint64 hash2;
	uint64 bit_count;
	int i;

	hash1 = fb_summary_hash_bytes(UINT64CONST(1469598103934665603), data, len);
	hash2 = fb_summary_hash_bytes(UINT64CONST(1099511628211), data, len);
	bit_count = bits_len * 8;

	for (i = 0; i < 4; i++)
	{
		uint64 bit = (hash1 + ((uint64) i * hash2)) % bit_count;

		if ((bits[bit / 8] & (1U << (bit % 8))) == 0)
			return false;
	}

	return true;
}

static void
fb_summary_add_locator(FbSummaryBuildState *state, const RelFileLocator *locator)
{
	FbSummaryBuildRelationEntry *entry;

	if (state == NULL || locator == NULL)
		return;

	fb_summary_bloom_add(state->file.locator_bloom,
						 sizeof(state->file.locator_bloom),
						 locator,
						 sizeof(*locator));
	entry = fb_summary_get_or_add_locator_relation(state, locator);
	(void) entry;
}

static void
fb_summary_add_reltag(FbSummaryBuildState *state, Oid db_oid, Oid rel_oid)
{
	FbSummaryRelTag tag;
	FbSummaryBuildRelationEntry *entry;

	if (state == NULL || !OidIsValid(db_oid) || !OidIsValid(rel_oid))
		return;

	tag.db_oid = db_oid;
	tag.rel_oid = rel_oid;
	fb_summary_bloom_add(state->file.reltag_bloom,
						 sizeof(state->file.reltag_bloom),
						 &tag,
						 sizeof(tag));
	entry = fb_summary_get_or_add_reltag_relation(state, db_oid, rel_oid);
	(void) entry;
}

static void
fb_summary_note_xact_timestamp(FbSummaryBuildState *state, TimestampTz ts)
{
	if (state == NULL || ts == 0)
		return;

	if (state->file.oldest_xact_ts == 0 || ts < state->file.oldest_xact_ts)
		state->file.oldest_xact_ts = ts;
	if (state->file.newest_xact_ts == 0 || ts > state->file.newest_xact_ts)
		state->file.newest_xact_ts = ts;
}

static bool
fb_summary_record_locator_from_smgr(XLogReaderState *reader, RelFileLocator *locator_out)
{
	uint8 info_code;

	if (reader == NULL || locator_out == NULL || XLogRecGetRmid(reader) != RM_SMGR_ID)
		return false;

	info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	if (info_code == XLOG_SMGR_CREATE)
	{
		xl_smgr_create *xlrec = (xl_smgr_create *) XLogRecGetData(reader);

		if (xlrec->forkNum != MAIN_FORKNUM)
			return false;
		*locator_out = FB_XL_SMGR_CREATE_LOCATOR(xlrec);
		return true;
	}
	if (info_code == XLOG_SMGR_TRUNCATE)
	{
		xl_smgr_truncate *xlrec = (xl_smgr_truncate *) XLogRecGetData(reader);

		if ((xlrec->flags & SMGR_TRUNCATE_HEAP) == 0)
			return false;
		*locator_out = FB_XL_SMGR_TRUNCATE_LOCATOR(xlrec);
		return true;
	}

	return false;
}

static bool
fb_summary_record_block_is_anchor(XLogReaderState *reader,
								  int block_id,
								  uint16 *flags_out)
{
	uint8 rmid;
	uint8 info_code;
	uint16 flags = 0;

	if (flags_out != NULL)
		*flags_out = 0;
	if (reader == NULL || block_id < 0)
		return false;

	if (XLogRecHasBlockImage(reader, block_id))
	{
		flags |= 0x0001;
		if (flags_out != NULL)
			*flags_out = flags;
		return true;
	}

	if (block_id != 0 || (XLogRecGetInfo(reader) & XLOG_HEAP_INIT_PAGE) == 0)
		return false;

	rmid = XLogRecGetRmid(reader);
	if (rmid == RM_HEAP_ID)
	{
		info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;
		if (info_code == XLOG_HEAP_INSERT ||
			info_code == XLOG_HEAP_UPDATE ||
			info_code == XLOG_HEAP_HOT_UPDATE)
		{
			flags |= 0x0002;
			if (flags_out != NULL)
				*flags_out = flags;
			return true;
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;
		if (info_code == XLOG_HEAP2_MULTI_INSERT)
		{
			flags |= 0x0002;
			if (flags_out != NULL)
				*flags_out = flags;
			return true;
		}
	}

	return false;
}

static int
fb_summary_open_file_at_path(const char *path)
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

static void
fb_summary_note_record(XLogReaderState *reader, FbSummaryBuildState *state)
{
	uint8 rmid;
	int block_id;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	TransactionId xid;
	TransactionId top_xid;

	if (reader == NULL || state == NULL)
		return;

	rmid = XLogRecGetRmid(reader);
	record_start = reader->ReadRecPtr;
	record_end = reader->EndRecPtr;
	xid = fb_summary_record_xid(reader);
	top_xid = FB_XLOGREC_GET_TOP_XID(reader);
	state->file.flags |= (1U << Min((unsigned int) rmid, 31U));

	for (block_id = 0; block_id <= FB_XLOGREC_MAX_BLOCK_ID(reader); block_id++)
	{
		RelFileLocator locator;
		ForkNumber forknum;
		BlockNumber blkno;
		FbSummaryBuildRelationEntry *entry;
		uint16 anchor_flags = 0;

		if (!fb_xlogrec_get_block_tag(reader, block_id, &locator, &forknum, &blkno))
			continue;
		if (forknum != MAIN_FORKNUM)
			continue;

		fb_summary_add_locator(state, &locator);
		entry = fb_summary_get_or_add_locator_relation(state, &locator);
		fb_summary_relation_append_span(entry, record_start, record_end, 0);
		fb_summary_relation_append_touched_xid(entry, xid);
		if (TransactionIdIsValid(top_xid) && top_xid != xid)
			fb_summary_relation_append_touched_xid(entry, top_xid);
		if (fb_summary_record_block_is_anchor(reader, block_id, &anchor_flags))
			fb_summary_relation_append_block_anchor(entry,
													&locator,
													forknum,
													blkno,
													record_start,
													anchor_flags);
	}

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_HEAP_TRUNCATE)
		{
			xl_heap_truncate *xlrec = (xl_heap_truncate *) XLogRecGetData(reader);
			uint32 i;

			for (i = 0; i < xlrec->nrelids; i++)
			{
				FbSummaryBuildRelationEntry *entry;

				fb_summary_add_reltag(state, xlrec->dbId, xlrec->relids[i]);
				entry = fb_summary_get_or_add_reltag_relation(state, xlrec->dbId, xlrec->relids[i]);
				fb_summary_relation_append_span(entry, record_start, record_end, 0);
				fb_summary_relation_append_unsafe_fact_reltag(entry,
															  FB_WAL_UNSAFE_TRUNCATE,
															  FB_WAL_UNSAFE_SCOPE_NONE,
															  FB_WAL_STORAGE_CHANGE_UNKNOWN,
															  fb_summary_record_xid(reader),
															  record_start,
															  xlrec->dbId,
															  xlrec->relids[i]);
			}
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (info_code == XLOG_HEAP2_REWRITE)
		{
			xl_heap_rewrite_mapping *xlrec;
			FbSummaryBuildRelationEntry *entry;

			xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);
			fb_summary_add_reltag(state, xlrec->mapped_db, xlrec->mapped_rel);
			entry = fb_summary_get_or_add_reltag_relation(state, xlrec->mapped_db, xlrec->mapped_rel);
			fb_summary_relation_append_span(entry, record_start, record_end, 0);
			fb_summary_relation_append_unsafe_fact_reltag(entry,
														  FB_WAL_UNSAFE_REWRITE,
														  FB_WAL_UNSAFE_SCOPE_NONE,
														  FB_WAL_STORAGE_CHANGE_UNKNOWN,
														  xlrec->mapped_xid,
														  record_start,
														  xlrec->mapped_db,
														  xlrec->mapped_rel);
		}
	}
	else if (rmid == RM_SMGR_ID)
	{
		RelFileLocator locator;

		if (fb_summary_record_locator_from_smgr(reader, &locator))
		{
			uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
			FbSummaryBuildRelationEntry *entry;

			fb_summary_add_locator(state, &locator);
			entry = fb_summary_get_or_add_locator_relation(state, &locator);
			fb_summary_relation_append_span(entry, record_start, record_end, 0);
			fb_summary_relation_append_unsafe_fact_locator(entry,
														   FB_WAL_UNSAFE_STORAGE_CHANGE,
														   FB_WAL_UNSAFE_SCOPE_NONE,
														   (info_code == XLOG_SMGR_CREATE) ?
														   FB_WAL_STORAGE_CHANGE_SMGR_CREATE :
														   FB_WAL_STORAGE_CHANGE_SMGR_TRUNCATE,
														   fb_summary_record_xid(reader),
														   record_start,
														   &locator);
		}
	}
	else if (rmid == RM_STANDBY_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_STANDBY_LOCK)
		{
			xl_standby_locks *xlrec = (xl_standby_locks *) XLogRecGetData(reader);
			int i;

			for (i = 0; i < xlrec->nlocks; i++)
			{
				FbSummaryBuildRelationEntry *entry;

				fb_summary_add_reltag(state,
									  xlrec->locks[i].dbOid,
									  xlrec->locks[i].relOid);
				entry = fb_summary_get_or_add_reltag_relation(state,
															 xlrec->locks[i].dbOid,
															 xlrec->locks[i].relOid);
				fb_summary_relation_append_span(entry, record_start, record_end, 0);
				fb_summary_relation_append_unsafe_fact_reltag(entry,
															  FB_WAL_UNSAFE_STORAGE_CHANGE,
															  FB_WAL_UNSAFE_SCOPE_NONE,
															  FB_WAL_STORAGE_CHANGE_STANDBY_LOCK,
															  xlrec->locks[i].xid,
															  record_start,
															  xlrec->locks[i].dbOid,
															  xlrec->locks[i].relOid);
			}
		}
	}
	else if (rmid == RM_XACT_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

		if (info_code == XLOG_XACT_COMMIT ||
			info_code == XLOG_XACT_COMMIT_PREPARED)
			{
				xl_xact_commit *xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				xl_xact_parsed_commit parsed;
				int i;

				ParseCommitRecord(info_code, xlrec, &parsed);
			fb_summary_note_xact_timestamp(state, parsed.xact_time);
			fb_summary_note_xid_outcome(state,
										xid,
										FB_WAL_XID_COMMITTED,
										parsed.xact_time,
										record_end);
				for (i = 0; i < parsed.nsubxacts; i++)
					fb_summary_note_xid_outcome(state,
											parsed.subxacts[i],
											FB_WAL_XID_COMMITTED,
											parsed.xact_time,
											record_end);
		}
		else if (info_code == XLOG_XACT_ABORT ||
				 info_code == XLOG_XACT_ABORT_PREPARED)
			{
				xl_xact_abort *xlrec = (xl_xact_abort *) XLogRecGetData(reader);
				xl_xact_parsed_abort parsed;
				int i;

				ParseAbortRecord(info_code, xlrec, &parsed);
			fb_summary_note_xact_timestamp(state, parsed.xact_time);
			fb_summary_note_xid_outcome(state,
										xid,
										FB_WAL_XID_ABORTED,
										parsed.xact_time,
										record_end);
				for (i = 0; i < parsed.nsubxacts; i++)
					fb_summary_note_xid_outcome(state,
											parsed.subxacts[i],
											FB_WAL_XID_ABORTED,
											parsed.xact_time,
											record_end);
		}
		else if (info_code == XLOG_XACT_PREPARE)
		{
			xl_xact_prepare *xlrec = (xl_xact_prepare *) XLogRecGetData(reader);

			fb_summary_note_xact_timestamp(state, xlrec->prepared_at);
			fb_summary_note_xid_outcome(state,
										xid,
										FB_WAL_XID_UNKNOWN,
										xlrec->prepared_at,
										record_end);
		}
	}
}

static int
fb_summary_read_wal_segment_size_from_path(const char *path)
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
				 errmsg("invalid WAL segment size in \"%s\": %d",
						path, wal_seg_size)));

	return wal_seg_size;
}

static bool
fb_summary_parse_timeline_id(const char *name, TimeLineID *timeline_id)
{
	char tli_buf[9];
	char *endptr = NULL;
	unsigned long parsed;

	if (timeline_id == NULL || name == NULL || strlen(name) < 8)
		return false;

	memcpy(tli_buf, name, 8);
	tli_buf[8] = '\0';

	errno = 0;
	parsed = strtoul(tli_buf, &endptr, 16);
	if (errno != 0 || endptr == tli_buf || *endptr != '\0' ||
		parsed > PG_UINT32_MAX)
		return false;

	*timeline_id = (TimeLineID) parsed;
	return true;
}

static int
fb_summary_segment_name_cmp(const void *lhs, const void *rhs)
{
	const FbSummarySegmentEntry *left = (const FbSummarySegmentEntry *) lhs;
	const FbSummarySegmentEntry *right = (const FbSummarySegmentEntry *) rhs;

	if (left->timeline_id < right->timeline_id)
		return -1;
	if (left->timeline_id > right->timeline_id)
		return 1;
	if (left->segno < right->segno)
		return -1;
	if (left->segno > right->segno)
		return 1;
	return 0;
}

static void
fb_summary_append_segment_entry(FbSummarySegmentEntry **segments,
								int *segment_count,
								int *segment_capacity,
								const char *directory,
								const char *name,
								TimeLineID timeline_id,
								int source_kind,
								off_t bytes)
{
	FbSummarySegmentEntry *entry;

	if (*segment_count == *segment_capacity)
	{
		*segment_capacity = (*segment_capacity == 0) ? 32 : (*segment_capacity * 2);
		if (*segments == NULL)
			*segments = palloc(sizeof(FbSummarySegmentEntry) * (*segment_capacity));
		else
			*segments = repalloc(*segments,
								 sizeof(FbSummarySegmentEntry) * (*segment_capacity));
	}

	entry = &(*segments)[(*segment_count)++];
	MemSet(entry, 0, sizeof(*entry));
	strlcpy(entry->name, name, sizeof(entry->name));
	snprintf(entry->path, sizeof(entry->path), "%s/%s", directory, name);
	entry->timeline_id = timeline_id;
	entry->source_kind = source_kind;
	entry->bytes = bytes;
	entry->valid = true;
}

static void
fb_summary_collect_segments_from_directory(const char *directory,
										   int source_kind,
										   FbSummarySegmentEntry **segments,
										   int *segment_count,
										   int *segment_capacity)
{
	DIR *dir;
	struct dirent *de;

	if (directory == NULL || directory[0] == '\0')
		return;

	dir = AllocateDir(directory);
	if (dir == NULL)
		return;

	while ((de = ReadDir(dir, directory)) != NULL)
	{
		TimeLineID timeline_id;
		char path[MAXPGPATH];
		struct stat st;

		if (!IsXLogFileName(de->d_name))
			continue;
		if (!fb_summary_parse_timeline_id(de->d_name, &timeline_id))
			continue;

		snprintf(path, sizeof(path), "%s/%s", directory, de->d_name);
		if (stat(path, &st) != 0)
			continue;

		fb_summary_append_segment_entry(segments, segment_count, segment_capacity,
										directory, de->d_name, timeline_id,
										source_kind, st.st_size);
	}

	FreeDir(dir);
}

static bool
fb_summary_read_segment_identity_path(const char *path,
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
	if (!IsValidWalSegSize(longhdr->xlp_seg_size) ||
		longhdr->xlp_seg_size != wal_seg_size)
		return false;

	*timeline_id = longhdr->std.xlp_tli;
	XLByteToSeg(longhdr->std.xlp_pageaddr, *segno, wal_seg_size);
	return true;
}

static bool
fb_summary_validate_segment_identity(FbSummarySegmentEntry *entry, int wal_seg_size)
{
	TimeLineID actual_tli;
	XLogSegNo actual_segno;

	if (entry == NULL)
		return false;
	if (!fb_summary_read_segment_identity_path(entry->path, &actual_tli,
											   &actual_segno, wal_seg_size))
		return false;

	if (actual_tli != entry->timeline_id || actual_segno != entry->segno)
		return false;

	return true;
}

#if PG_VERSION_NUM >= 130000
static void
fb_summary_open_segment(XLogReaderState *state, XLogSegNo next_segno,
						TimeLineID *timeline_id)
{
	FbSummaryReaderPrivate *private = (FbSummaryReaderPrivate *) state->private_data;
	const FbSummarySegmentEntry *entry = NULL;

	if (private->segment != NULL && private->segment->segno == next_segno)
		entry = private->segment;
	else if (private->next_segment != NULL &&
			 private->next_segment->segno == next_segno)
		entry = private->next_segment;

	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("summary builder missing decode-tail segment for segno %llu",
						(unsigned long long) next_segno)));

	*timeline_id = entry->timeline_id;
	state->seg.ws_file = fb_summary_open_file_at_path(entry->path);
	if (state->seg.ws_file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open WAL segment \"%s\": %m", entry->path)));
}

static void
fb_summary_close_segment(XLogReaderState *state)
{
	CloseTransientFile(state->seg.ws_file);
	state->seg.ws_file = -1;
}
#endif

static int
fb_summary_read_page(XLogReaderState *state, XLogRecPtr target_page_ptr,
					 int req_len, XLogRecPtr target_rec_ptr, char *read_buf
#if PG_VERSION_NUM < 130000
					 , TimeLineID *page_tli
#endif
	)
{
	FbSummaryReaderPrivate *private = (FbSummaryReaderPrivate *) state->private_data;
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
		const FbSummarySegmentEntry *entry = NULL;
		int nread;

		XLByteToSeg(target_page_ptr, target_segno, state->wal_segment_size);
		segment_offset = target_page_ptr % state->wal_segment_size;

		if (private->segment != NULL && private->segment->segno == target_segno)
			entry = private->segment;
		else if (private->next_segment != NULL &&
				 private->next_segment->segno == target_segno)
			entry = private->next_segment;

		if (entry == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("summary builder missing decode-tail segment for segno %llu",
							(unsigned long long) target_segno)));

		if (page_tli != NULL)
			*page_tli = entry->timeline_id;

		if (!private->current_file_open ||
			!private->current_file_segno_valid ||
			private->current_file_segno != target_segno)
		{
			if (private->current_file_open)
				CloseTransientFile(private->current_file);
			private->current_file = fb_summary_open_file_at_path(entry->path);
			if (private->current_file < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open WAL segment \"%s\": %m", entry->path)));
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

		if (!WALRead(state, read_buf, target_page_ptr, count, private->timeline_id, &errinfo))
		{
			WALOpenSegment *seg = &errinfo.wre_seg;
			char segment_name[MAXPGPATH];

			XLogFileName(segment_name, seg->ws_tli, seg->ws_segno, state->segcxt.ws_segsize);
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

static bool
fb_summary_visit_record(XLogReaderState *reader, void *arg)
{
	FbSummaryBuildState *state = (FbSummaryBuildState *) arg;

	fb_summary_note_record(reader, state);
	return true;
}

static bool
fb_summary_build_file_for_segment(const FbSummarySegmentEntry *entry,
								  const FbSummarySegmentEntry *next_entry,
								  int wal_seg_size)
{
	struct stat st;
	char *summary_path;
	char *tmp_path;
	FbSummaryBuildState state;
	FbSummaryDiskRelationEntry *disk_relations = NULL;
	FbSummaryDiskRelationSpan *disk_spans = NULL;
	FbSummaryDiskXidOutcome *disk_outcomes = NULL;
	FbSummaryDiskTouchedXid *disk_touched_xids = NULL;
	FbSummaryDiskUnsafeFact *disk_unsafe_facts = NULL;
	FbSummaryDiskBlockAnchor *disk_block_anchors = NULL;
	FbSummaryReaderPrivate private;
	XLogReaderState *reader;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	XLogRecPtr next_end_lsn = InvalidXLogRecPtr;
	XLogRecPtr first_record;
	StringInfoData buf;
	HASH_SEQ_STATUS xid_status;
	FbSummaryBuildXidOutcome *xid_entry;
	uint32 total_spans = 0;
	uint32 span_index = 0;
	uint32 outcome_count = 0;
	uint32 total_touched_xids = 0;
	uint32 touched_xid_index = 0;
	uint32 total_unsafe_facts = 0;
	uint32 unsafe_fact_index = 0;
	uint32 total_block_anchors = 0;
	uint32 block_anchor_index = 0;
	uint32 section_index = 0;
	uint32 i;
	int fd;
	ssize_t nwritten;

	if (entry == NULL || stat(entry->path, &st) != 0)
		return false;

	summary_path = fb_summary_file_path(entry->path, &st);
	tmp_path = psprintf("%s.tmp.%d", summary_path, MyProcPid);

	MemSet(&state, 0, sizeof(state));
	state.file.magic = FB_SUMMARY_MAGIC;
	state.file.version = FB_SUMMARY_VERSION;
	state.file.source_kind = (uint16) entry->source_kind;
	state.file.file_identity_hash =
		fb_summary_file_identity_hash(entry->path, &st);
	state.file.timeline_id = entry->timeline_id;
	state.file.wal_seg_size = wal_seg_size;
	state.file.segno = entry->segno;
	state.file.built_at = GetCurrentTimestamp();
	state.locator_hash = fb_summary_create_locator_hash();
	state.reltag_hash = fb_summary_create_reltag_hash();
	state.xid_outcomes = fb_summary_create_xid_outcome_hash();

	MemSet(&private, 0, sizeof(private));
	private.segment = entry;
	private.next_segment = next_entry;
	private.timeline_id = entry->timeline_id;
#if PG_VERSION_NUM < 130000
	private.current_file = -1;
#endif
	XLogSegNoOffsetToRecPtr(entry->segno, 0, wal_seg_size, start_lsn);
	end_lsn = start_lsn + (XLogRecPtr) Min((off_t) wal_seg_size, entry->bytes);
	if (next_entry != NULL)
	{
		XLogSegNoOffsetToRecPtr(next_entry->segno, 0, wal_seg_size, next_end_lsn);
		next_end_lsn += (XLogRecPtr) Min((off_t) wal_seg_size, next_entry->bytes);
	}
	private.endptr = XLogRecPtrIsInvalid(next_end_lsn) ? end_lsn : next_end_lsn;

#if PG_VERSION_NUM < 130000
	reader = XLogReaderAllocate(wal_seg_size, fb_summary_read_page, &private);
#else
	reader = XLogReaderAllocate(wal_seg_size, "",
								XL_ROUTINE(.page_read = fb_summary_read_page,
										   .segment_open = fb_summary_open_segment,
										   .segment_close = fb_summary_close_segment),
								&private);
#endif
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a summary WAL reader.")));

	first_record = XLogFindNextRecord(reader, start_lsn);
	if (!XLogRecPtrIsInvalid(first_record))
	{
		char *errormsg = NULL;

#if PG_VERSION_NUM >= 130000
		XLogBeginRead(reader, first_record);
#endif
		while (true)
		{
			XLogRecord *record;

#if PG_VERSION_NUM < 130000
			record = XLogReadRecord(reader, InvalidXLogRecPtr, &errormsg);
#else
			record = XLogReadRecord(reader, &errormsg);
#endif
			if (record == NULL)
				break;
			if (reader->ReadRecPtr >= end_lsn)
				break;

			fb_summary_visit_record(reader, &state);
		}
	}

	XLogReaderFree(reader);
	#if PG_VERSION_NUM < 130000
	if (private.current_file_open)
		CloseTransientFile(private.current_file);
	#endif

	state.file.relation_entry_count = state.relation_count;
	for (i = 0; i < state.relation_count; i++)
	{
		total_spans += state.relations[i].span_count;
		total_touched_xids += state.relations[i].touched_xid_count;
		total_unsafe_facts += state.relations[i].unsafe_fact_count;
		total_block_anchors += state.relations[i].block_anchor_count;
	}
	state.file.relation_span_count = total_spans;
	state.file.touched_xid_count = total_touched_xids;
	state.file.unsafe_fact_count = total_unsafe_facts;
	state.file.block_anchor_count = total_block_anchors;

	if (state.relation_count > 0)
		disk_relations = palloc0(sizeof(*disk_relations) * state.relation_count);
	if (total_spans > 0)
		disk_spans = palloc0(sizeof(*disk_spans) * total_spans);
	if (total_touched_xids > 0)
		disk_touched_xids = palloc0(sizeof(*disk_touched_xids) * total_touched_xids);
	if (total_unsafe_facts > 0)
		disk_unsafe_facts = palloc0(sizeof(*disk_unsafe_facts) * total_unsafe_facts);
	if (total_block_anchors > 0)
		disk_block_anchors = palloc0(sizeof(*disk_block_anchors) * total_block_anchors);

	for (i = 0; i < state.relation_count; i++)
	{
		FbSummaryBuildRelationEntry *src = &state.relations[i];
		FbSummaryDiskRelationEntry *dst = &disk_relations[i];
		uint32 j;

		dst->kind = src->kind;
		dst->slot = src->slot;
		dst->first_span = span_index;
		dst->span_count = src->span_count;
		dst->first_touched_xid = touched_xid_index;
		dst->first_unsafe_fact = unsafe_fact_index;
		dst->first_block_anchor = block_anchor_index;
		dst->locator = src->locator;
		dst->db_oid = src->db_oid;
		dst->rel_oid = src->rel_oid;

		for (j = 0; j < src->span_count; j++)
		{
			disk_spans[span_index].slot = src->slot;
			disk_spans[span_index].flags = src->spans[j].flags;
			disk_spans[span_index].start_lsn = src->spans[j].start_lsn;
			disk_spans[span_index].end_lsn = src->spans[j].end_lsn;
			span_index++;
		}

		for (j = 0; j < src->touched_xid_count; j++)
		{
			disk_touched_xids[touched_xid_index].slot = src->slot;
			disk_touched_xids[touched_xid_index].xid = src->touched_xids[j];
			touched_xid_index++;
		}
		dst->touched_xid_count = src->touched_xid_count;

		for (j = 0; j < src->unsafe_fact_count; j++)
		{
			disk_unsafe_facts[unsafe_fact_index].slot = src->slot;
			disk_unsafe_facts[unsafe_fact_index].reason = src->unsafe_facts[j].reason;
			disk_unsafe_facts[unsafe_fact_index].scope = src->unsafe_facts[j].scope;
			disk_unsafe_facts[unsafe_fact_index].storage_op = src->unsafe_facts[j].storage_op;
			disk_unsafe_facts[unsafe_fact_index].match_kind = src->unsafe_facts[j].match_kind;
			disk_unsafe_facts[unsafe_fact_index].xid = src->unsafe_facts[j].xid;
			disk_unsafe_facts[unsafe_fact_index].record_lsn = src->unsafe_facts[j].record_lsn;
			disk_unsafe_facts[unsafe_fact_index].locator = src->unsafe_facts[j].locator;
			disk_unsafe_facts[unsafe_fact_index].db_oid = src->unsafe_facts[j].db_oid;
			disk_unsafe_facts[unsafe_fact_index].rel_oid = src->unsafe_facts[j].rel_oid;
			unsafe_fact_index++;
		}
		dst->unsafe_fact_count = src->unsafe_fact_count;

		for (j = 0; j < src->block_anchor_count; j++)
		{
			disk_block_anchors[block_anchor_index].slot = src->slot;
			disk_block_anchors[block_anchor_index].flags = src->block_anchors[j].flags;
			disk_block_anchors[block_anchor_index].blkno = src->block_anchors[j].blkno;
			disk_block_anchors[block_anchor_index].anchor_lsn =
				src->block_anchors[j].anchor_lsn;
			block_anchor_index++;
		}
		dst->block_anchor_count = block_anchor_index - dst->first_block_anchor;
	}

	state.file.touched_xid_count = touched_xid_index;
	state.file.unsafe_fact_count = unsafe_fact_index;
	state.file.block_anchor_count = block_anchor_index;

	outcome_count = (state.xid_outcomes == NULL) ? 0 : (uint32) hash_get_num_entries(state.xid_outcomes);
	state.file.xid_outcome_count = outcome_count;
	if (outcome_count > 0)
		disk_outcomes = palloc0(sizeof(*disk_outcomes) * outcome_count);
	if (state.xid_outcomes != NULL)
	{
		hash_seq_init(&xid_status, state.xid_outcomes);
		i = 0;
		while ((xid_entry = (FbSummaryBuildXidOutcome *) hash_seq_search(&xid_status)) != NULL)
			disk_outcomes[i++] = xid_entry->outcome;
	}

	state.file.section_count = 0;
	if (state.relation_count > 0)
	{
		state.file.sections[section_index].kind = FB_SUMMARY_SECTION_RELATION_ENTRIES;
		state.file.sections[section_index].offset = sizeof(FbSummaryFileHeader);
		state.file.sections[section_index].length = sizeof(*disk_relations) * state.relation_count;
		state.file.sections[section_index].item_count = state.relation_count;
		section_index++;
	}
	if (total_spans > 0)
	{
		state.file.sections[section_index].kind = FB_SUMMARY_SECTION_RELATION_SPANS;
		state.file.sections[section_index].offset =
			(section_index == 0) ? sizeof(FbSummaryFileHeader) :
			state.file.sections[section_index - 1].offset +
			state.file.sections[section_index - 1].length;
		state.file.sections[section_index].length = sizeof(*disk_spans) * total_spans;
		state.file.sections[section_index].item_count = total_spans;
		section_index++;
	}
	if (outcome_count > 0)
	{
		state.file.sections[section_index].kind = FB_SUMMARY_SECTION_XID_OUTCOMES;
		state.file.sections[section_index].offset =
			(section_index == 0) ? sizeof(FbSummaryFileHeader) :
			state.file.sections[section_index - 1].offset +
			state.file.sections[section_index - 1].length;
		state.file.sections[section_index].length = sizeof(*disk_outcomes) * outcome_count;
		state.file.sections[section_index].item_count = outcome_count;
		section_index++;
	}
	if (touched_xid_index > 0)
	{
		state.file.sections[section_index].kind = FB_SUMMARY_SECTION_TOUCHED_XIDS;
		state.file.sections[section_index].offset =
			(section_index == 0) ? sizeof(FbSummaryFileHeader) :
			state.file.sections[section_index - 1].offset +
			state.file.sections[section_index - 1].length;
		state.file.sections[section_index].length = sizeof(*disk_touched_xids) * touched_xid_index;
		state.file.sections[section_index].item_count = touched_xid_index;
		section_index++;
	}
	if (unsafe_fact_index > 0)
	{
		state.file.sections[section_index].kind = FB_SUMMARY_SECTION_UNSAFE_FACTS;
		state.file.sections[section_index].offset =
			(section_index == 0) ? sizeof(FbSummaryFileHeader) :
			state.file.sections[section_index - 1].offset +
			state.file.sections[section_index - 1].length;
		state.file.sections[section_index].length = sizeof(*disk_unsafe_facts) * unsafe_fact_index;
		state.file.sections[section_index].item_count = unsafe_fact_index;
		section_index++;
	}
	if (block_anchor_index > 0)
	{
		state.file.sections[section_index].kind = FB_SUMMARY_SECTION_BLOCK_ANCHORS;
		state.file.sections[section_index].offset =
			(section_index == 0) ? sizeof(FbSummaryFileHeader) :
			state.file.sections[section_index - 1].offset +
			state.file.sections[section_index - 1].length;
		state.file.sections[section_index].length =
			sizeof(*disk_block_anchors) * block_anchor_index;
		state.file.sections[section_index].item_count = block_anchor_index;
		section_index++;
	}
	state.file.section_count = section_index;

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, (char *) &state.file, sizeof(state.file));
	if (state.relation_count > 0)
		appendBinaryStringInfo(&buf, (char *) disk_relations,
							   sizeof(*disk_relations) * state.relation_count);
	if (total_spans > 0)
		appendBinaryStringInfo(&buf, (char *) disk_spans, sizeof(*disk_spans) * total_spans);
	if (outcome_count > 0)
		appendBinaryStringInfo(&buf, (char *) disk_outcomes, sizeof(*disk_outcomes) * outcome_count);
	if (touched_xid_index > 0)
		appendBinaryStringInfo(&buf,
							   (char *) disk_touched_xids,
							   sizeof(*disk_touched_xids) * touched_xid_index);
	if (unsafe_fact_index > 0)
		appendBinaryStringInfo(&buf,
							   (char *) disk_unsafe_facts,
							   sizeof(*disk_unsafe_facts) * unsafe_fact_index);
	if (block_anchor_index > 0)
		appendBinaryStringInfo(&buf,
							   (char *) disk_block_anchors,
							   sizeof(*disk_block_anchors) * block_anchor_index);

	fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
			  S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create summary file \"%s\": %m", tmp_path)));

	nwritten = write(fd, buf.data, buf.len);
	if (nwritten != buf.len)
	{
		close(fd);
		unlink(tmp_path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write summary file \"%s\": %m", tmp_path)));
	}

	(void) fsync(fd);
	close(fd);
	if (rename(tmp_path, summary_path) != 0)
	{
		unlink(tmp_path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not publish summary file \"%s\": %m", summary_path)));
	}

	pfree(summary_path);
	pfree(tmp_path);
	pfree(buf.data);
	if (disk_relations != NULL)
		pfree(disk_relations);
	if (disk_spans != NULL)
		pfree(disk_spans);
	if (disk_outcomes != NULL)
		pfree(disk_outcomes);
	if (disk_touched_xids != NULL)
		pfree(disk_touched_xids);
	if (disk_unsafe_facts != NULL)
		pfree(disk_unsafe_facts);
	if (state.locator_hash != NULL)
		hash_destroy(state.locator_hash);
	if (state.reltag_hash != NULL)
		hash_destroy(state.reltag_hash);
	if (state.xid_outcomes != NULL)
		hash_destroy(state.xid_outcomes);
	for (i = 0; i < state.relation_count; i++)
	{
		if (state.relations[i].spans != NULL)
			pfree(state.relations[i].spans);
		if (state.relations[i].touched_xids != NULL)
			pfree(state.relations[i].touched_xids);
		if (state.relations[i].unsafe_facts != NULL)
			pfree(state.relations[i].unsafe_facts);
	}
	if (state.relations != NULL)
		pfree(state.relations);
	return true;
}

static void
fb_summary_collect_meta_counts(FbSummaryMetaCounts *counts)
{
	char *meta_dir;
	char *summary_dir;
	DIR *dir;
	struct dirent *de;

	if (counts == NULL)
		return;
	MemSet(counts, 0, sizeof(*counts));

	summary_dir = fb_runtime_meta_summary_dir();
	dir = AllocateDir(summary_dir);
	if (dir != NULL)
	{
		while ((de = ReadDir(dir, summary_dir)) != NULL)
		{
			if (strncmp(de->d_name, "summary-", 8) == 0)
				counts->summary_files++;
		}
		FreeDir(dir);
	}

	meta_dir = fb_runtime_meta_dir();
	dir = AllocateDir(meta_dir);
	if (dir != NULL)
	{
		while ((de = ReadDir(dir, meta_dir)) != NULL)
		{
			if (strncmp(de->d_name, "prefilter-", 10) == 0)
				counts->prefilter_files++;
		}
		FreeDir(dir);
	}

	pfree(meta_dir);
	pfree(summary_dir);
}

FbSummaryQueryCache *
fb_summary_query_cache_create(MemoryContext mcxt)
{
	FbSummaryQueryCache *cache;

	if (mcxt == NULL)
		mcxt = CurrentMemoryContext;

	cache = MemoryContextAllocZero(mcxt, sizeof(*cache));
	cache->mcxt = mcxt;
	cache->entries = fb_summary_create_query_cache_hash(mcxt);
	return cache;
}

static void
fb_summary_cache_load_sections(FbSummaryQueryCache *cache,
							   FbSummaryQueryCacheEntry *entry,
							   int fd)
{
	uint32 offset = 0;
	uint32 length = 0;
	uint32 count = 0;

	if (entry == NULL || fd < 0)
		return;

	if (fb_summary_find_section(&entry->file, FB_SUMMARY_SECTION_RELATION_ENTRIES,
								&offset, &length, &count) &&
		count > 0)
	{
		entry->relations = MemoryContextAlloc(cache->mcxt, sizeof(*entry->relations) * count);
		if (lseek(fd, offset, SEEK_SET) < 0 ||
			read(fd, entry->relations, sizeof(*entry->relations) * count) !=
			(ssize_t) (sizeof(*entry->relations) * count))
			elog(ERROR, "failed to load summary relation entries");
	}

	if (fb_summary_find_section(&entry->file, FB_SUMMARY_SECTION_RELATION_SPANS,
								&offset, &length, &count) &&
		count > 0)
	{
		entry->spans = MemoryContextAlloc(cache->mcxt, sizeof(*entry->spans) * count);
		if (lseek(fd, offset, SEEK_SET) < 0 ||
			read(fd, entry->spans, sizeof(*entry->spans) * count) !=
			(ssize_t) (sizeof(*entry->spans) * count))
			elog(ERROR, "failed to load summary spans");
	}

	if (fb_summary_find_section(&entry->file, FB_SUMMARY_SECTION_XID_OUTCOMES,
								&offset, &length, &count) &&
		count > 0)
	{
		entry->xid_outcomes = MemoryContextAlloc(cache->mcxt, sizeof(*entry->xid_outcomes) * count);
		if (lseek(fd, offset, SEEK_SET) < 0 ||
			read(fd, entry->xid_outcomes, sizeof(*entry->xid_outcomes) * count) !=
			(ssize_t) (sizeof(*entry->xid_outcomes) * count))
			elog(ERROR, "failed to load summary xid outcomes");
	}

	if (fb_summary_find_section(&entry->file, FB_SUMMARY_SECTION_TOUCHED_XIDS,
								&offset, &length, &count) &&
		count > 0)
	{
		entry->touched_xids = MemoryContextAlloc(cache->mcxt, sizeof(*entry->touched_xids) * count);
		if (lseek(fd, offset, SEEK_SET) < 0 ||
			read(fd, entry->touched_xids, sizeof(*entry->touched_xids) * count) !=
			(ssize_t) (sizeof(*entry->touched_xids) * count))
			elog(ERROR, "failed to load summary touched xids");
	}

	if (fb_summary_find_section(&entry->file, FB_SUMMARY_SECTION_UNSAFE_FACTS,
								&offset, &length, &count) &&
		count > 0)
	{
		entry->unsafe_facts = MemoryContextAlloc(cache->mcxt, sizeof(*entry->unsafe_facts) * count);
		if (lseek(fd, offset, SEEK_SET) < 0 ||
			read(fd, entry->unsafe_facts, sizeof(*entry->unsafe_facts) * count) !=
			(ssize_t) (sizeof(*entry->unsafe_facts) * count))
			elog(ERROR, "failed to load summary unsafe facts");
	}

	if (fb_summary_find_section(&entry->file, FB_SUMMARY_SECTION_BLOCK_ANCHORS,
								&offset, &length, &count) &&
		count > 0)
	{
		entry->block_anchors = MemoryContextAlloc(cache->mcxt, sizeof(*entry->block_anchors) * count);
		if (lseek(fd, offset, SEEK_SET) < 0 ||
			read(fd, entry->block_anchors, sizeof(*entry->block_anchors) * count) !=
			(ssize_t) (sizeof(*entry->block_anchors) * count))
			elog(ERROR, "failed to load summary block anchors");
	}
}

static bool
fb_summary_cache_get_or_load(const char *path,
							 off_t bytes,
							 TimeLineID timeline_id,
							 XLogSegNo segno,
							 int wal_seg_size,
							 int source_kind,
							 FbSummaryQueryCache *cache,
							 FbSummaryQueryCacheEntry **entry_out)
{
	FbSummaryFileHeader file;
	struct stat st;
	uint64 file_identity_hash;
	FbSummaryQueryCacheEntry *entry;
	bool found = false;
	int fd = -1;

	if (entry_out != NULL)
		*entry_out = NULL;
	if (cache == NULL || cache->entries == NULL || path == NULL || stat(path, &st) != 0)
		return false;

	file_identity_hash = fb_summary_file_identity_hash(path, &st);
	entry = (FbSummaryQueryCacheEntry *) hash_search(cache->entries,
													 &file_identity_hash,
													 HASH_ENTER,
													 &found);
	if (found && entry->loaded)
	{
		if (entry_out != NULL)
			*entry_out = entry;
		return entry->valid;
	}

	MemSet(entry, 0, sizeof(*entry));
	entry->file_identity_hash = file_identity_hash;
	entry->loaded = true;
	entry->valid = fb_summary_open_validated_file(path,
												  bytes,
												  timeline_id,
												  segno,
												  wal_seg_size,
												  source_kind,
												  &file,
												  &fd);
	if (entry->valid)
	{
		entry->file = file;
		fb_summary_cache_load_sections(cache, entry, fd);
	}
	if (fd >= 0)
		close(fd);
	if (entry_out != NULL)
		*entry_out = entry;
	return entry->valid;
}

bool
fb_summary_segment_matches(const char *path,
						   off_t bytes,
						   TimeLineID timeline_id,
						   XLogSegNo segno,
						   int wal_seg_size,
						   int source_kind,
						   const FbRelationInfo *info,
						   bool *summary_available,
						   bool *hit)
{
	FbSummaryFileHeader file;
	FbSummaryRelTag reltag;
	bool matched = false;

	if (summary_available != NULL)
		*summary_available = false;
	if (hit != NULL)
		*hit = false;
	if (path == NULL || info == NULL)
		return false;

	if (!fb_summary_load_file(path, bytes, timeline_id, segno, wal_seg_size,
							  source_kind, &file))
		return false;

	if (summary_available != NULL)
		*summary_available = true;

	if (fb_summary_bloom_maybe_contains(file.locator_bloom,
										sizeof(file.locator_bloom),
										&info->locator,
										sizeof(info->locator)))
		matched = true;
	else if (info->has_toast_locator &&
			 fb_summary_bloom_maybe_contains(file.locator_bloom,
											 sizeof(file.locator_bloom),
											 &info->toast_locator,
											 sizeof(info->toast_locator)))
		matched = true;

	reltag.db_oid = FB_LOCATOR_DBOID(info->locator);
	reltag.rel_oid = info->relid;
	if (!matched &&
		fb_summary_bloom_maybe_contains(file.reltag_bloom,
										sizeof(file.reltag_bloom),
										&reltag,
										sizeof(reltag)))
		matched = true;

	if (!matched && OidIsValid(info->toast_relid))
	{
		reltag.rel_oid = info->toast_relid;
		if (fb_summary_bloom_maybe_contains(file.reltag_bloom,
											sizeof(file.reltag_bloom),
											&reltag,
											sizeof(reltag)))
			matched = true;
	}

	if (hit != NULL)
		*hit = matched;
	return true;
}

bool
fb_summary_segment_lookup_spans(const char *path,
								 off_t bytes,
								 TimeLineID timeline_id,
								 XLogSegNo segno,
								 int wal_seg_size,
								 int source_kind,
								 const FbRelationInfo *info,
								 FbSummarySpan **spans_out,
								 uint32 *span_count_out)
{
	return fb_summary_segment_lookup_spans_cached(path,
												 bytes,
												 timeline_id,
												 segno,
												 wal_seg_size,
												 source_kind,
												 info,
												 NULL,
												 spans_out,
												 span_count_out);
}

bool
fb_summary_segment_lookup_xid_outcomes(const char *path,
										off_t bytes,
										TimeLineID timeline_id,
										XLogSegNo segno,
										int wal_seg_size,
										int source_kind,
										FbSummaryXidOutcome **outcomes_out,
										uint32 *outcome_count_out)
{
	return fb_summary_segment_lookup_xid_outcomes_cached(path,
														 bytes,
														 timeline_id,
														 segno,
														 wal_seg_size,
														 source_kind,
														 NULL,
														 outcomes_out,
														 outcome_count_out);
}

bool
fb_summary_segment_lookup_spans_cached(const char *path,
										off_t bytes,
										TimeLineID timeline_id,
										XLogSegNo segno,
										int wal_seg_size,
										int source_kind,
										const FbRelationInfo *info,
										FbSummaryQueryCache *cache,
										FbSummarySpan **spans_out,
										uint32 *span_count_out)
{
	FbSummaryQueryCacheEntry *cache_entry = NULL;
	FbSummarySpan *spans = NULL;
	uint32 match_count = 0;
	uint32 i;

	if (spans_out != NULL)
		*spans_out = NULL;
	if (span_count_out != NULL)
		*span_count_out = 0;
	if (path == NULL || info == NULL)
		return false;

	if (cache != NULL)
	{
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}
	else
	{
		cache = fb_summary_query_cache_create(CurrentMemoryContext);
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}

	if (cache_entry == NULL)
		return false;
	if (cache_entry->file.relation_entry_count == 0)
		return true;
	if (cache_entry->relations == NULL)
		elog(ERROR, "summary relation entry section is missing");
	if (cache_entry->spans == NULL && cache_entry->file.relation_span_count > 0)
		elog(ERROR, "summary relation span section is missing");

	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;
		match_count += entry->span_count;
	}

	if (match_count > 0)
		spans = palloc(sizeof(*spans) * match_count);

	match_count = 0;
	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		uint32 j;
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;

		for (j = 0; j < entry->span_count; j++)
		{
			FbSummaryDiskRelationSpan *src = &cache_entry->spans[entry->first_span + j];
			spans[match_count].start_lsn = src->start_lsn;
			spans[match_count].end_lsn = src->end_lsn;
			spans[match_count].flags = src->flags;
			match_count++;
		}
	}

	if (spans_out != NULL)
		*spans_out = spans;
	else if (spans != NULL)
		pfree(spans);
	if (span_count_out != NULL)
		*span_count_out = match_count;
	return true;
}

bool
fb_summary_segment_lookup_xid_outcomes_cached(const char *path,
											  off_t bytes,
											  TimeLineID timeline_id,
											  XLogSegNo segno,
											  int wal_seg_size,
											  int source_kind,
											  FbSummaryQueryCache *cache,
											  FbSummaryXidOutcome **outcomes_out,
											  uint32 *outcome_count_out)
{
	FbSummaryQueryCacheEntry *cache_entry = NULL;
	FbSummaryXidOutcome *outcomes = NULL;
	uint32 count = 0;
	uint32 i;

	if (outcomes_out != NULL)
		*outcomes_out = NULL;
	if (outcome_count_out != NULL)
		*outcome_count_out = 0;

	if (cache != NULL)
	{
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}
	else
	{
		cache = fb_summary_query_cache_create(CurrentMemoryContext);
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}

	if (cache_entry == NULL)
		return false;

	count = cache_entry->file.xid_outcome_count;

	if (count > 0)
		outcomes = palloc(sizeof(*outcomes) * count);
	for (i = 0; i < count; i++)
	{
		outcomes[i].xid = cache_entry->xid_outcomes[i].xid;
		outcomes[i].status = cache_entry->xid_outcomes[i].status;
		outcomes[i].commit_ts = cache_entry->xid_outcomes[i].commit_ts;
		outcomes[i].commit_lsn = cache_entry->xid_outcomes[i].commit_lsn;
	}

	if (outcomes_out != NULL)
		*outcomes_out = outcomes;
	else if (outcomes != NULL)
		pfree(outcomes);
	if (outcome_count_out != NULL)
		*outcome_count_out = count;
	return true;
}

bool
fb_summary_segment_lookup_touched_xids_cached(const char *path,
											  off_t bytes,
											  TimeLineID timeline_id,
											  XLogSegNo segno,
											  int wal_seg_size,
											  int source_kind,
											  const FbRelationInfo *info,
											  FbSummaryQueryCache *cache,
											  TransactionId **xids_out,
											  uint32 *xid_count_out)
{
	FbSummaryQueryCacheEntry *cache_entry = NULL;
	TransactionId *xids = NULL;
	uint32 count = 0;
	uint32 i;

	if (xids_out != NULL)
		*xids_out = NULL;
	if (xid_count_out != NULL)
		*xid_count_out = 0;
	if (path == NULL || info == NULL)
		return false;

	if (cache != NULL)
	{
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}
	else
	{
		cache = fb_summary_query_cache_create(CurrentMemoryContext);
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}

	if (cache_entry == NULL)
		return false;
	if (cache_entry->file.relation_entry_count == 0)
		return true;
	if (cache_entry->relations == NULL)
		elog(ERROR, "summary relation entry section is missing");
	if (cache_entry->touched_xids == NULL && cache_entry->file.touched_xid_count > 0)
		elog(ERROR, "summary touched xid section is missing");

	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;
		count += entry->touched_xid_count;
	}

	if (count > 0)
		xids = palloc(sizeof(*xids) * count);

	count = 0;
	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];
		uint32 j;

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;

		for (j = 0; j < entry->touched_xid_count; j++)
			xids[count++] = cache_entry->touched_xids[entry->first_touched_xid + j].xid;
	}

	if (xids_out != NULL)
		*xids_out = xids;
	else if (xids != NULL)
		pfree(xids);
	if (xid_count_out != NULL)
		*xid_count_out = count;
	return true;
}

bool
fb_summary_segment_lookup_unsafe_facts_cached(const char *path,
											  off_t bytes,
											  TimeLineID timeline_id,
											  XLogSegNo segno,
											  int wal_seg_size,
											  int source_kind,
											  const FbRelationInfo *info,
											  FbSummaryQueryCache *cache,
											  FbSummaryUnsafeFact **facts_out,
											  uint32 *fact_count_out)
{
	FbSummaryQueryCacheEntry *cache_entry = NULL;
	FbSummaryUnsafeFact *facts = NULL;
	uint32 count = 0;
	uint32 i;

	if (facts_out != NULL)
		*facts_out = NULL;
	if (fact_count_out != NULL)
		*fact_count_out = 0;
	if (path == NULL || info == NULL)
		return false;

	if (cache != NULL)
	{
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}
	else
	{
		cache = fb_summary_query_cache_create(CurrentMemoryContext);
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}

	if (cache_entry == NULL)
		return false;
	if (cache_entry->file.relation_entry_count == 0)
		return true;
	if (cache_entry->relations == NULL)
		elog(ERROR, "summary relation entry section is missing");
	if (cache_entry->unsafe_facts == NULL && cache_entry->file.unsafe_fact_count > 0)
		elog(ERROR, "summary unsafe fact section is missing");

	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;
		count += entry->unsafe_fact_count;
	}

	if (count > 0)
		facts = palloc0(sizeof(*facts) * count);

	count = 0;
	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];
		uint32 j;

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;

		for (j = 0; j < entry->unsafe_fact_count; j++)
		{
			FbSummaryDiskUnsafeFact *src =
				&cache_entry->unsafe_facts[entry->first_unsafe_fact + j];

			facts[count].reason = src->reason;
			facts[count].scope = src->scope;
			facts[count].storage_op = src->storage_op;
			facts[count].match_kind = src->match_kind;
			facts[count].xid = src->xid;
			facts[count].record_lsn = src->record_lsn;
			facts[count].locator = src->locator;
			facts[count].db_oid = src->db_oid;
			facts[count].rel_oid = src->rel_oid;
			count++;
		}
	}

	if (facts_out != NULL)
		*facts_out = facts;
	else if (facts != NULL)
		pfree(facts);
	if (fact_count_out != NULL)
		*fact_count_out = count;
	return true;
}

bool
fb_summary_segment_lookup_block_anchors_cached(const char *path,
												off_t bytes,
												TimeLineID timeline_id,
												XLogSegNo segno,
												int wal_seg_size,
												int source_kind,
												const FbRelationInfo *info,
												FbSummaryQueryCache *cache,
												FbSummaryBlockAnchor **anchors_out,
												uint32 *anchor_count_out)
{
	FbSummaryQueryCacheEntry *cache_entry = NULL;
	FbSummaryBlockAnchor *anchors = NULL;
	uint32 count = 0;
	uint32 i;

	if (anchors_out != NULL)
		*anchors_out = NULL;
	if (anchor_count_out != NULL)
		*anchor_count_out = 0;
	if (path == NULL || info == NULL)
		return false;

	if (cache != NULL)
	{
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}
	else
	{
		cache = fb_summary_query_cache_create(CurrentMemoryContext);
		if (!fb_summary_cache_get_or_load(path, bytes, timeline_id, segno,
										  wal_seg_size, source_kind, cache, &cache_entry))
			return false;
	}

	if (cache_entry == NULL)
		return false;
	if (cache_entry->file.relation_entry_count == 0)
		return true;
	if (cache_entry->relations == NULL)
		elog(ERROR, "summary relation entry section is missing");
	if (cache_entry->block_anchors == NULL && cache_entry->file.block_anchor_count > 0)
		elog(ERROR, "summary block anchor section is missing");

	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;
		count += entry->block_anchor_count;
	}

	if (count > 0)
		anchors = palloc0(sizeof(*anchors) * count);

	count = 0;
	for (i = 0; i < cache_entry->file.relation_entry_count; i++)
	{
		FbSummaryDiskRelationEntry *entry = &cache_entry->relations[i];
		uint32 j;

		if (!fb_summary_relation_entry_matches_info(entry, info))
			continue;

		for (j = 0; j < entry->block_anchor_count; j++)
		{
			FbSummaryDiskBlockAnchor *src =
				&cache_entry->block_anchors[entry->first_block_anchor + j];

			anchors[count].locator = entry->locator;
			anchors[count].forknum = MAIN_FORKNUM;
			anchors[count].blkno = src->blkno;
			anchors[count].anchor_lsn = src->anchor_lsn;
			anchors[count].flags = src->flags;
			count++;
		}
	}

	if (count > 1)
		qsort(anchors, count, sizeof(*anchors), fb_summary_block_anchor_public_cmp);

	if (anchors_out != NULL)
		*anchors_out = anchors;
	else if (anchors != NULL)
		pfree(anchors);
	if (anchor_count_out != NULL)
		*anchor_count_out = count;
	return true;
}

int
fb_summary_collect_build_candidates(FbSummaryBuildCandidate **candidates_out,
									 bool skip_unstable_tail)
{
	FbSummarySegmentEntry *selected = NULL;
	FbSummaryBuildCandidate *candidates = NULL;
	int selected_count;
	int wal_seg_size = 0;
	int count = 0;
	int i;

	if (candidates_out != NULL)
		*candidates_out = NULL;

	selected_count = fb_summary_collect_selected_segments(&selected, &wal_seg_size);
	if (selected_count <= 0)
		return 0;

	candidates = palloc0(sizeof(FbSummaryBuildCandidate) * selected_count);
	for (i = 0; i < selected_count; i++)
	{
		FbSummaryBuildCandidate *candidate = &candidates[count];
		FbSummarySegmentEntry *entry = &selected[i];
		FbSummarySegmentEntry *next_entry = (i + 1 < selected_count) ? &selected[i + 1] : NULL;

		if (skip_unstable_tail)
		{
			if (next_entry == NULL ||
				next_entry->timeline_id != entry->timeline_id ||
				next_entry->segno != entry->segno + 1)
				continue;
		}

		strlcpy(candidate->path, entry->path, sizeof(candidate->path));
		candidate->timeline_id = entry->timeline_id;
		candidate->segno = entry->segno;
		candidate->bytes = entry->bytes;
		candidate->wal_seg_size = wal_seg_size;
		candidate->source_kind = entry->source_kind;
		if (next_entry != NULL &&
			next_entry->timeline_id == entry->timeline_id &&
			next_entry->segno == entry->segno + 1)
		{
			candidate->has_next_segment = true;
			strlcpy(candidate->next_path, next_entry->path, sizeof(candidate->next_path));
			candidate->next_timeline_id = next_entry->timeline_id;
			candidate->next_segno = next_entry->segno;
			candidate->next_bytes = next_entry->bytes;
		}
		count++;
	}

	pfree(selected);
	if (count == 0)
	{
		pfree(candidates);
		return 0;
	}

	if (count < selected_count)
		candidates = repalloc(candidates, sizeof(FbSummaryBuildCandidate) * count);
	if (candidates_out != NULL)
		*candidates_out = candidates;
	else
		pfree(candidates);
	return count;
}

void
fb_summary_free_build_candidates(FbSummaryBuildCandidate *candidates)
{
	if (candidates != NULL)
		pfree(candidates);
}

bool
fb_summary_candidate_summary_exists(const FbSummaryBuildCandidate *candidate)
{
	FbSummaryFileHeader file;

	if (candidate == NULL)
		return false;

	return fb_summary_load_file(candidate->path,
								candidate->bytes,
								candidate->timeline_id,
								candidate->segno,
								candidate->wal_seg_size,
								candidate->source_kind,
								&file);
}

bool
fb_summary_candidate_time_bounds(const FbSummaryBuildCandidate *candidate,
								 TimestampTz *oldest_xact_ts_out,
								 TimestampTz *newest_xact_ts_out)
{
	FbSummaryFileHeader file;
	bool loaded;

	if (oldest_xact_ts_out != NULL)
		*oldest_xact_ts_out = 0;
	if (newest_xact_ts_out != NULL)
		*newest_xact_ts_out = 0;
	if (candidate == NULL)
		return false;

	loaded = fb_summary_load_file(candidate->path,
								  candidate->bytes,
								  candidate->timeline_id,
								  candidate->segno,
								  candidate->wal_seg_size,
								  candidate->source_kind,
								  &file);
	if (!loaded)
		return false;

	if (oldest_xact_ts_out != NULL)
		*oldest_xact_ts_out = file.oldest_xact_ts;
	if (newest_xact_ts_out != NULL)
		*newest_xact_ts_out = file.newest_xact_ts;
	return true;
}

bool
fb_summary_build_candidate(const FbSummaryBuildCandidate *candidate)
{
	FbSummarySegmentEntry entry;
	FbSummarySegmentEntry next_entry;
	FbSummarySegmentEntry *next_ptr = NULL;

	if (candidate == NULL)
		return false;

	fb_runtime_ensure_initialized();

	MemSet(&entry, 0, sizeof(entry));
	strlcpy(entry.path, candidate->path, sizeof(entry.path));
	entry.timeline_id = candidate->timeline_id;
	entry.segno = candidate->segno;
	entry.bytes = candidate->bytes;
	entry.source_kind = candidate->source_kind;
	XLogFileName(entry.name, entry.timeline_id, entry.segno, candidate->wal_seg_size);

	if (candidate->has_next_segment)
	{
		MemSet(&next_entry, 0, sizeof(next_entry));
		strlcpy(next_entry.path, candidate->next_path, sizeof(next_entry.path));
		next_entry.timeline_id = candidate->next_timeline_id;
		next_entry.segno = candidate->next_segno;
		next_entry.bytes = candidate->next_bytes;
		next_entry.source_kind = candidate->source_kind;
		XLogFileName(next_entry.name, next_entry.timeline_id, next_entry.segno,
					 candidate->wal_seg_size);
		next_ptr = &next_entry;
	}

	return fb_summary_build_file_for_segment(&entry, next_ptr, candidate->wal_seg_size);
}

uint64
fb_summary_candidate_identity_hash(const FbSummaryBuildCandidate *candidate)
{
	struct stat st;

	if (candidate == NULL || stat(candidate->path, &st) != 0)
		return 0;
	return fb_summary_file_identity_hash(candidate->path, &st);
}

uint64
fb_summary_meta_summary_size_bytes(uint32 *file_count_out)
{
	char *summary_dir;
	DIR *dir;
	struct dirent *de;
	uint64 total = 0;
	uint32 count = 0;

	summary_dir = fb_runtime_meta_summary_dir();
	dir = AllocateDir(summary_dir);
	if (dir != NULL)
	{
		while ((de = ReadDir(dir, summary_dir)) != NULL)
		{
			char path[MAXPGPATH];
			struct stat st;

			if (strncmp(de->d_name, "summary-", 8) != 0)
				continue;
			snprintf(path, sizeof(path), "%s/%s", summary_dir, de->d_name);
			if (stat(path, &st) != 0)
				continue;
			total += (uint64) st.st_size;
			count++;
		}
		FreeDir(dir);
	}
	pfree(summary_dir);

	if (file_count_out != NULL)
		*file_count_out = count;
	return total;
}

int
fb_summary_build_available_debug_impl(void)
{
	FbSummaryBuildCandidate *candidates = NULL;
	int candidate_count = 0;
	int built = 0;
	int i;

	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	for (i = 0; i < candidate_count; i++)
	{
		if (fb_summary_candidate_summary_exists(&candidates[i]))
			continue;
		if (fb_summary_build_candidate(&candidates[i]))
			built++;
	}
	fb_summary_free_build_candidates(candidates);
	return built;
}

char *
fb_summary_meta_stats_cstring(void)
{
	FbSummaryMetaCounts counts;

	fb_runtime_ensure_initialized();
	fb_summary_collect_meta_counts(&counts);
	return psprintf("summary_files=%u prefilter_files=%u",
					counts.summary_files,
					counts.prefilter_files);
}

Datum
fb_summary_build_available_debug(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(fb_summary_build_available_debug_impl());
}

Datum
fb_summary_block_anchor_debug(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	FbRelationInfo info;
	FbSummaryBuildCandidate *candidates = NULL;
	FbSummaryQueryCache *cache = NULL;
	uint32 total_anchors = 0;
	int candidate_count;
	int i;

	fb_runtime_ensure_initialized();
	fb_catalog_load_relation_info(relid, &info);

	candidate_count = fb_summary_collect_build_candidates(&candidates, false);
	cache = fb_summary_query_cache_create(CurrentMemoryContext);

	PG_TRY();
	{
		for (i = 0; i < candidate_count; i++)
		{
			FbSummaryBlockAnchor *anchors = NULL;
			uint32 anchor_count = 0;

			if (!fb_summary_candidate_summary_exists(&candidates[i]))
				continue;
			if (!fb_summary_segment_lookup_block_anchors_cached(candidates[i].path,
															   candidates[i].bytes,
															   candidates[i].timeline_id,
															   candidates[i].segno,
															   candidates[i].wal_seg_size,
															   candidates[i].source_kind,
															   &info,
															   cache,
															   &anchors,
															   &anchor_count))
				continue;
			total_anchors += anchor_count;
			if (anchors != NULL)
				pfree(anchors);
		}

		fb_summary_free_build_candidates(candidates);
	}
	PG_CATCH();
	{
		if (candidates != NULL)
			fb_summary_free_build_candidates(candidates);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PG_RETURN_INT32((int32) total_anchors);
}

Datum
fb_summary_meta_stats_debug(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(fb_summary_meta_stats_cstring()));
}
