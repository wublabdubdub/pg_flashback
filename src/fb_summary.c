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
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "catalog/storage_xlog.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/standbydefs.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "fb_ckwal.h"
#include "fb_compat.h"
#include "fb_guc.h"
#include "fb_runtime.h"
#include "fb_summary.h"

PG_FUNCTION_INFO_V1(fb_summary_build_available_debug);
PG_FUNCTION_INFO_V1(fb_summary_meta_stats_debug);

#define FB_SUMMARY_MAGIC ((uint32) 0x4642534d)
#define FB_SUMMARY_VERSION 1
#define FB_SUMMARY_LOCATOR_BLOOM_BYTES 64
#define FB_SUMMARY_RELTAG_BLOOM_BYTES 64

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
	uint32 flags;
	uint32 reserved;
	unsigned char locator_bloom[FB_SUMMARY_LOCATOR_BLOOM_BYTES];
	unsigned char reltag_bloom[FB_SUMMARY_RELTAG_BLOOM_BYTES];
} FbSummaryFileHeader;

typedef struct FbSummaryRelTag
{
	Oid db_oid;
	Oid rel_oid;
} FbSummaryRelTag;

typedef struct FbSummaryBuildState
{
	FbSummaryFileHeader file;
} FbSummaryBuildState;

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

static uint64 fb_summary_hash_bytes(uint64 seed, const void *data, Size len);
static uint64 fb_summary_file_identity_hash(const char *path, const struct stat *st);
static char *fb_summary_file_path(const char *path, const struct stat *st);
static bool fb_summary_load_file(const char *path,
								 off_t bytes,
								 TimeLineID timeline_id,
								 XLogSegNo segno,
								 int wal_seg_size,
								 int source_kind,
								 FbSummaryFileHeader *file);
static void fb_summary_bloom_add(unsigned char *bits, Size bits_len,
								 const void *data, Size len);
static bool fb_summary_bloom_maybe_contains(const unsigned char *bits, Size bits_len,
											 const void *data, Size len);
static void fb_summary_add_locator(FbSummaryBuildState *state,
								   const RelFileLocator *locator);
static void fb_summary_add_reltag(FbSummaryBuildState *state,
								  Oid db_oid,
								  Oid rel_oid);
static bool fb_summary_record_locator_from_smgr(XLogReaderState *reader,
												 RelFileLocator *locator_out);
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

static char *
fb_summary_file_path(const char *path, const struct stat *st)
{
	char *summary_dir;
	char *summary_path;
	uint64 identity_hash;

	summary_dir = fb_runtime_meta_summary_dir();
	identity_hash = fb_summary_file_identity_hash(path, st);
	summary_path = psprintf("%s/summary-%016llx.meta",
							summary_dir,
							(unsigned long long) identity_hash);
	pfree(summary_dir);
	return summary_path;
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
	char *summary_path;
	struct stat st;
	uint64 file_identity_hash;
	int fd;
	ssize_t nread;

	if (file != NULL)
		MemSet(file, 0, sizeof(*file));

	if (path == NULL || file == NULL || stat(path, &st) != 0)
		return false;

	file_identity_hash = fb_summary_file_identity_hash(path, &st);
	summary_path = fb_summary_file_path(path, &st);
	fd = open(summary_path, O_RDONLY | PG_BINARY, 0);
	pfree(summary_path);
	if (fd < 0)
		return false;

	nread = read(fd, file, sizeof(*file));
	close(fd);
	if (nread != sizeof(*file))
		return false;

	if (file->magic != FB_SUMMARY_MAGIC ||
		file->version != FB_SUMMARY_VERSION ||
		file->file_identity_hash != file_identity_hash ||
		file->timeline_id != timeline_id ||
		file->segno != segno ||
		file->wal_seg_size != (uint32) wal_seg_size ||
		file->source_kind != (uint16) source_kind)
		return false;

	return true;
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
	if (state == NULL || locator == NULL)
		return;

	fb_summary_bloom_add(state->file.locator_bloom,
						 sizeof(state->file.locator_bloom),
						 locator,
						 sizeof(*locator));
}

static void
fb_summary_add_reltag(FbSummaryBuildState *state, Oid db_oid, Oid rel_oid)
{
	FbSummaryRelTag tag;

	if (state == NULL || !OidIsValid(db_oid) || !OidIsValid(rel_oid))
		return;

	tag.db_oid = db_oid;
	tag.rel_oid = rel_oid;
	fb_summary_bloom_add(state->file.reltag_bloom,
						 sizeof(state->file.reltag_bloom),
						 &tag,
						 sizeof(tag));
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

	if (reader == NULL || state == NULL)
		return;

	rmid = XLogRecGetRmid(reader);
	state->file.flags |= (1U << Min((unsigned int) rmid, 31U));

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

		fb_summary_add_locator(state, &locator);
	}

	if (rmid == RM_HEAP_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_HEAP_TRUNCATE)
		{
			xl_heap_truncate *xlrec = (xl_heap_truncate *) XLogRecGetData(reader);
			uint32 i;

			for (i = 0; i < xlrec->nrelids; i++)
				fb_summary_add_reltag(state, xlrec->dbId, xlrec->relids[i]);
		}
	}
	else if (rmid == RM_HEAP2_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & XLOG_HEAP_OPMASK;

		if (info_code == XLOG_HEAP2_REWRITE)
		{
			xl_heap_rewrite_mapping *xlrec;

			xlrec = (xl_heap_rewrite_mapping *) XLogRecGetData(reader);
			fb_summary_add_reltag(state, xlrec->mapped_db, xlrec->mapped_rel);
		}
	}
	else if (rmid == RM_SMGR_ID)
	{
		RelFileLocator locator;

		if (fb_summary_record_locator_from_smgr(reader, &locator))
			fb_summary_add_locator(state, &locator);
	}
	else if (rmid == RM_STANDBY_ID)
	{
		uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;

		if (info_code == XLOG_STANDBY_LOCK)
		{
			xl_standby_locks *xlrec = (xl_standby_locks *) XLogRecGetData(reader);
			int i;

			for (i = 0; i < xlrec->nlocks; i++)
				fb_summary_add_reltag(state,
									  xlrec->locks[i].dbOid,
									  xlrec->locks[i].relOid);
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
	FbSummaryReaderPrivate private;
	XLogReaderState *reader;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	XLogRecPtr next_end_lsn = InvalidXLogRecPtr;
	XLogRecPtr first_record;
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

	fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
			  S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create summary file \"%s\": %m", tmp_path)));

	nwritten = write(fd, &state.file, sizeof(state.file));
	if (nwritten != sizeof(state.file))
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
fb_summary_build_candidate(const FbSummaryBuildCandidate *candidate)
{
	FbSummarySegmentEntry entry;
	FbSummarySegmentEntry next_entry;
	FbSummarySegmentEntry *next_ptr = NULL;

	if (candidate == NULL)
		return false;

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
fb_summary_meta_stats_debug(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(fb_summary_meta_stats_cstring()));
}
