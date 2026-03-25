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
extern void *memmem(const void *haystack, size_t haystacklen,
					const void *needle, size_t needlelen);
#endif

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

typedef struct FbWalReaderPrivate
{
	TimeLineID timeline_id;
	XLogRecPtr endptr;
	bool endptr_reached;
	XLogSegNo last_open_segno;
	bool last_open_segno_valid;
	FbWalScanContext *ctx;
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

typedef struct FbWalPayloadArenaChunk
{
	struct FbWalPayloadArenaChunk *next;
	Size capacity;
	Size used;
	char data[FLEXIBLE_ARRAY_MEMBER];
} FbWalPayloadArenaChunk;

typedef struct FbWalPayloadArena
{
	FbWalPayloadArenaChunk *head;
	FbWalPayloadArenaChunk *tail;
} FbWalPayloadArena;

typedef struct FbWalPrefilterPattern
{
	char bytes[sizeof(RelFileLocator)];
	Size len;
	unsigned char skip[256];
} FbWalPrefilterPattern;

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

typedef struct FbWalVisitWindow
{
	FbWalSegmentEntry *segments;
	uint32 segment_count;
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
} FbWalVisitWindow;

typedef struct FbPrefilterCacheEntry
{
	uint64 key;
	bool hit;
} FbPrefilterCacheEntry;

static void fb_mark_unsafe(FbWalScanContext *ctx, FbWalUnsafeReason reason);
static TransactionId fb_record_xid(XLogReaderState *reader);
static bool fb_locator_matches_relation(const RelFileLocator *locator,
										const FbRelationInfo *info);
static bool fb_standby_record_matches_relation(XLogReaderState *reader,
											   const FbRelationInfo *info,
											   TransactionId *matched_xid);
static bool fb_record_touches_relation(XLogReaderState *reader,
									   const FbRelationInfo *info);
static void fb_index_charge_bytes(FbWalRecordIndex *index, Size bytes,
								  const char *what);
static const char *fb_wal_source_name(FbWalSourceKind source_kind);
static bool fb_validate_segment_identity(FbWalSegmentEntry *entry,
										 int wal_seg_size);
static bool fb_try_ckwal_segment(FbWalScanContext *ctx,
								 TimeLineID timeline_id,
								 XLogSegNo segno,
								 int wal_seg_size,
								 FbWalSegmentEntry *entry);
static bool fb_convert_mismatched_pg_wal_entry(FbWalScanContext *ctx,
												   FbWalSegmentEntry *entry,
												   FbWalSegmentEntry *converted);
static bool fb_record_is_speculative_insert(const FbRecordRef *record);
static bool fb_record_is_super_delete(const FbRecordRef *record);
static XLogRecPtr fb_segment_start_lsn(const FbWalSegmentEntry *entry,
									   int wal_seg_size);
static XLogRecPtr fb_segment_end_lsn(const FbWalSegmentEntry *entry,
									 int wal_seg_size);
static uint64 fb_prefilter_hash_bytes(uint64 seed, const void *data, Size len);
static uint64 fb_prefilter_patterns_hash(FbWalPrefilterPattern *patterns,
										 int pattern_count);
static uint64 fb_prefilter_cache_key(const FbWalSegmentEntry *entry,
									 uint64 pattern_hash);
static bool fb_prefilter_cache_lookup_memory(uint64 key, bool *hit);
static void fb_prefilter_cache_store_memory(uint64 key, bool hit);
static void fb_prepare_segment_prefilter(const FbRelationInfo *info,
										 FbWalScanContext *ctx);
static void fb_wal_visit_window(FbWalScanContext *ctx,
								const FbWalVisitWindow *window,
								FbWalRecordVisitor visitor,
								void *arg);

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

static FbWalPayloadArena *
fb_get_payload_arena(FbWalRecordIndex *index)
{
	if (index == NULL)
		return NULL;

	if (index->payload_arena == NULL)
		index->payload_arena = palloc0(sizeof(FbWalPayloadArena));

	return (FbWalPayloadArena *) index->payload_arena;
}

static char *
fb_payload_arena_alloc(FbWalRecordIndex *index, Size len)
{
	FbWalPayloadArena *arena;
	FbWalPayloadArenaChunk *chunk;
	Size chunk_capacity;
	char *ptr;

	if (index == NULL || len == 0)
		return NULL;

	arena = fb_get_payload_arena(index);
	chunk = arena->tail;

	if (chunk == NULL || chunk->capacity - chunk->used < len)
	{
		chunk_capacity = Max((Size) (64 * 1024), MAXALIGN(len));
		chunk = palloc(sizeof(FbWalPayloadArenaChunk) + chunk_capacity);
		chunk->next = NULL;
		chunk->capacity = chunk_capacity;
		chunk->used = 0;

		if (arena->tail == NULL)
			arena->head = chunk;
		else
			arena->tail->next = chunk;
		arena->tail = chunk;
	}

	ptr = chunk->data + chunk->used;
	chunk->used += MAXALIGN(len);
	return ptr;
}

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

static bool
fb_bytes_contains_pattern(const unsigned char *buf, Size buf_len,
						  const FbWalPrefilterPattern *pattern)
{
	if (buf == NULL || pattern == NULL || pattern->len == 0 ||
		buf_len < pattern->len)
		return false;

	return memmem(buf, buf_len, pattern->bytes, pattern->len) != NULL;
}

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
	int cache_fd = -1;
	char cache_value = '\0';
	int i;

	if (pattern_count <= 0)
		return true;

	cache_path[0] = '\0';

	if (meta_dir != NULL && meta_dir[0] != '\0' && stat(path, &st) == 0)
	{
		cache_hash = UINT64CONST(1469598103934665603);
		cache_hash = fb_prefilter_hash_bytes(cache_hash, path, strlen(path));
		cache_hash = fb_prefilter_hash_bytes(cache_hash, &st.st_mtime, sizeof(st.st_mtime));
		cache_hash = fb_prefilter_hash_bytes(cache_hash, &st.st_size, sizeof(st.st_size));
		cache_hash = fb_prefilter_hash_bytes(cache_hash, &pattern_hash, sizeof(pattern_hash));
		snprintf(cache_path, sizeof(cache_path), "%s/prefilter-%016llx.meta",
				 meta_dir, (unsigned long long) cache_hash);

		cache_fd = open(cache_path, O_RDONLY | PG_BINARY, 0);
		if (cache_fd >= 0)
		{
			if (read(cache_fd, &cache_value, 1) == 1)
			{
				close(cache_fd);
				return cache_value == '1';
			}
			close(cache_fd);
		}
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
			{
				cache_fd = open(cache_path,
								O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
								S_IRUSR | S_IWUSR);
				if (cache_fd >= 0)
				{
					cache_value = '1';
					write(cache_fd, &cache_value, 1);
					close(cache_fd);
				}
			}
			munmap(map, st.st_size);
			return true;
		}
	}

	if (cache_path[0] != '\0')
	{
		cache_fd = open(cache_path,
						O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY,
						S_IRUSR | S_IWUSR);
		if (cache_fd >= 0)
		{
			cache_value = '0';
			write(cache_fd, &cache_value, 1);
			close(cache_fd);
		}
	}

	munmap(map, st.st_size);
	return false;
}

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

	if (locator->spcOid == MyDatabaseTableSpace || MyDatabaseTableSpace == InvalidOid)
		return;

	alt_locator = *locator;
	alt_locator.spcOid = MyDatabaseTableSpace;
	memcpy(patterns[*pattern_count].bytes, &alt_locator, sizeof(RelFileLocator));
	patterns[*pattern_count].len = sizeof(RelFileLocator);
	fb_prefilter_pattern_init(&patterns[*pattern_count]);
	(*pattern_count)++;
}

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

static uint32
fb_build_prefilter_visit_windows(FbWalScanContext *ctx, FbWalVisitWindow **windows_out)
{
	FbWalSegmentEntry *segments;
	bool *selected;
	FbWalVisitWindow *windows;
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
	 * Always visit the leading segment so scan/debug paths still have a chance
	 * to discover the earliest checkpoint anchor. Around each hit segment, add
	 * one neighbor on both sides to keep cross-segment record decoding safe.
	 */
	selected[0] = true;
	selected_count++;
	if (ctx->resolved_segment_count > 1)
	{
		selected[1] = true;
		selected_count++;
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
			(start == 0) ? ctx->start_lsn :
			fb_segment_start_lsn(&segments[start], ctx->wal_seg_size);
		windows[window_count].end_lsn =
			(end == ctx->resolved_segment_count - 1) ? ctx->end_lsn :
			fb_segment_end_lsn(&segments[end], ctx->wal_seg_size);
		window_count++;
		i = end;
	}

	pfree(selected);
	*windows_out = windows;
	return window_count;
}

static int
fb_open_file_at_path(const char *path)
{
	int fd;

	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	return fd;
}

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

static FbWalSegmentEntry *
fb_find_segment_by_segno(FbWalSegmentEntry *segments, int segment_count,
						 XLogSegNo segno)
{
	int i;

	for (i = 0; i < segment_count; i++)
	{
		if (segments[i].segno == segno)
			return &segments[i];
	}

	return NULL;
}

static XLogRecPtr
fb_segment_start_lsn(const FbWalSegmentEntry *entry, int wal_seg_size)
{
	XLogRecPtr lsn;

	if (entry == NULL)
		return InvalidXLogRecPtr;

	XLogSegNoOffsetToRecPtr(entry->segno, 0, wal_seg_size, lsn);
	return lsn;
}

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

static void
fb_wal_open_segment(XLogReaderState *state, XLogSegNo next_segno,
					TimeLineID *timeline_id)
{
	FbWalReaderPrivate *private = (FbWalReaderPrivate *) state->private_data;
	FbWalScanContext *ctx = private->ctx;
	FbWalSegmentEntry *entry;
	FbWalSegmentEntry *segments;

	entry = fb_find_segment_by_segno((FbWalSegmentEntry *) ctx->resolved_segments,
									 ctx->resolved_segment_count,
									 next_segno);
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: missing segment for segno %llu",
						(unsigned long long) next_segno)));

	*timeline_id = entry->timeline_id;
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
		ctx->visited_segment_count++;
		fb_progress_update_fraction(FB_PROGRESS_STAGE_BUILD_INDEX,
									 ctx->visited_segment_count,
									 ctx->progress_segment_total,
									 NULL);
	}
	state->seg.ws_file = fb_open_file_at_path(entry->path);
	if (state->seg.ws_file < 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: missing segment %s in %s",
						entry->name, entry->path)));
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

static char *
fb_copy_bytes(FbWalRecordIndex *index, const char *data, Size len)
{
	char *copy;

	if (data == NULL || len == 0)
		return NULL;

	copy = fb_payload_arena_alloc(index, len);
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
	block_ref->image = fb_copy_bytes(index, page, BLCKSZ);
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
	block_ref->is_toast_relation = info->has_toast_locator &&
		RelFileLocatorEquals(locator, info->toast_locator);

	if (XLogRecHasBlockData(reader, block_id))
	{
		data = XLogRecGetBlockData(reader, block_id, &datalen);
		block_ref->has_data = (data != NULL && datalen > 0);
			if (block_ref->has_data)
			{
				fb_index_charge_bytes(index, datalen, "block data");
				block_ref->data = fb_copy_bytes(index, data, datalen);
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
	record.main_data = fb_copy_bytes(index, XLogRecGetData(reader),
									 XLogRecGetDataLen(reader));
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

static void
fb_copy_xlog_fpi_record_ref(XLogReaderState *reader, const FbRelationInfo *info,
							FbWalRecordKind kind, FbWalRecordIndex *index)
{
	FbRecordRef record;
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

static bool
fb_standby_record_matches_relation(XLogReaderState *reader,
								   const FbRelationInfo *info,
								   TransactionId *matched_xid)
{
	uint8 info_code = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
	xl_standby_locks *xlrec;
	int i;

	if (matched_xid != NULL)
		*matched_xid = InvalidTransactionId;

	if (info_code != XLOG_STANDBY_LOCK)
		return false;

	xlrec = (xl_standby_locks *) XLogRecGetData(reader);
	for (i = 0; i < xlrec->nlocks; i++)
	{
		xl_standby_lock *lock = &xlrec->locks[i];

		if (lock->dbOid != info->locator.dbOid)
			continue;
		if (lock->relOid != info->relid &&
			(!OidIsValid(info->toast_relid) || lock->relOid != info->toast_relid))
			continue;

		if (matched_xid != NULL)
			*matched_xid = lock->xid;
		return true;
	}

	return false;
}

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

	if (ctx->segment_prefilter_used && !ctx->current_segment_may_hit)
	{
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
		else if (rmid == RM_STANDBY_ID)
		{
			TransactionId lock_xid = InvalidTransactionId;

			if (fb_standby_record_matches_relation(reader, info, &lock_xid))
				fb_mark_xid_unsafe(unsafe_xids, lock_xid,
								   FB_WAL_UNSAFE_STORAGE_CHANGE, ctx);
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
	else if (rmid == RM_XLOG_ID)
	{
		/* checkpoint records are already handled above; nothing else to do here */
	}
	else if (rmid == RM_SMGR_ID)
	{
		if (fb_smgr_record_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_STORAGE_CHANGE, ctx);
	}
	else if (rmid == RM_STANDBY_ID)
	{
		TransactionId lock_xid = InvalidTransactionId;

		if (fb_standby_record_matches_relation(reader, info, &lock_xid))
			fb_mark_xid_unsafe(unsafe_xids, lock_xid,
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

	if (ctx->segment_prefilter_used && !ctx->current_segment_may_hit)
	{
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
		else if (rmid == RM_STANDBY_ID)
		{
			TransactionId lock_xid = InvalidTransactionId;

			if (fb_standby_record_matches_relation(reader, info, &lock_xid))
				fb_mark_xid_unsafe(unsafe_xids, lock_xid,
								   FB_WAL_UNSAFE_STORAGE_CHANGE, ctx);
		}

		if (rmid == RM_XACT_ID)
			fb_note_xact_status_for_touched(reader, touched_xids, unsafe_xids,
											index, ctx);
		return !ctx->unsafe;
	}

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
				case XLOG_HEAP_CONFIRM:
					fb_copy_heap_record_ref(reader, info,
										 FB_WAL_RECORD_HEAP_CONFIRM, index);
					break;
				case XLOG_HEAP_LOCK:
					fb_copy_heap_record_ref(reader, info,
										 FB_WAL_RECORD_HEAP_LOCK, index);
					break;
				case XLOG_HEAP_INPLACE:
					fb_copy_heap_record_ref(reader, info,
										 FB_WAL_RECORD_HEAP_INPLACE, index);
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
		else if ((info_code == XLOG_HEAP2_PRUNE_ON_ACCESS ||
				  info_code == XLOG_HEAP2_PRUNE_VACUUM_SCAN ||
				  info_code == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP) &&
				 fb_record_touches_relation(reader, info))
		{
			fb_mark_record_xids_touched(reader, touched_xids, ctx);
			fb_copy_heap_record_ref(reader, info,
								 FB_WAL_RECORD_HEAP2_PRUNE, index);
		}
		else if (info_code == XLOG_HEAP2_VISIBLE &&
				 fb_record_touches_relation(reader, info))
		{
			fb_mark_record_xids_touched(reader, touched_xids, ctx);
			fb_copy_heap_record_ref(reader, info,
								 FB_WAL_RECORD_HEAP2_VISIBLE, index);
		}
		else if (info_code == XLOG_HEAP2_MULTI_INSERT &&
				 fb_record_touches_relation(reader, info))
		{
			fb_mark_record_xids_touched(reader, touched_xids, ctx);
			fb_copy_heap_record_ref(reader, info,
								 FB_WAL_RECORD_HEAP2_MULTI_INSERT, index);
		}
			else if (info_code == XLOG_HEAP2_LOCK_UPDATED &&
					 fb_record_touches_relation(reader, info))
			{
				fb_mark_record_xids_touched(reader, touched_xids, ctx);
				fb_copy_heap_record_ref(reader, info,
									 FB_WAL_RECORD_HEAP2_LOCK_UPDATED, index);
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
			fb_copy_xlog_fpi_record_ref(reader, info,
										info_code == XLOG_FPI ?
										FB_WAL_RECORD_XLOG_FPI :
										FB_WAL_RECORD_XLOG_FPI_FOR_HINT,
										index);
		}
	}
	else if (rmid == RM_SMGR_ID)
	{
		if (fb_smgr_record_matches_relation(reader, info))
			fb_mark_xid_unsafe(unsafe_xids, fb_record_xid(reader),
							   FB_WAL_UNSAFE_STORAGE_CHANGE, ctx);
	}
	else if (rmid == RM_STANDBY_ID)
	{
		TransactionId lock_xid = InvalidTransactionId;

		if (fb_standby_record_matches_relation(reader, info, &lock_xid))
			fb_mark_xid_unsafe(unsafe_xids, lock_xid,
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
	FbWalVisitWindow window;

	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");

	window.segments = (FbWalSegmentEntry *) ctx->resolved_segments;
	window.segment_count = ctx->resolved_segment_count;
	window.start_lsn = ctx->start_lsn;
	window.end_lsn = ctx->end_lsn;

	fb_wal_visit_window(ctx, &window, visitor, arg);
}

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
	private.ctx = ctx;

	reader = XLogReaderAllocate(ctx->wal_seg_size, fb_get_effective_archive_dir(),
								XL_ROUTINE(.page_read = fb_wal_read_page,
										   .segment_open = fb_wal_open_segment,
										   .segment_close = fb_wal_close_segment),
								&private);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	first_record = XLogFindNextRecord(reader, window->start_lsn);
	if (XLogRecPtrIsInvalid(first_record))
	{
		XLogReaderFree(reader);
		goto done;
	}

	if (XLogRecPtrIsInvalid(ctx->first_record_lsn))
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

done:
	ctx->resolved_segments = saved_segments;
	ctx->resolved_segment_count = saved_segment_count;
	ctx->segment_hit_map = saved_hit_map;
	ctx->segment_prefilter_used = saved_prefilter_used;
	ctx->current_segment_may_hit = saved_current_segment_may_hit;
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
	ctx->parallel_segment_scan_enabled = fb_parallel_segment_scan_enabled();
	ctx->current_segment_may_hit = true;

	fb_collect_archive_segments(ctx);
}

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

void
fb_wal_build_record_index(const FbRelationInfo *info,
						  FbWalScanContext *ctx,
						  FbWalRecordIndex *index)
{
	FbWalIndexBuildState state;
	FbWalVisitWindow *windows = NULL;
	uint32		window_count = 0;
	uint32 i;

	if (info == NULL)
		elog(ERROR, "FbRelationInfo must not be NULL");
	if (ctx == NULL)
		elog(ERROR, "FbWalScanContext must not be NULL");
	if (index == NULL)
		elog(ERROR, "FbWalRecordIndex must not be NULL");

	fb_progress_enter_stage(FB_PROGRESS_STAGE_BUILD_INDEX, NULL);

	MemSet(index, 0, sizeof(*index));
	index->memory_limit_bytes = fb_get_memory_limit_bytes();
	index->xid_statuses = fb_create_xid_status_hash();

	MemSet(&state, 0, sizeof(state));
	state.info = info;
	state.ctx = ctx;
	state.index = index;
	state.touched_xids = fb_create_touched_xid_hash();
	state.unsafe_xids = fb_create_unsafe_xid_hash();

	fb_prepare_segment_prefilter(info, ctx);
	window_count = fb_build_prefilter_visit_windows(ctx, &windows);
	ctx->visited_segment_count = 0;
	ctx->progress_segment_total = 0;
	if (window_count == 0)
		ctx->progress_segment_total = ctx->resolved_segment_count;
	else
	{
		for (i = 0; i < window_count; i++)
			ctx->progress_segment_total += windows[i].segment_count;
	}
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
		pfree(windows);
	}

	if (!ctx->anchor_found)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("WAL not complete: no checkpoint before target timestamp")));

	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_INDEX, 100, NULL);

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
			case FB_WAL_RECORD_HEAP_CONFIRM:
			case FB_WAL_RECORD_HEAP_LOCK:
			case FB_WAL_RECORD_HEAP_INPLACE:
			case FB_WAL_RECORD_HEAP2_PRUNE:
			case FB_WAL_RECORD_HEAP2_VISIBLE:
			case FB_WAL_RECORD_HEAP2_MULTI_INSERT:
			case FB_WAL_RECORD_HEAP2_LOCK_UPDATED:
			case FB_WAL_RECORD_XLOG_FPI:
			case FB_WAL_RECORD_XLOG_FPI_FOR_HINT:
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
