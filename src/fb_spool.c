/*
 * fb_spool.c
 *    Query-scoped spill directory and append-only spool helpers.
 */

#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>

#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/wait_event_types.h"

#include "fb_runtime.h"
#include "fb_spool.h"

#define FB_SPOOL_ANCHOR_STRIDE 1024

typedef struct FbSpoolAnchor
{
	uint32 item_index;
	off_t offset;
} FbSpoolAnchor;

struct FbSpoolLog
{
	char *path;
	File file;
	off_t size;
	uint32 item_count;
	FbSpoolAnchor *anchors;
	uint32 anchor_count;
	uint32 anchor_capacity;
	struct FbSpoolLog *next;
};

struct FbSpoolSession
{
	char *dir;
	FbSpoolLog *logs;
	uint64 serial;
};

struct FbSpoolCursor
{
	FbSpoolLog *log;
	FbSpoolDirection direction;
	off_t offset;
	uint32 next_item_index;
};

static uint64 fb_spool_session_counter = 0;

static void
fb_spool_remove_session_dir(const char *path)
{
	DIR *dir;
	struct dirent *de;

	if (path == NULL || path[0] == '\0')
		return;

	dir = AllocateDir(path);
	if (dir == NULL)
	{
		if (errno == ENOENT)
			return;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open fb spill directory"),
				 errdetail("path=%s: %m", path)));
	}

	while ((de = ReadDir(dir, path)) != NULL)
	{
		char *child_path;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		child_path = psprintf("%s/%s", path, de->d_name);
		if (unlink(child_path) != 0 && errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove fb spill file"),
					 errdetail("path=%s: %m", child_path)));
		pfree(child_path);
	}

	FreeDir(dir);
	if (rmdir(path) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove fb spill directory"),
				 errdetail("path=%s: %m", path)));
}

static void
fb_spool_append_anchor(FbSpoolLog *log)
{
	uint32 old_capacity;

	if (log == NULL)
		return;

	if (log->item_count % FB_SPOOL_ANCHOR_STRIDE != 0)
		return;

	if (log->anchor_count == log->anchor_capacity)
	{
		old_capacity = log->anchor_capacity;
		log->anchor_capacity = (log->anchor_capacity == 0) ? 16 : log->anchor_capacity * 2;
		if (log->anchors == NULL)
			log->anchors = palloc0(sizeof(FbSpoolAnchor) * log->anchor_capacity);
		else
		{
			log->anchors = repalloc(log->anchors,
									sizeof(FbSpoolAnchor) * log->anchor_capacity);
			MemSet(log->anchors + old_capacity, 0,
				   sizeof(FbSpoolAnchor) * (log->anchor_capacity - old_capacity));
		}
	}

	log->anchors[log->anchor_count].item_index = log->item_count;
	log->anchors[log->anchor_count].offset = log->size;
	log->anchor_count++;
}

static void
fb_spool_write_exact(FbSpoolLog *log, const void *data, size_t len, off_t offset)
{
	if (log == NULL || len == 0)
		return;

	if (FileWrite(log->file, data, len, offset, WAIT_EVENT_BUFFILE_WRITE) != (int) len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write fb spill file"),
				 errdetail("path=%s: %m", log->path)));
}

static void
fb_spool_read_exact(FbSpoolLog *log, void *data, size_t len, off_t offset)
{
	if (log == NULL || len == 0)
		return;

	if (FileRead(log->file, data, len, offset, WAIT_EVENT_BUFFILE_READ) != (int) len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read fb spill file"),
				 errdetail("path=%s: %m", log->path)));
}

static void
fb_spool_append_raw_bytes(FbSpoolLog *log, const void *data, size_t len)
{
	if (log == NULL || data == NULL || len == 0)
		return;

	fb_spool_write_exact(log, data, len, log->size);
	log->size += (off_t) len;
}

FbSpoolSession *
fb_spool_session_create(void)
{
	FbSpoolSession *session;
	char *runtime_dir;

	fb_runtime_ensure_initialized();
	runtime_dir = fb_runtime_runtime_dir();

	session = palloc0(sizeof(*session));
	session->dir = psprintf("%s/fbspill-%d-%llu",
							runtime_dir,
							MyProcPid,
							(unsigned long long) ++fb_spool_session_counter);
	pfree(runtime_dir);

	if (mkdir(session->dir, pg_dir_create_mode) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create fb spill directory"),
				 errdetail("path=%s: %m", session->dir)));

	return session;
}

void
fb_spool_session_destroy(FbSpoolSession *session)
{
	FbSpoolLog *log;
	FbSpoolLog *next;

	if (session == NULL)
		return;

	for (log = session->logs; log != NULL; log = log->next)
	{
		if (log->file >= 0)
		{
			FileClose(log->file);
			log->file = -1;
		}
	}

	for (log = session->logs; log != NULL; log = next)
	{
		next = log->next;
		fb_spool_log_close(log);
	}

	if (session->dir != NULL)
	{
		fb_spool_remove_session_dir(session->dir);
		pfree(session->dir);
	}

	pfree(session);
}

const char *
fb_spool_session_dir(const FbSpoolSession *session)
{
	return (session == NULL) ? NULL : session->dir;
}

FbSpoolLog *
fb_spool_log_create(FbSpoolSession *session, const char *label)
{
	FbSpoolLog *log;

	if (session == NULL || label == NULL || label[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid fb spill log parameters")));

	log = palloc0(sizeof(*log));
	log->path = psprintf("%s/%s-%llu.bin",
						 session->dir,
						 label,
						 (unsigned long long) ++session->serial);
	log->file = PathNameOpenFile(log->path, O_CREAT | O_TRUNC | O_RDWR | PG_BINARY);
	if (log->file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create fb spill file"),
				 errdetail("path=%s: %m", log->path)));

	log->next = session->logs;
	session->logs = log;
	return log;
}

FbSpoolLog *
fb_spool_log_create_path(const char *path)
{
	FbSpoolLog *log;

	if (path == NULL || path[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid fb spill log path")));

	log = palloc0(sizeof(*log));
	log->path = pstrdup(path);
	log->file = PathNameOpenFile(log->path, O_CREAT | O_TRUNC | O_RDWR | PG_BINARY);
	if (log->file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create fb spill file"),
				 errdetail("path=%s: %m", log->path)));

	return log;
}

FbSpoolLog *
fb_spool_log_open_readonly(const char *path, uint32 item_count)
{
	FbSpoolLog *log;
	struct stat st;

	if (path == NULL || path[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid fb spill log path")));

	log = palloc0(sizeof(*log));
	log->path = pstrdup(path);
	log->file = PathNameOpenFile(log->path, O_RDONLY | PG_BINARY);
	if (log->file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open fb spill file"),
				 errdetail("path=%s: %m", log->path)));
	if (stat(log->path, &st) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat fb spill file"),
				 errdetail("path=%s: %m", log->path)));

	log->size = st.st_size;
	log->item_count = item_count;
	return log;
}

void
fb_spool_log_close(FbSpoolLog *log)
{
	if (log == NULL)
		return;

	if (log->file >= 0)
	{
		FileClose(log->file);
		log->file = -1;
	}
	if (log->path != NULL)
		pfree(log->path);
	if (log->anchors != NULL)
		pfree(log->anchors);
	pfree(log);
}

const char *
fb_spool_log_path(const FbSpoolLog *log)
{
	return (log == NULL) ? NULL : log->path;
}

void
fb_spool_log_append(FbSpoolLog *log, const void *data, uint32 len)
{
	uint32 trailer = len;

	if (log == NULL)
		return;

	fb_spool_append_anchor(log);
	fb_spool_write_exact(log, &len, sizeof(len), log->size);
	if (len > 0)
		fb_spool_write_exact(log, data, len, log->size + sizeof(len));
	fb_spool_write_exact(log, &trailer, sizeof(trailer),
						 log->size + sizeof(len) + len);
	log->size += (off_t) sizeof(len) + (off_t) len + (off_t) sizeof(trailer);
	log->item_count++;
}

void
fb_spool_log_append_file(FbSpoolLog *dest, const char *path, uint32 item_count)
{
	File source;
	char buf[1 << 20];
	off_t source_size;
	off_t offset = 0;
	struct stat st;

	if (dest == NULL || path == NULL || path[0] == '\0' || item_count == 0)
		return;

	if (stat(path, &st) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat fb spill file"),
				 errdetail("path=%s: %m", path)));
	source_size = st.st_size;
	if (source_size <= 0)
		return;

	source = PathNameOpenFile(path, O_RDONLY | PG_BINARY);
	if (source < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open fb spill file"),
				 errdetail("path=%s: %m", path)));

	PG_TRY();
	{
		while (offset < source_size)
		{
			size_t chunk = (size_t) Min((off_t) sizeof(buf), source_size - offset);

			if (FileRead(source, buf, chunk, offset, WAIT_EVENT_BUFFILE_READ) != (int) chunk)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read fb spill file"),
						 errdetail("path=%s: %m", path)));
			fb_spool_append_raw_bytes(dest, buf, chunk);
			offset += (off_t) chunk;
		}
		dest->item_count += item_count;
	}
	PG_FINALLY();
	{
		FileClose(source);
	}
	PG_END_TRY();
}

void
fb_spool_log_rebuild_anchors(FbSpoolLog *log)
{
	off_t offset = 0;
	uint32 item_index = 0;

	if (log == NULL || log->item_count == 0)
		return;

	if (log->anchors != NULL)
	{
		pfree(log->anchors);
		log->anchors = NULL;
	}
	log->anchor_count = 0;
	log->anchor_capacity = 0;

	while (item_index < log->item_count)
	{
		uint32 len = 0;
		uint32 suffix = 0;

		if (item_index % FB_SPOOL_ANCHOR_STRIDE == 0)
			fb_spool_append_anchor(log);

		fb_spool_read_exact(log, &len, sizeof(len), offset);
		offset += (off_t) sizeof(len) + (off_t) len;
		fb_spool_read_exact(log, &suffix, sizeof(suffix), offset);
		if (suffix != len)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("fb spill record length trailer mismatch"),
					 errdetail("path=%s offset=%lld expected=%u actual=%u",
							   log->path,
							   (long long) (offset - sizeof(len) - len),
							   len,
							   suffix)));
		offset += (off_t) sizeof(suffix);
		item_index++;
	}

	if (offset != log->size)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("fb spill file size mismatch after rebuilding anchors"),
				 errdetail("path=%s expected=%lld actual=%lld",
						   log->path,
						   (long long) log->size,
						   (long long) offset)));
}

uint32
fb_spool_log_count(const FbSpoolLog *log)
{
	return (log == NULL) ? 0 : log->item_count;
}

off_t
fb_spool_log_size(const FbSpoolLog *log)
{
	return (log == NULL) ? 0 : log->size;
}

FbSpoolCursor *
fb_spool_cursor_open(FbSpoolLog *log, FbSpoolDirection direction)
{
	FbSpoolCursor *cursor;

	if (log == NULL)
		return NULL;

	cursor = palloc0(sizeof(*cursor));
	cursor->log = log;
	cursor->direction = direction;
	cursor->offset = (direction == FB_SPOOL_FORWARD) ? 0 : log->size;
	cursor->next_item_index = (direction == FB_SPOOL_FORWARD) ? 0 : log->item_count;
	return cursor;
}

void
fb_spool_cursor_close(FbSpoolCursor *cursor)
{
	if (cursor == NULL)
		return;

	pfree(cursor);
}

bool
fb_spool_cursor_seek_item(FbSpoolCursor *cursor, uint32 item_index)
{
	FbSpoolLog *log;
	uint32 left = 0;
	uint32 right;
	uint32 best = UINT32_MAX;
	FbSpoolAnchor *anchor = NULL;
	uint32 scratch_index;
	StringInfoData scratch;

	if (cursor == NULL || cursor->direction != FB_SPOOL_FORWARD)
		return false;

	log = cursor->log;
	if (log == NULL || item_index > log->item_count)
		return false;
	if (item_index == log->item_count)
	{
		cursor->offset = log->size;
		cursor->next_item_index = item_index;
		return true;
	}

	right = log->anchor_count;
	while (left < right)
	{
		uint32 mid = left + (right - left) / 2;

		if (log->anchors[mid].item_index <= item_index)
		{
			best = mid;
			left = mid + 1;
		}
		else
			right = mid;
	}

	if (best != UINT32_MAX)
		anchor = &log->anchors[best];

	cursor->offset = (anchor != NULL) ? anchor->offset : 0;
	cursor->next_item_index = (anchor != NULL) ? anchor->item_index : 0;
	if (cursor->next_item_index == item_index)
		return true;

	initStringInfo(&scratch);
	while (cursor->next_item_index < item_index)
	{
		if (!fb_spool_cursor_read(cursor, &scratch, &scratch_index))
			break;
	}
	pfree(scratch.data);
	return cursor->next_item_index == item_index;
}

bool
fb_spool_cursor_read(FbSpoolCursor *cursor, StringInfo buf, uint32 *item_index)
{
	FbSpoolLog *log;
	uint32 len = 0;
	uint32 suffix = 0;

	if (cursor == NULL || buf == NULL)
		return false;

	log = cursor->log;
	if (log == NULL)
		return false;

	if (cursor->direction == FB_SPOOL_FORWARD)
	{
		if (cursor->offset >= log->size || cursor->next_item_index >= log->item_count)
			return false;

		fb_spool_read_exact(log, &len, sizeof(len), cursor->offset);
		resetStringInfo(buf);
		enlargeStringInfo(buf, len);
		if (len > 0)
			fb_spool_read_exact(log, buf->data, len, cursor->offset + sizeof(len));
		buf->len = len;
		buf->data[len] = '\0';
		fb_spool_read_exact(log, &suffix, sizeof(suffix),
							cursor->offset + sizeof(len) + len);
		if (suffix != len)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("fb spill record length trailer mismatch"),
					 errdetail("path=%s offset=%lld expected=%u actual=%u",
							   log->path,
							   (long long) cursor->offset,
							   len,
							   suffix)));

		if (item_index != NULL)
			*item_index = cursor->next_item_index;
		cursor->offset += (off_t) sizeof(len) + (off_t) len + (off_t) sizeof(suffix);
		cursor->next_item_index++;
		return true;
	}

	if (cursor->offset <= 0 || cursor->next_item_index == 0)
		return false;

	fb_spool_read_exact(log, &len, sizeof(len), cursor->offset - sizeof(len));
	resetStringInfo(buf);
	enlargeStringInfo(buf, len);
	if (len > 0)
		fb_spool_read_exact(log, buf->data, len,
							cursor->offset - sizeof(len) - len);
	buf->len = len;
	buf->data[len] = '\0';
	fb_spool_read_exact(log, &suffix, sizeof(suffix),
						cursor->offset - sizeof(len) - len - sizeof(suffix));
	if (suffix != len)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("fb spill record length prefix mismatch"),
				 errdetail("path=%s offset=%lld expected=%u actual=%u",
						   log->path,
						   (long long) cursor->offset,
						   len,
						   suffix)));

	cursor->offset -= (off_t) sizeof(len) + (off_t) len + (off_t) sizeof(suffix);
	cursor->next_item_index--;
	if (item_index != NULL)
		*item_index = cursor->next_item_index;
	return true;
}
