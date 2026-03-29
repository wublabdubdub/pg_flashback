/*
 * fb_ckwal.c
 *    Embedded ckwal recovery helpers.
 */

#include "postgres.h"

#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/elog.h"

#include "fb_ckwal.h"
#include "fb_guc.h"
#include "fb_runtime.h"

/*
 * fb_ckwal_read_segment_header
 *    ckwal helper.
 */

static bool fb_ckwal_read_segment_header(const char *path,
										  TimeLineID *timeline_id,
										  XLogSegNo *segno,
										  int wal_seg_size);
/*
 * fb_ckwal_find_segment_in_directory
 *    ckwal helper.
 */

static bool fb_ckwal_find_segment_in_directory(const char *directory,
											   TimeLineID timeline_id,
											   XLogSegNo segno,
											   int wal_seg_size,
											   char *candidate_path,
											   Size candidate_path_len);
/*
 * fb_ckwal_copy_file
 *    ckwal helper.
 */

static bool fb_ckwal_copy_file(const char *src_path, const char *dst_path);
/*
 * fb_ckwal_materialize_segment
 *    ckwal helper.
 */

static bool fb_ckwal_materialize_segment(const char *src_path,
										 int wal_seg_size,
										 TimeLineID *timeline_id,
										 XLogSegNo *segno,
										 char *out_path,
										 Size out_path_len);

/*
 * fb_ckwal_read_segment_header
 *    ckwal helper.
 */

static bool
fb_ckwal_read_segment_header(const char *path,
							 TimeLineID *timeline_id,
							 XLogSegNo *segno,
							 int wal_seg_size)
{
	PGAlignedXLogBlock buf;
	FILE *fp;
	size_t bytes_read;
	XLogLongPageHeader longhdr;

	fp = AllocateFile(path, PG_BINARY_R);
	if (fp == NULL)
		return false;

	bytes_read = fread(buf.data, 1, XLOG_BLCKSZ, fp);
	FreeFile(fp);
	if (bytes_read != XLOG_BLCKSZ)
		return false;

	longhdr = (XLogLongPageHeader) buf.data;
	if ((longhdr->std.xlp_info & XLP_LONG_HEADER) == 0)
		return false;
	if (longhdr->xlp_seg_size != wal_seg_size)
		return false;
	if (longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		return false;
	if (timeline_id != NULL)
		*timeline_id = longhdr->std.xlp_tli;
	if (segno != NULL)
		XLByteToSeg(longhdr->std.xlp_pageaddr, *segno, wal_seg_size);

	return true;
}

/*
 * fb_ckwal_find_segment_in_directory
 *    ckwal helper.
 */

static bool
fb_ckwal_find_segment_in_directory(const char *directory,
								   TimeLineID timeline_id,
								   XLogSegNo segno,
								   int wal_seg_size,
								   char *candidate_path,
								   Size candidate_path_len)
{
	DIR *dir;
	struct dirent *de;

	dir = AllocateDir(directory);
	if (dir == NULL)
		return false;

	while ((de = ReadDir(dir, directory)) != NULL)
	{
		char path[MAXPGPATH];
		struct stat st;
		TimeLineID file_tli;
		XLogSegNo file_segno;

		if (!IsXLogFileName(de->d_name))
			continue;

		snprintf(path, sizeof(path), "%s/%s", directory, de->d_name);
		if (stat(path, &st) != 0 || !S_ISREG(st.st_mode))
			continue;

		if (!fb_ckwal_read_segment_header(path, &file_tli, &file_segno,
										  wal_seg_size))
			continue;

		if (file_tli != timeline_id || file_segno != segno)
			continue;

		strlcpy(candidate_path, path, candidate_path_len);
		FreeDir(dir);
		return true;
	}

	FreeDir(dir);
	return false;
}

/*
 * fb_ckwal_copy_file
 *    ckwal helper.
 */

static bool
fb_ckwal_copy_file(const char *src_path, const char *dst_path)
{
	struct stat st;
	char temp_path[MAXPGPATH];

	if (stat(src_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0)
		return false;

	if (stat(dst_path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0)
		return true;

	snprintf(temp_path, sizeof(temp_path), "%s.tmp.%d", dst_path, MyProcPid);
	(void) unlink(temp_path);
	copy_file(src_path, temp_path);

	if (stat(temp_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0)
		return false;

	if (rename(temp_path, dst_path) != 0)
	{
		int save_errno = errno;

		(void) unlink(temp_path);
		if (save_errno != EEXIST)
			return false;
	}

	if (stat(dst_path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size <= 0)
		return false;

	return true;
}

/*
 * fb_ckwal_materialize_segment
 *    ckwal helper.
 */

static bool
fb_ckwal_materialize_segment(const char *src_path,
							 int wal_seg_size,
							 TimeLineID *timeline_id,
							 XLogSegNo *segno,
							 char *out_path,
							 Size out_path_len)
{
	char *restore_dir;
	char fname[MAXPGPATH];
	TimeLineID actual_tli;
	XLogSegNo actual_segno;
	struct stat st;

	if (!fb_ckwal_read_segment_header(src_path, &actual_tli, &actual_segno,
									  wal_seg_size))
		return false;

	fb_runtime_ensure_initialized();
	restore_dir = fb_runtime_recovered_wal_dir();
	XLogFileName(fname, actual_tli, actual_segno, wal_seg_size);
	snprintf(out_path, out_path_len, "%s/%s", restore_dir, fname);
	pfree(restore_dir);

	if (stat(out_path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0)
	{
		if (timeline_id != NULL)
			*timeline_id = actual_tli;
		if (segno != NULL)
			*segno = actual_segno;
		return true;
	}

	if (!fb_ckwal_copy_file(src_path, out_path))
		return false;

	if (timeline_id != NULL)
		*timeline_id = actual_tli;
	if (segno != NULL)
		*segno = actual_segno;
	return true;
}

/*
 * fb_ckwal_restore_segment
 *    ckwal entry point.
 */

bool
fb_ckwal_restore_segment(TimeLineID timeline_id,
						 XLogSegNo segno,
						 int wal_seg_size,
						 char *out_path,
						 Size out_path_len)
{
	char *restore_dir;
	char *archive_dir = NULL;
	char *pg_wal_dir = NULL;
	char *recovered_search_dir = NULL;
	char fname[MAXPGPATH];
	char candidate[MAXPGPATH];
	const char *search_dirs[3];
	int i;
	struct stat st;

	fb_runtime_ensure_initialized();
	restore_dir = fb_runtime_recovered_wal_dir();
	XLogFileName(fname, timeline_id, segno, wal_seg_size);
	snprintf(out_path, out_path_len, "%s/%s", restore_dir, fname);
	pfree(restore_dir);

	if (stat(out_path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0)
		return true;

	archive_dir = fb_get_effective_archive_dir();
	pg_wal_dir = fb_get_pg_wal_dir();
	recovered_search_dir = fb_runtime_recovered_wal_dir();

	search_dirs[0] = archive_dir;
	search_dirs[1] = pg_wal_dir;
	search_dirs[2] = recovered_search_dir;

	for (i = 0; i < lengthof(search_dirs); i++)
	{
		const char *dirpath = search_dirs[i];
		char exact_path[MAXPGPATH];
		TimeLineID file_tli;
		XLogSegNo file_segno;

		if (dirpath == NULL || dirpath[0] == '\0')
			continue;

		snprintf(exact_path, sizeof(exact_path), "%s/%s", dirpath, fname);
		if (fb_ckwal_read_segment_header(exact_path, &file_tli, &file_segno,
										 wal_seg_size) &&
			file_tli == timeline_id &&
			file_segno == segno &&
			fb_ckwal_copy_file(exact_path, out_path))
			goto success;

		if (fb_ckwal_find_segment_in_directory(dirpath, timeline_id, segno,
											   wal_seg_size, candidate,
											   sizeof(candidate)) &&
			fb_ckwal_copy_file(candidate, out_path))
			goto success;
	}

	if (archive_dir != NULL)
		pfree(archive_dir);
	if (pg_wal_dir != NULL)
		pfree(pg_wal_dir);
	if (recovered_search_dir != NULL)
		pfree(recovered_search_dir);
	return false;

	success:
	if (archive_dir != NULL)
		pfree(archive_dir);
	if (pg_wal_dir != NULL)
		pfree(pg_wal_dir);
	if (recovered_search_dir != NULL)
		pfree(recovered_search_dir);
	return true;
}

/*
 * fb_ckwal_convert_mismatched_segment
 *    ckwal entry point.
 */

bool
fb_ckwal_convert_mismatched_segment(const char *src_path,
									int wal_seg_size,
									TimeLineID *timeline_id,
									XLogSegNo *segno,
									char *out_path,
									Size out_path_len)
{
	if (src_path == NULL || src_path[0] == '\0')
		return false;

	return fb_ckwal_materialize_segment(src_path, wal_seg_size,
										timeline_id, segno,
										out_path, out_path_len);
}
