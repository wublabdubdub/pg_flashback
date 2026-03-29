/*
 * fb_runtime.c
 *    Runtime directory initialization helpers.
 */

#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "miscadmin.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/elog.h"

#include "fb_guc.h"
#include "fb_runtime.h"

#define FB_RUNTIME_CLEANUP_MIN_INTERVAL_SEC 60

static time_t fb_runtime_last_cleanup_at = 0;

/*
 * fb_runtime_join_path
 *    Runtime helper.
 */

static char *
fb_runtime_join_path(const char *suffix)
{
	return psprintf("%s/pg_flashback/%s", DataDir, suffix);
}

static bool
fb_runtime_pid_is_live(pid_t pid)
{
	if (pid <= 0)
		return false;

	if (kill(pid, 0) == 0)
		return true;

	return errno != ESRCH;
}

static bool
fb_runtime_parse_spool_pid(const char *name, pid_t *pid_out)
{
	long pid_value;
	char *endptr = NULL;

	if (name == NULL || strncmp(name, "fbspill-", 8) != 0)
		return false;

	pid_value = strtol(name + 8, &endptr, 10);
	if (endptr == name + 8 || endptr == NULL || *endptr != '-')
		return false;

	if (pid_out != NULL)
		*pid_out = (pid_t) pid_value;
	return true;
}

static bool
fb_runtime_entry_is_expired(const struct stat *st, time_t now, int retention_seconds)
{
	if (st == NULL || retention_seconds <= 0 || now <= 0)
		return false;

	return (now - st->st_mtime) >= retention_seconds;
}

static bool
fb_runtime_remove_path(const char *path, bool is_dir)
{
	if (path == NULL || path[0] == '\0')
		return false;

	errno = 0;
	if (is_dir)
	{
		if (rmtree(path, true))
			return true;
		if (errno == ENOENT)
			return false;
	}
	else
	{
		if (unlink(path) == 0)
			return true;
		if (errno == ENOENT)
			return false;
	}

	ereport(WARNING,
			(errcode_for_file_access(),
			 errmsg("could not remove pg_flashback runtime path"),
			 errdetail("path=%s: %m", path)));
	return false;
}

static void
fb_runtime_cleanup_runtime_dir(const char *runtime_dir,
							   time_t now,
							   int retention_seconds,
							   FbRuntimeCleanupStats *stats)
{
	DIR *dir;
	struct dirent *entry;

	dir = AllocateDir(runtime_dir);
	if (dir == NULL)
		return;

	while ((entry = ReadDir(dir, runtime_dir)) != NULL)
	{
		char path[MAXPGPATH];
		struct stat st;
		pid_t owner_pid = 0;
		bool has_pid = false;
		bool is_dir;
		bool remove = false;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", runtime_dir, entry->d_name);
		if (lstat(path, &st) != 0)
			continue;

		is_dir = S_ISDIR(st.st_mode);
		has_pid = fb_runtime_parse_spool_pid(entry->d_name, &owner_pid);

		if (has_pid)
		{
			if (!fb_runtime_pid_is_live(owner_pid))
				remove = true;
			else
				continue;
		}
		else if (!fb_runtime_entry_is_expired(&st, now, retention_seconds))
			continue;
		else
			remove = true;

		if (!remove)
			continue;

		if (fb_runtime_remove_path(path, is_dir) && stats != NULL)
			stats->runtime_removed++;
	}

	FreeDir(dir);
}

static void
fb_runtime_cleanup_flat_dir(const char *dir_path,
							time_t now,
							int retention_seconds,
							uint32 *removed_count)
{
	DIR *dir;
	struct dirent *entry;

	if (retention_seconds <= 0)
		return;

	dir = AllocateDir(dir_path);
	if (dir == NULL)
		return;

	while ((entry = ReadDir(dir, dir_path)) != NULL)
	{
		char path[MAXPGPATH];
		struct stat st;
		bool is_dir;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", dir_path, entry->d_name);
		if (lstat(path, &st) != 0)
			continue;
		if (!fb_runtime_entry_is_expired(&st, now, retention_seconds))
			continue;

		is_dir = S_ISDIR(st.st_mode);
		if (!(is_dir || S_ISREG(st.st_mode) || S_ISLNK(st.st_mode)))
			continue;

		if (fb_runtime_remove_path(path, is_dir) && removed_count != NULL)
			(*removed_count)++;
	}

	FreeDir(dir);
}

/*
 * fb_runtime_base_dir
 *    Runtime entry point.
 */

char *
fb_runtime_base_dir(void)
{
	return psprintf("%s/pg_flashback", DataDir);
}

/*
 * fb_runtime_runtime_dir
 *    Runtime entry point.
 */

char *
fb_runtime_runtime_dir(void)
{
	return fb_runtime_join_path("runtime");
}

/*
 * fb_runtime_recovered_wal_dir
 *    Runtime entry point.
 */

char *
fb_runtime_recovered_wal_dir(void)
{
	return fb_runtime_join_path("recovered_wal");
}

/*
 * fb_runtime_meta_dir
 *    Runtime entry point.
 */

char *
fb_runtime_meta_dir(void)
{
	return fb_runtime_join_path("meta");
}

/*
 * fb_runtime_ensure_directory
 *    Runtime helper.
 */

static void
fb_runtime_ensure_directory(const char *path, const char *label)
{
	char *mutable_path;

	mutable_path = pstrdup(path);
	if (pg_mkdir_p(mutable_path, pg_dir_create_mode) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not initialize pg_flashback %s directory", label),
				 errdetail("path=%s", path)));
	pfree(mutable_path);

	if (access(path, W_OK | X_OK) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_flashback %s directory is not writable", label),
				 errdetail("path=%s: %m", path)));
}

/*
 * fb_runtime_ensure_initialized
 *    Runtime entry point.
 */

void
fb_runtime_ensure_initialized(void)
{
	char *base_dir;
	char *runtime_dir;
	char *recovered_wal_dir;
	char *meta_dir;

	base_dir = fb_runtime_base_dir();
	runtime_dir = fb_runtime_runtime_dir();
	recovered_wal_dir = fb_runtime_recovered_wal_dir();
	meta_dir = fb_runtime_meta_dir();

	fb_runtime_ensure_directory(base_dir, "base");
	fb_runtime_ensure_directory(runtime_dir, "runtime");
	fb_runtime_ensure_directory(recovered_wal_dir, "recovered_wal");
	fb_runtime_ensure_directory(meta_dir, "meta");
	fb_runtime_cleanup(false, NULL);

	pfree(base_dir);
	pfree(runtime_dir);
	pfree(recovered_wal_dir);
	pfree(meta_dir);
}

void
fb_runtime_cleanup(bool force, FbRuntimeCleanupStats *stats)
{
	char *runtime_dir;
	char *recovered_wal_dir;
	char *meta_dir;
	time_t now;

	if (stats != NULL)
		MemSet(stats, 0, sizeof(*stats));

	now = time(NULL);
	if (!force &&
		fb_runtime_last_cleanup_at > 0 &&
		now > 0 &&
		(now - fb_runtime_last_cleanup_at) < FB_RUNTIME_CLEANUP_MIN_INTERVAL_SEC)
		return;

	runtime_dir = fb_runtime_runtime_dir();
	recovered_wal_dir = fb_runtime_recovered_wal_dir();
	meta_dir = fb_runtime_meta_dir();

	fb_runtime_cleanup_runtime_dir(runtime_dir,
								   now,
								   fb_runtime_retention_seconds(),
								   stats);
	fb_runtime_cleanup_flat_dir(recovered_wal_dir,
								now,
								fb_recovered_wal_retention_seconds(),
								stats != NULL ? &stats->recovered_wal_removed : NULL);
	fb_runtime_cleanup_flat_dir(meta_dir,
								now,
								fb_meta_retention_seconds(),
								stats != NULL ? &stats->meta_removed : NULL);

	fb_runtime_last_cleanup_at = now;

	pfree(runtime_dir);
	pfree(recovered_wal_dir);
	pfree(meta_dir);
}
