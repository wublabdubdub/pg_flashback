/*
 * fb_runtime.c
 *    Runtime directory initialization helpers.
 */

#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/file_perm.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/elog.h"

#include "fb_compat.h"
#include "fb_runtime.h"

PG_FUNCTION_INFO_V1(fb_runtime_create_test_artifacts_debug);
PG_FUNCTION_INFO_V1(fb_runtime_remove_test_artifacts_debug);

/*
 * fb_runtime_join_path
 *    Runtime helper.
 */

static char *
fb_runtime_join_path(const char *suffix)
{
	return psprintf("%s/pg_flashback/%s", DataDir, suffix);
}

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

char *
fb_runtime_meta_summary_dir(void)
{
	return fb_runtime_join_path("meta/summary");
}

char *
fb_runtime_meta_summaryd_dir(void)
{
	return fb_runtime_join_path("meta/summaryd");
}

char *
fb_runtime_summaryd_state_path(void)
{
	return fb_runtime_join_path("meta/summaryd/state.json");
}

char *
fb_runtime_summaryd_debug_path(void)
{
	return fb_runtime_join_path("meta/summaryd/debug.json");
}

char *
fb_runtime_summary_hint_dir(void)
{
	return fb_runtime_join_path("runtime/summary-hints");
}

char *
fb_runtime_summary_last_query_hint_path(void)
{
	return fb_runtime_join_path("runtime/summary-hints/last-query.json");
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
	if (fb_mkdir_p_compat(mutable_path, pg_dir_create_mode) != 0)
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

void
fb_runtime_ensure_summary_daemon_dirs(void)
{
	char *summaryd_dir;
	char *hint_dir;

	fb_runtime_ensure_initialized();
	summaryd_dir = fb_runtime_meta_summaryd_dir();
	hint_dir = fb_runtime_summary_hint_dir();
	fb_runtime_ensure_directory(summaryd_dir, "summary daemon meta");
	fb_runtime_ensure_directory(hint_dir, "summary hint");
	pfree(summaryd_dir);
	pfree(hint_dir);
}

static bool
fb_runtime_owner_pid_active(pid_t owner_pid)
{
	if (owner_pid <= 0)
		return false;

	if (kill(owner_pid, 0) == 0)
		return true;

	return errno == EPERM;
}

static bool
fb_runtime_parse_spill_name(const char *name, pid_t *owner_pid)
{
	int parsed = 0;
	int pid = 0;
	unsigned long long serial = 0;

	if (name == NULL)
		return false;

	if (sscanf(name, "fbspill-%d-%llu%n", &pid, &serial, &parsed) != 2)
		return false;
	if (parsed != (int) strlen(name) || pid <= 0)
		return false;

	if (owner_pid != NULL)
		*owner_pid = (pid_t) pid;
	return true;
}

static bool
fb_runtime_parse_toast_name(const char *name, pid_t *owner_pid)
{
	int parsed = 0;
	int pid = 0;
	unsigned long long serial = 0;

	if (name == NULL)
		return false;

	if (sscanf(name, "toast-retired-%d-%llu.bin%n", &pid, &serial, &parsed) != 2)
		return false;
	if (parsed != (int) strlen(name) || pid <= 0)
		return false;

	if (owner_pid != NULL)
		*owner_pid = (pid_t) pid;
	return true;
}

static void
fb_runtime_remove_tree(const char *path)
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
				 errmsg("could not open fb runtime spill directory"),
				 errdetail("path=%s: %m", path)));
	}

	while ((de = ReadDir(dir, path)) != NULL)
	{
		char *child_path;
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		child_path = psprintf("%s/%s", path, de->d_name);
		if (lstat(child_path, &st) != 0)
		{
			if (errno != ENOENT)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat fb runtime spill path"),
						 errdetail("path=%s: %m", child_path)));
			pfree(child_path);
			continue;
		}

		if (S_ISDIR(st.st_mode))
			fb_runtime_remove_tree(child_path);
		else if (unlink(child_path) != 0 && errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove fb runtime spill file"),
					 errdetail("path=%s: %m", child_path)));
		pfree(child_path);
	}

	FreeDir(dir);
	if (rmdir(path) != 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove fb runtime spill directory"),
				 errdetail("path=%s: %m", path)));
}

static void
fb_runtime_cleanup_matching_owner(bool only_stale, pid_t force_owner_pid)
{
	char *runtime_dir;
	DIR *dir;
	struct dirent *de;

	fb_runtime_ensure_initialized();
	runtime_dir = fb_runtime_runtime_dir();
	dir = AllocateDir(runtime_dir);
	if (dir == NULL)
	{
		if (errno == ENOENT)
		{
			pfree(runtime_dir);
			return;
		}

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open pg_flashback runtime directory"),
				 errdetail("path=%s: %m", runtime_dir)));
	}

	while ((de = ReadDir(dir, runtime_dir)) != NULL)
	{
		char *path;
		pid_t owner_pid = 0;
		bool matches = false;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		if (fb_runtime_parse_spill_name(de->d_name, &owner_pid))
			matches = true;
		else if (fb_runtime_parse_toast_name(de->d_name, &owner_pid))
			matches = true;

		if (!matches)
			continue;
		if (force_owner_pid > 0 && owner_pid != force_owner_pid)
			continue;
		if (only_stale && fb_runtime_owner_pid_active(owner_pid))
			continue;

		path = psprintf("%s/%s", runtime_dir, de->d_name);
		if (strncmp(de->d_name, "fbspill-", 8) == 0)
			fb_runtime_remove_tree(path);
		else if (unlink(path) != 0 && errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove fb runtime toast spill file"),
					 errdetail("path=%s: %m", path)));
		pfree(path);
	}

	FreeDir(dir);
	pfree(runtime_dir);
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
	char *meta_summary_dir;

	base_dir = fb_runtime_base_dir();
	runtime_dir = fb_runtime_runtime_dir();
	recovered_wal_dir = fb_runtime_recovered_wal_dir();
	meta_dir = fb_runtime_meta_dir();
	meta_summary_dir = fb_runtime_meta_summary_dir();

	fb_runtime_ensure_directory(base_dir, "base");
	fb_runtime_ensure_directory(runtime_dir, "runtime");
	fb_runtime_ensure_directory(recovered_wal_dir, "recovered_wal");
	fb_runtime_ensure_directory(meta_dir, "meta");
	fb_runtime_ensure_directory(meta_summary_dir, "meta summary");

	pfree(base_dir);
	pfree(runtime_dir);
	pfree(recovered_wal_dir);
	pfree(meta_dir);
	pfree(meta_summary_dir);
}

void
fb_runtime_cleanup_stale(void)
{
	fb_runtime_cleanup_matching_owner(true, 0);
}

void
fb_runtime_cleanup_current_backend(void)
{
	fb_runtime_cleanup_matching_owner(false, MyProcPid);
}

PGDLLEXPORT Datum
fb_runtime_create_test_artifacts_debug(PG_FUNCTION_ARGS)
{
	int32 owner_pid = PG_GETARG_INT32(0);
	char *runtime_dir;
	char *spill_dir;
	char *spill_file;
	char *toast_file;
	int fd;

	if (owner_pid <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("owner pid must be positive")));

	fb_runtime_ensure_initialized();
	runtime_dir = fb_runtime_runtime_dir();
	spill_dir = psprintf("%s/fbspill-%d-999999", runtime_dir, owner_pid);
	spill_file = psprintf("%s/test.bin", spill_dir);
	toast_file = psprintf("%s/toast-retired-%d-999999.bin", runtime_dir, owner_pid);

	if (mkdir(spill_dir, pg_dir_create_mode) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create debug fb spill directory"),
				 errdetail("path=%s: %m", spill_dir)));

	fd = open(spill_file, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create debug fb spill file"),
				 errdetail("path=%s: %m", spill_file)));
	close(fd);

	fd = open(toast_file, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create debug fb toast spill file"),
				 errdetail("path=%s: %m", toast_file)));
	close(fd);

	pfree(runtime_dir);
	pfree(spill_dir);
	pfree(spill_file);
	pfree(toast_file);

	PG_RETURN_BOOL(true);
}

PGDLLEXPORT Datum
fb_runtime_remove_test_artifacts_debug(PG_FUNCTION_ARGS)
{
	int32 owner_pid = PG_GETARG_INT32(0);

	if (owner_pid <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("owner pid must be positive")));

	fb_runtime_cleanup_matching_owner(false, (pid_t) owner_pid);
	PG_RETURN_BOOL(true);
}
