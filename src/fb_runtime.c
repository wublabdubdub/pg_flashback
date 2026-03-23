#include "postgres.h"

#include <unistd.h>

#include "common/file_perm.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "port.h"
#include "utils/builtins.h"
#include "utils/elog.h"

#include "fb_runtime.h"

PG_FUNCTION_INFO_V1(fb_runtime_dir_debug);

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

char *
fb_runtime_runtime_dir(void)
{
	return fb_runtime_join_path("runtime");
}

char *
fb_runtime_recovered_wal_dir(void)
{
	return fb_runtime_join_path("recovered_wal");
}

char *
fb_runtime_meta_dir(void)
{
	return fb_runtime_join_path("meta");
}

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

	pfree(base_dir);
	pfree(runtime_dir);
	pfree(recovered_wal_dir);
	pfree(meta_dir);
}

char *
fb_runtime_debug_summary(void)
{
	char *base_dir;
	char *summary;

	fb_runtime_ensure_initialized();
	base_dir = fb_runtime_base_dir();
	summary = psprintf("base=%s runtime=true recovered=true meta=true", base_dir);
	pfree(base_dir);

	return summary;
}

Datum
fb_runtime_dir_debug(PG_FUNCTION_ARGS)
{
	char *summary = fb_runtime_debug_summary();

	PG_RETURN_TEXT_P(cstring_to_text(summary));
}
