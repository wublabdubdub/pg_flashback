#include "postgres.h"

#include <sys/stat.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include "fb_guc.h"
#include "fb_runtime.h"

static char *fb_archive_dir = NULL;
static char *fb_archive_dest = NULL;
static char *fb_debug_pg_wal_dir = NULL;
static int fb_memory_limit_kb = 65536;

PGDLLEXPORT void _PG_init(void);

void
_PG_init(void)
{
	DefineCustomStringVariable("pg_flashback.archive_dir",
							   "Legacy single-directory WAL source for pg_flashback.",
							   NULL,
							   &fb_archive_dir,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pg_flashback.archive_dest",
							   "Primary archive destination for pg_flashback historical WAL reads.",
							   NULL,
							   &fb_archive_dest,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pg_flashback.debug_pg_wal_dir",
							   "Development-only override for the pg_wal directory used by pg_flashback.",
							   NULL,
							   &fb_debug_pg_wal_dir,
							   NULL,
							   PGC_USERSET,
							   GUC_NOT_IN_SAMPLE,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("pg_flashback.memory_limit_kb",
							"Per-query memory cap for tracked pg_flashback hot-path structures.",
							"Limits tracked RecordRef/FPI/block-data/main-data and BlockReplayStore bytes.",
							&fb_memory_limit_kb,
							65536,
							1,
							INT_MAX / 1024,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	fb_runtime_ensure_initialized();
}

const char *
fb_get_archive_dir(void)
{
	return fb_archive_dir;
}

const char *
fb_get_archive_dest(void)
{
	return fb_archive_dest;
}

const char *
fb_get_effective_archive_dir(void)
{
	if (fb_archive_dest != NULL && fb_archive_dest[0] != '\0')
		return fb_archive_dest;

	return fb_archive_dir;
}

bool
fb_using_legacy_archive_dir(void)
{
	return fb_archive_dest == NULL || fb_archive_dest[0] == '\0';
}

char *
fb_get_pg_wal_dir(void)
{
	if (fb_debug_pg_wal_dir != NULL && fb_debug_pg_wal_dir[0] != '\0')
		return pstrdup(fb_debug_pg_wal_dir);

	return psprintf("%s/pg_wal", DataDir);
}

static void
fb_require_existing_directory(const char *guc_name, const char *path)
{
	struct stat st;

	if (path == NULL || path[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s is not set", guc_name)));

	if (stat(path, &st) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s does not exist: %s", guc_name, path)));

	if (!S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s is not a directory: %s", guc_name, path)));
}

void
fb_require_archive_dir(void)
{
	if (fb_archive_dest != NULL && fb_archive_dest[0] != '\0')
	{
		fb_require_existing_directory("pg_flashback.archive_dest", fb_archive_dest);
		return;
	}

	if (fb_archive_dir != NULL && fb_archive_dir[0] != '\0')
	{
		fb_require_existing_directory("pg_flashback.archive_dir", fb_archive_dir);
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("neither pg_flashback.archive_dest nor legacy pg_flashback.archive_dir is set")));
}

uint64
fb_get_memory_limit_bytes(void)
{
	return (uint64) fb_memory_limit_kb * (uint64) 1024;
}
