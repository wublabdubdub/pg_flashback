/*
 * fb_guc.c
 *    GUC definitions and runtime configuration selection.
 */

#include "postgres.h"

#include <limits.h>
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
static double fb_memory_limit_kb = 65536.0;
static bool fb_parallel_segment_scan = false;
static bool fb_show_progress = true;

/*
 * _PG_init
 *    GUC entry point.
 */

PGDLLEXPORT void _PG_init(void);

/*
 * _PG_init
 *    GUC entry point.
 */

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

	DefineCustomRealVariable("pg_flashback.memory_limit_kb",
							 "Per-query memory cap for tracked pg_flashback hot-path structures.",
							 "Limits tracked RecordRef/FPI/block-data/main-data and BlockReplayStore bytes.",
							 &fb_memory_limit_kb,
							 65536.0,
							 1.0,
							 4194304.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_flashback.parallel_segment_scan",
							 "Enable segment-level parallel prefiltering before ordered WAL parsing.",
							 "When on, pg_flashback performs a conservative segment hit prefilter before the final ordered XLogReader pass.",
							 &fb_parallel_segment_scan,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_flashback.show_progress",
							 "Show pg_flashback stage progress via NOTICE messages.",
							 "When on, pg_flashback emits client-visible stage progress and percentages for long-running flashback steps.",
							 &fb_show_progress,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	fb_runtime_ensure_initialized();
}

/*
 * fb_get_archive_dir
 *    GUC entry point.
 */

const char *
fb_get_archive_dir(void)
{
	return fb_archive_dir;
}

/*
 * fb_get_archive_dest
 *    GUC entry point.
 */

const char *
fb_get_archive_dest(void)
{
	return fb_archive_dest;
}

/*
 * fb_get_effective_archive_dir
 *    GUC entry point.
 */

const char *
fb_get_effective_archive_dir(void)
{
	if (fb_archive_dest != NULL && fb_archive_dest[0] != '\0')
		return fb_archive_dest;

	return fb_archive_dir;
}

/*
 * fb_using_legacy_archive_dir
 *    GUC entry point.
 */

bool
fb_using_legacy_archive_dir(void)
{
	return fb_archive_dest == NULL || fb_archive_dest[0] == '\0';
}

/*
 * fb_get_pg_wal_dir
 *    GUC entry point.
 */

char *
fb_get_pg_wal_dir(void)
{
	if (fb_debug_pg_wal_dir != NULL && fb_debug_pg_wal_dir[0] != '\0')
		return pstrdup(fb_debug_pg_wal_dir);

	return psprintf("%s/pg_wal", DataDir);
}

/*
 * fb_require_existing_directory
 *    GUC helper.
 */

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

/*
 * fb_require_archive_dir
 *    GUC entry point.
 */

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

/*
 * fb_get_memory_limit_bytes
 *    GUC entry point.
 */

uint64
fb_get_memory_limit_bytes(void)
{
	return (uint64) fb_memory_limit_kb * (uint64) 1024;
}

/*
 * fb_parallel_segment_scan_enabled
 *    GUC entry point.
 */

bool
fb_parallel_segment_scan_enabled(void)
{
	return fb_parallel_segment_scan;
}

/*
 * fb_show_progress_enabled
 *    GUC entry point.
 */

bool
fb_show_progress_enabled(void)
{
	return fb_show_progress;
}
