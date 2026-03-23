#include "postgres.h"

#include <sys/stat.h>

#include "fmgr.h"
#include "storage/fd.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include "fb_guc.h"

static char *fb_archive_dir = NULL;
static int fb_memory_limit_kb = 65536;

PGDLLEXPORT void _PG_init(void);

void
_PG_init(void)
{
	DefineCustomStringVariable("pg_flashback.archive_dir",
							   "Directory containing archived WAL segments for pg_flashback.",
							   NULL,
							   &fb_archive_dir,
							   NULL,
							   PGC_USERSET,
							   0,
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
}

const char *
fb_get_archive_dir(void)
{
	return fb_archive_dir;
}

void
fb_require_archive_dir(void)
{
	struct stat st;

	if (fb_archive_dir == NULL || fb_archive_dir[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_flashback.archive_dir is not set")));

	if (stat(fb_archive_dir, &st) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_flashback.archive_dir does not exist: %s",
						fb_archive_dir)));

	if (!S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_flashback.archive_dir is not a directory: %s",
						fb_archive_dir)));
}

uint64
fb_get_memory_limit_bytes(void)
{
	return (uint64) fb_memory_limit_kb * (uint64) 1024;
}
