/*
 * fb_guc.h
 *    GUC declarations and runtime configuration accessors.
 */

#ifndef FB_GUC_H
#define FB_GUC_H

#include "postgres.h"

typedef enum FbArchiveDirSource
{
	FB_ARCHIVE_DIR_SOURCE_NONE = 0,
	FB_ARCHIVE_DIR_SOURCE_EXPLICIT_DEST,
	FB_ARCHIVE_DIR_SOURCE_LEGACY_DIR,
	FB_ARCHIVE_DIR_SOURCE_ARCHIVE_COMMAND
} FbArchiveDirSource;

typedef enum FbSpillMode
{
	FB_SPILL_MODE_AUTO = 0,
	FB_SPILL_MODE_MEMORY,
	FB_SPILL_MODE_DISK
} FbSpillMode;

/*
 * fb_get_archive_dir
 *    GUC API.
 */

const char *fb_get_archive_dir(void);
/*
 * fb_get_archive_dest
 *    GUC API.
 */

const char *fb_get_archive_dest(void);
/*
 * fb_get_effective_archive_dir
 *    GUC API.
 */

char *fb_get_effective_archive_dir(void);
/*
 * fb_resolve_archive_dir
 *    GUC API.
 */

char *fb_resolve_archive_dir(FbArchiveDirSource *source_out,
							 const char **setting_name_out);
/*
 * fb_get_archive_dir_source
 *    GUC API.
 */

FbArchiveDirSource fb_get_archive_dir_source(void);
/*
 * fb_get_archive_dir_setting_name
 *    GUC API.
 */

const char *fb_get_archive_dir_setting_name(void);
/*
 * fb_archive_dir_source_name
 *    GUC API.
 */

const char *fb_archive_dir_source_name(FbArchiveDirSource source);
/*
 * fb_using_legacy_archive_dir
 *    GUC API.
 */

bool fb_using_legacy_archive_dir(void);
/*
 * fb_get_pg_wal_dir
 *    GUC API.
 */

char *fb_get_pg_wal_dir(void);
/*
 * fb_require_archive_dir
 *    GUC API.
 */

void fb_require_archive_dir(void);
/*
 * fb_get_memory_limit_bytes
 *    GUC API.
 */

uint64 fb_get_memory_limit_bytes(void);
/*
 * fb_get_spill_mode
 *    GUC API.
 */

FbSpillMode fb_get_spill_mode(void);
const char *fb_spill_mode_name(FbSpillMode mode);
int fb_runtime_retention_seconds(void);
int fb_recovered_wal_retention_seconds(void);
int fb_meta_retention_seconds(void);
/*
 * fb_parallel_workers
 *    GUC API.
 */

int fb_parallel_workers(void);
int fb_export_parallel_workers(void);
/*
 * fb_show_progress_enabled
 *    GUC API.
 */

bool fb_show_progress_enabled(void);

#endif
