/*
 * fb_guc.h
 *    GUC declarations and runtime configuration accessors.
 */

#ifndef FB_GUC_H
#define FB_GUC_H

#include "postgres.h"

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

const char *fb_get_effective_archive_dir(void);
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
 * fb_parallel_segment_scan_enabled
 *    GUC API.
 */

bool fb_parallel_segment_scan_enabled(void);
/*
 * fb_show_progress_enabled
 *    GUC API.
 */

bool fb_show_progress_enabled(void);

#endif
