/*
 * fb_runtime.h
 *    Runtime directory initialization and path helpers.
 */

#ifndef FB_RUNTIME_H
#define FB_RUNTIME_H

#include "fb_common.h"

typedef struct FbRuntimeCleanupStats
{
	uint32 runtime_removed;
	uint32 recovered_wal_removed;
	uint32 meta_removed;
} FbRuntimeCleanupStats;

/*
 * fb_runtime_base_dir
 *    Runtime API.
 */

char *fb_runtime_base_dir(void);
/*
 * fb_runtime_runtime_dir
 *    Runtime API.
 */

char *fb_runtime_runtime_dir(void);
/*
 * fb_runtime_recovered_wal_dir
 *    Runtime API.
 */

char *fb_runtime_recovered_wal_dir(void);
/*
 * fb_runtime_meta_dir
 *    Runtime API.
 */

char *fb_runtime_meta_dir(void);
/*
 * fb_runtime_ensure_initialized
 *    Runtime API.
 */

void fb_runtime_ensure_initialized(void);
void fb_runtime_cleanup(bool force, FbRuntimeCleanupStats *stats);

#endif
