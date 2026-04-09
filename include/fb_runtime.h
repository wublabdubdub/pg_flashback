/*
 * fb_runtime.h
 *    Runtime directory initialization and path helpers.
 */

#ifndef FB_RUNTIME_H
#define FB_RUNTIME_H

#include "fb_common.h"

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
char *fb_runtime_meta_summary_dir(void);
char *fb_runtime_meta_summaryd_dir(void);
char *fb_runtime_summaryd_state_path(void);
char *fb_runtime_summaryd_debug_path(void);
char *fb_runtime_summary_hint_dir(void);
char *fb_runtime_summary_last_query_hint_path(void);
/*
 * fb_runtime_ensure_initialized
 *    Runtime API.
 */

void fb_runtime_ensure_initialized(void);
void fb_runtime_ensure_summary_daemon_dirs(void);
void fb_runtime_cleanup_stale(void);
void fb_runtime_cleanup_current_backend(void);

#endif
