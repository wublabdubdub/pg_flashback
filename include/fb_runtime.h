#ifndef FB_RUNTIME_H
#define FB_RUNTIME_H

#include "fb_common.h"

char *fb_runtime_base_dir(void);
char *fb_runtime_runtime_dir(void);
char *fb_runtime_recovered_wal_dir(void);
char *fb_runtime_meta_dir(void);
void fb_runtime_ensure_initialized(void);
char *fb_runtime_debug_summary(void);
Datum fb_runtime_dir_debug(PG_FUNCTION_ARGS);

#endif
