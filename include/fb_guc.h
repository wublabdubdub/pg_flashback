#ifndef FB_GUC_H
#define FB_GUC_H

#include "postgres.h"

const char *fb_get_archive_dir(void);
const char *fb_get_archive_dest(void);
const char *fb_get_effective_archive_dir(void);
bool fb_using_legacy_archive_dir(void);
char *fb_get_pg_wal_dir(void);
void fb_require_archive_dir(void);
uint64 fb_get_memory_limit_bytes(void);

#endif
