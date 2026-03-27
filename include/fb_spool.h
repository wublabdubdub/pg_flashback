/*
 * fb_spool.h
 *    Query-scoped spill directory and append-only spool helpers.
 */

#ifndef FB_SPOOL_H
#define FB_SPOOL_H

#include "postgres.h"

#include "lib/stringinfo.h"

typedef struct FbSpoolSession FbSpoolSession;
typedef struct FbSpoolLog FbSpoolLog;
typedef struct FbSpoolCursor FbSpoolCursor;

typedef enum FbSpoolDirection
{
	FB_SPOOL_FORWARD = 1,
	FB_SPOOL_BACKWARD
} FbSpoolDirection;

FbSpoolSession *fb_spool_session_create(void);
void fb_spool_session_destroy(FbSpoolSession *session);

FbSpoolLog *fb_spool_log_create(FbSpoolSession *session, const char *label);
void fb_spool_log_append(FbSpoolLog *log, const void *data, uint32 len);
uint32 fb_spool_log_count(const FbSpoolLog *log);
off_t fb_spool_log_size(const FbSpoolLog *log);

FbSpoolCursor *fb_spool_cursor_open(FbSpoolLog *log, FbSpoolDirection direction);
void fb_spool_cursor_close(FbSpoolCursor *cursor);
bool fb_spool_cursor_seek_item(FbSpoolCursor *cursor, uint32 item_index);
bool fb_spool_cursor_read(FbSpoolCursor *cursor, StringInfo buf, uint32 *item_index);

#endif
