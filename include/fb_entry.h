/*
 * fb_entry.h
 *    SQL entry point declarations for the extension user surface.
 */

#ifndef FB_ENTRY_H
#define FB_ENTRY_H

#include "fb_common.h"

/*
 * fb_version
 *    SQL entry API.
 */

Datum fb_version(PG_FUNCTION_ARGS);
/*
 * fb_check_relation
 *    SQL entry API.
 */

Datum fb_check_relation(PG_FUNCTION_ARGS);
/*
 * pg_flashback
 *    SQL entry API.
 */

Datum pg_flashback(PG_FUNCTION_ARGS);
/*
 * fb_export_undo
 *    SQL entry API.
 */

Datum fb_export_undo(PG_FUNCTION_ARGS);

#endif
