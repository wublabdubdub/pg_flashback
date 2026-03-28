/*
 * fb_entry.h
 *    SQL entry point declarations for the extension user surface.
 */

#ifndef FB_ENTRY_H
#define FB_ENTRY_H

#include "fb_common.h"

typedef struct FbFlashbackQueryState FbFlashbackQueryState;

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
Datum fb_pg_flashback_support(PG_FUNCTION_ARGS);
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

/*
 * Internal flashback query session helpers shared by SRF and CustomScan.
 */

FbFlashbackQueryState *fb_flashback_query_begin(Oid source_relid,
												text *target_ts_text);
bool fb_flashback_query_next_datum(FbFlashbackQueryState *state,
								   Datum *result);
TupleDesc fb_flashback_query_tupdesc(FbFlashbackQueryState *state);
void fb_flashback_query_finish(FbFlashbackQueryState *state);
void fb_flashback_query_abort(FbFlashbackQueryState *state);

#endif
