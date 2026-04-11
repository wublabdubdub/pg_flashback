/*
 * fb_entry.h
 *    SQL entry point declarations for the extension user surface.
 */

#ifndef FB_ENTRY_H
#define FB_ENTRY_H

#define FB_EXTENSION_VERSION "0.2.4"

#include "fb_common.h"
#include "fb_reverse_ops.h"
#include "fb_spool.h"

typedef struct FbFlashbackQueryState FbFlashbackQueryState;
typedef struct TupleTableSlot TupleTableSlot;

typedef struct FbFlashbackReverseBuildState
{
	FbRelationInfo info;
	TupleDesc tupdesc;
	FbSpoolSession *spool;
	FbReverseOpSource *reverse;
} FbFlashbackReverseBuildState;

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
Datum pg_flashback_dml_profile(PG_FUNCTION_ARGS);
Datum pg_flashback_dml_profile_detail(PG_FUNCTION_ARGS);
/*
 * pg_flashback
 *    SQL entry API.
 */

Datum pg_flashback(PG_FUNCTION_ARGS);
/*
 * Internal flashback query session helpers shared by SRF and CustomScan.
 */

FbFlashbackQueryState *fb_flashback_query_begin(Oid source_relid,
												text *target_ts_text,
												const FbFastPathSpec *fast_path);
void fb_flashback_build_reverse_state(Oid source_relid,
									  text *target_ts_text,
									  bool shareable_reverse,
									  FbFlashbackReverseBuildState *state);
void fb_flashback_release_reverse_state(FbFlashbackReverseBuildState *state);
bool fb_flashback_query_next_datum(FbFlashbackQueryState *state,
								   Datum *result);
bool fb_flashback_query_next_slot(FbFlashbackQueryState *state,
								  TupleTableSlot *slot);
TupleDesc fb_flashback_query_tupdesc(FbFlashbackQueryState *state);
void fb_flashback_query_finish(FbFlashbackQueryState *state);
void fb_flashback_query_abort(FbFlashbackQueryState *state);

#endif
