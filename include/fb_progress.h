/*
 * fb_progress.h
 *    Progress reporting interfaces for pg_flashback().
 */

#ifndef FB_PROGRESS_H
#define FB_PROGRESS_H

#include "postgres.h"

typedef enum FbProgressStage
{
	FB_PROGRESS_STAGE_VALIDATE = 1,
	FB_PROGRESS_STAGE_PREPARE_WAL,
	FB_PROGRESS_STAGE_BUILD_INDEX,
	FB_PROGRESS_STAGE_REPLAY_DISCOVER,
	FB_PROGRESS_STAGE_REPLAY_WARM,
	FB_PROGRESS_STAGE_REPLAY_FINAL,
	FB_PROGRESS_STAGE_BUILD_REVERSE,
	FB_PROGRESS_STAGE_APPLY,
	FB_PROGRESS_STAGE_MATERIALIZE
} FbProgressStage;

/*
 * fb_progress_begin
 *    Progress API.
 */

void fb_progress_begin(void);
/*
 * fb_progress_finish
 *    Progress API.
 */

void fb_progress_finish(void);
/*
 * fb_progress_abort
 *    Progress API.
 */

void fb_progress_abort(void);
/*
 * fb_progress_enter_stage
 *    Progress API.
 */

void fb_progress_enter_stage(FbProgressStage stage, const char *detail);
/*
 * fb_progress_update_percent
 *    Progress API.
 */

void fb_progress_update_percent(FbProgressStage stage, uint32 percent,
								const char *detail);
/*
 * fb_progress_update_fraction
 *    Progress API.
 */

void fb_progress_update_fraction(FbProgressStage stage, uint64 done, uint64 total,
								 const char *detail);
/*
 * fb_progress_map_subrange
 *    Progress API.
 */

uint32 fb_progress_map_subrange(uint32 base_percent, uint32 span_percent,
								uint64 done, uint64 total);

/*
 * Regression-only helpers for deterministic clock injection.
 */
void fb_progress_debug_set_clock_script(const int64 *script, int count);
void fb_progress_debug_clear_clock(void);

#endif
