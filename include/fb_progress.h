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

void fb_progress_begin(void);
void fb_progress_finish(void);
void fb_progress_abort(void);
void fb_progress_enter_stage(FbProgressStage stage, const char *detail);
void fb_progress_update_percent(FbProgressStage stage, uint32 percent,
								const char *detail);
void fb_progress_update_fraction(FbProgressStage stage, uint64 done, uint64 total,
								 const char *detail);
uint32 fb_progress_map_subrange(uint32 base_percent, uint32 span_percent,
								uint64 done, uint64 total);

#endif
