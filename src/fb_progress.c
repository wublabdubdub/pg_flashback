#include "postgres.h"

#include "lib/stringinfo.h"

#include "fb_guc.h"
#include "fb_progress.h"

typedef struct FbProgressStageDef
{
	int stage_no;
	bool percent;
	const char *label;
} FbProgressStageDef;

typedef struct FbProgressContext
{
	bool active;
	bool enabled;
	FbProgressStage current_stage;
	int last_percent;
} FbProgressContext;

static const FbProgressStageDef fb_progress_defs[] = {
	{0, false, NULL},
	{1, false, "validating target relation and runtime"},
	{2, false, "preparing wal scan context"},
	{3, true, "scanning wal and building record index"},
	{4, true, "replay discover"},
	{5, true, "replay warm"},
	{6, true, "replay final and build forward ops"},
	{7, true, "building reverse ops"},
	{8, true, "applying reverse ops"},
	{9, true, "materializing result table"}
};

static FbProgressContext fb_progress_ctx = {0};

static const FbProgressStageDef *
fb_progress_get_def(FbProgressStage stage)
{
	if (stage <= 0 || stage >= lengthof(fb_progress_defs))
		elog(ERROR, "invalid fb progress stage: %d", (int) stage);

	return &fb_progress_defs[stage];
}

static void
fb_progress_emit(FbProgressStage stage, bool with_percent, uint32 percent,
				 const char *detail)
{
	const FbProgressStageDef *def;

	if (!fb_progress_ctx.active || !fb_progress_ctx.enabled)
		return;

	def = fb_progress_get_def(stage);
	if (with_percent && detail != NULL && detail[0] != '\0')
		ereport(NOTICE,
				(errmsg_internal("[%d/9 %u%%] %s %s",
								 def->stage_no,
								 percent,
								 def->label,
								 detail)));
	else if (with_percent)
		ereport(NOTICE,
				(errmsg_internal("[%d/9 %u%%] %s",
								 def->stage_no,
								 percent,
								 def->label)));
	else if (detail != NULL && detail[0] != '\0')
		ereport(NOTICE,
				(errmsg_internal("[%d/9] %s %s",
								 def->stage_no,
								 def->label,
								 detail)));
	else
		ereport(NOTICE,
				(errmsg_internal("[%d/9] %s",
								 def->stage_no,
								 def->label)));
}

static uint32
fb_progress_bucket_percent(uint32 percent)
{
	if (percent >= 100)
		return 100;

	return (percent / 20) * 20;
}

void
fb_progress_begin(void)
{
	MemSet(&fb_progress_ctx, 0, sizeof(fb_progress_ctx));
	fb_progress_ctx.active = true;
	fb_progress_ctx.enabled = fb_show_progress_enabled();
	fb_progress_ctx.last_percent = -1;
}

void
fb_progress_finish(void)
{
	MemSet(&fb_progress_ctx, 0, sizeof(fb_progress_ctx));
}

void
fb_progress_abort(void)
{
	MemSet(&fb_progress_ctx, 0, sizeof(fb_progress_ctx));
}

void
fb_progress_enter_stage(FbProgressStage stage, const char *detail)
{
	const FbProgressStageDef *def = fb_progress_get_def(stage);

	if (!fb_progress_ctx.active)
		return;

	fb_progress_ctx.current_stage = stage;
	fb_progress_ctx.last_percent = -1;

	if (def->percent)
		fb_progress_update_percent(stage, 0, detail);
	else
		fb_progress_emit(stage, false, 0, detail);
}

void
fb_progress_update_percent(FbProgressStage stage, uint32 percent, const char *detail)
{
	const FbProgressStageDef *def = fb_progress_get_def(stage);
	uint32 bucket;

	if (!fb_progress_ctx.active || !def->percent)
		return;

	bucket = fb_progress_bucket_percent(percent);
	if ((int) bucket <= fb_progress_ctx.last_percent)
		return;

	fb_progress_ctx.current_stage = stage;
	fb_progress_ctx.last_percent = (int) bucket;
	fb_progress_emit(stage, true, bucket, detail);
}

void
fb_progress_update_fraction(FbProgressStage stage, uint64 done, uint64 total,
							  const char *detail)
{
	uint32 percent;

	if (total == 0)
	{
		fb_progress_update_percent(stage, 100, detail);
		return;
	}

	if (done >= total)
		percent = 100;
	else
		percent = (uint32) ((done * 100) / total);

	fb_progress_update_percent(stage, percent, detail);
}

uint32
fb_progress_map_subrange(uint32 base_percent, uint32 span_percent,
						 uint64 done, uint64 total)
{
	uint64 offset;

	if (span_percent == 0)
		return Min(base_percent, 100U);

	if (total == 0)
		return Min(base_percent + span_percent, 100U);

	if (done >= total)
		return Min(base_percent + span_percent, 100U);

	offset = (done * span_percent) / total;
	return Min(base_percent + (uint32) offset, 100U);
}
