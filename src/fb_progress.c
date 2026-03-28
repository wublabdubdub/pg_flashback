/*
 * fb_progress.c
 *    Client-visible progress reporting.
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "fb_guc.h"
#include "fb_progress.h"

/*
 * FbProgressStageDef
 *    Defines one progress stage entry.
 */

typedef struct FbProgressStageDef
{
	int stage_no;
	bool percent;
	const char *label;
} FbProgressStageDef;

/*
 * FbProgressContext
 *    Tracks progress context.
 */

typedef struct FbProgressContext
{
	bool active;
	bool enabled;
	FbProgressStage current_stage;
	int last_percent;
	TimestampTz query_start_ts;
	TimestampTz last_emit_ts;
} FbProgressContext;

typedef struct FbProgressDebugClock
{
	TimestampTz *script;
	int count;
	int next;
} FbProgressDebugClock;

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
	{9, true, "emitting residual historical rows"}
};

static FbProgressContext fb_progress_ctx = {0};
static FbProgressDebugClock fb_progress_debug_clock = {0};

#define FB_PROGRESS_PERCENT_BUCKET 50U

/*
 * fb_progress_get_def
 *    Progress helper.
 */

static const FbProgressStageDef *
fb_progress_get_def(FbProgressStage stage)
{
	if (stage <= 0 || stage >= lengthof(fb_progress_defs))
		elog(ERROR, "invalid fb progress stage: %d", (int) stage);

	return &fb_progress_defs[stage];
}

/*
 * fb_progress_now
 *    Progress helper.
 */

static TimestampTz
fb_progress_now(void)
{
	TimestampTz ts;
	int idx;

	if (fb_progress_debug_clock.script == NULL || fb_progress_debug_clock.count <= 0)
		return GetCurrentTimestamp();

	idx = Min(fb_progress_debug_clock.next,
			  fb_progress_debug_clock.count - 1);
	ts = fb_progress_debug_clock.script[idx];
	if (fb_progress_debug_clock.next < fb_progress_debug_clock.count - 1)
		fb_progress_debug_clock.next++;

	return ts;
}

/*
 * fb_progress_append_elapsed_ms
 *    Progress helper.
 */

static void
fb_progress_append_elapsed_ms(StringInfo buf, TimestampTz elapsed_us,
							  const char *prefix)
{
	long long whole_ms;
	long long frac_ms;

	if (elapsed_us < 0)
		elapsed_us = 0;

	whole_ms = (long long) (elapsed_us / 1000);
	frac_ms = (long long) (elapsed_us % 1000);
	appendStringInfo(buf, " (%s%lld.%03lld ms)",
					 prefix,
					 whole_ms,
					 frac_ms);
}

/*
 * fb_progress_emit
 *    Progress helper.
 */

static void
fb_progress_emit(FbProgressStage stage, bool with_percent, uint32 percent,
				 const char *detail)
{
	const FbProgressStageDef *def;
	StringInfoData buf;
	TimestampTz now_ts;

	if (!fb_progress_ctx.active || !fb_progress_ctx.enabled)
		return;

	def = fb_progress_get_def(stage);
	now_ts = fb_progress_now();
	initStringInfo(&buf);

	if (with_percent)
		appendStringInfo(&buf, "[%d/9 %u%%] %s",
						 def->stage_no,
						 percent,
						 def->label);
	else
		appendStringInfo(&buf, "[%d/9] %s",
						 def->stage_no,
						 def->label);

	if (detail != NULL && detail[0] != '\0')
		appendStringInfo(&buf, " %s", detail);

	fb_progress_append_elapsed_ms(&buf,
								  now_ts - fb_progress_ctx.last_emit_ts,
								  "+");
	fb_progress_ctx.last_emit_ts = now_ts;

	ereport(NOTICE,
			(errmsg_internal("%s", buf.data)));
}

/*
 * fb_progress_emit_total
 *    Progress helper.
 */

static void
fb_progress_emit_total(void)
{
	StringInfoData buf;
	TimestampTz now_ts;
	long long whole_ms;
	long long frac_ms;

	if (!fb_progress_ctx.active || !fb_progress_ctx.enabled)
		return;

	now_ts = fb_progress_now();
	if (now_ts < fb_progress_ctx.query_start_ts)
		now_ts = fb_progress_ctx.query_start_ts;

	whole_ms = (long long) ((now_ts - fb_progress_ctx.query_start_ts) / 1000);
	frac_ms = (long long) ((now_ts - fb_progress_ctx.query_start_ts) % 1000);
	initStringInfo(&buf);
	appendStringInfo(&buf, "[done] total elapsed %lld.%03lld ms",
					 whole_ms,
					 frac_ms);
	ereport(NOTICE,
			(errmsg_internal("%s", buf.data)));
}

/*
 * fb_progress_bucket_percent
 *    Progress helper.
 */

static uint32
fb_progress_bucket_percent(uint32 percent)
{
	if (percent >= 100)
		return 100;

	return (percent / FB_PROGRESS_PERCENT_BUCKET) * FB_PROGRESS_PERCENT_BUCKET;
}

/*
 * fb_progress_begin
 *    Progress entry point.
 */

void
fb_progress_begin(void)
{
	MemSet(&fb_progress_ctx, 0, sizeof(fb_progress_ctx));
	fb_progress_ctx.active = true;
	fb_progress_ctx.enabled = fb_show_progress_enabled();
	fb_progress_ctx.last_percent = -1;
	if (fb_progress_ctx.enabled)
	{
		fb_progress_ctx.query_start_ts = fb_progress_now();
		fb_progress_ctx.last_emit_ts = fb_progress_ctx.query_start_ts;
	}
}

/*
 * fb_progress_finish
 *    Progress entry point.
 */

void
fb_progress_finish(void)
{
	fb_progress_emit_total();
	MemSet(&fb_progress_ctx, 0, sizeof(fb_progress_ctx));
}

/*
 * fb_progress_abort
 *    Progress entry point.
 */

void
fb_progress_abort(void)
{
	MemSet(&fb_progress_ctx, 0, sizeof(fb_progress_ctx));
}

/*
 * fb_progress_enter_stage
 *    Progress entry point.
 */

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

/*
 * fb_progress_update_percent
 *    Progress entry point.
 */

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

/*
 * fb_progress_update_fraction
 *    Progress entry point.
 */

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

/*
 * fb_progress_map_subrange
 *    Progress entry point.
 */

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

/*
 * fb_progress_debug_set_clock_script
 *    Regression-only helper.
 */

void
fb_progress_debug_set_clock_script(const int64 *script, int count)
{
	TimestampTz *copy = NULL;

	if (fb_progress_debug_clock.script != NULL)
	{
		pfree(fb_progress_debug_clock.script);
		fb_progress_debug_clock.script = NULL;
	}

	fb_progress_debug_clock.count = 0;
	fb_progress_debug_clock.next = 0;

	if (script == NULL || count <= 0)
		return;

	copy = MemoryContextAlloc(TopMemoryContext, sizeof(TimestampTz) * count);
	memcpy(copy, script, sizeof(TimestampTz) * count);
	fb_progress_debug_clock.script = copy;
	fb_progress_debug_clock.count = count;
}

/*
 * fb_progress_debug_clear_clock
 *    Regression-only helper.
 */

void
fb_progress_debug_clear_clock(void)
{
	if (fb_progress_debug_clock.script != NULL)
		pfree(fb_progress_debug_clock.script);

	MemSet(&fb_progress_debug_clock, 0, sizeof(fb_progress_debug_clock));
}
