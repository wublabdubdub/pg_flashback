/*
 * fb_apply.c
 *    Streaming apply coordinator for flashback query results.
 */

#include "postgres.h"

#include "access/relation.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "fb_apply.h"
#include "fb_progress.h"

typedef enum FbApplyPhase
{
	FB_APPLY_PHASE_SCAN = 1,
	FB_APPLY_PHASE_RESIDUAL,
	FB_APPLY_PHASE_DONE
} FbApplyPhase;

struct FbApplyContext
{
	const FbRelationInfo *info;
	TupleDesc tupdesc;
	Relation rel;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool pushed_snapshot;
	FbApplyPhase phase;
	uint64 estimated_rows;
	uint64 scanned_rows;
	uint64 residual_total;
	uint64 residual_emitted;
	void *mode_state;
};

static void
fb_apply_cleanup_resources(FbApplyContext *ctx)
{
	if (ctx == NULL)
		return;

	if (ctx->slot != NULL)
	{
		ExecDropSingleTupleTableSlot(ctx->slot);
		ctx->slot = NULL;
	}

	if (ctx->scan != NULL)
	{
		table_endscan(ctx->scan);
		ctx->scan = NULL;
	}

	if (ctx->rel != NULL)
	{
		relation_close(ctx->rel, AccessShareLock);
		ctx->rel = NULL;
	}

	if (ctx->pushed_snapshot)
	{
		PopActiveSnapshot();
		ctx->pushed_snapshot = false;
	}
}

static HeapTuple
fb_apply_process_current_tuple(FbApplyContext *ctx, HeapTuple tuple)
{
	if (ctx->info->mode == FB_APPLY_KEYED)
		return fb_keyed_apply_process_current(ctx->mode_state, tuple);

	return fb_bag_apply_process_current(ctx->mode_state, tuple);
}

/*
 * fb_apply_finish_scan_phase
 *    Apply helper.
 */

static void
fb_apply_finish_scan_phase(FbApplyContext *ctx)
{
	if (ctx->phase != FB_APPLY_PHASE_SCAN)
		return;

	if (ctx->info->mode == FB_APPLY_KEYED)
	{
		fb_keyed_apply_finish_scan(ctx->mode_state);
		ctx->residual_total = fb_keyed_apply_residual_total(ctx->mode_state);
	}
	else
	{
		fb_bag_apply_finish_scan(ctx->mode_state);
		ctx->residual_total = fb_bag_apply_residual_total(ctx->mode_state);
	}

	ctx->phase = FB_APPLY_PHASE_RESIDUAL;
	fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY, 100, NULL);
	fb_progress_enter_stage(FB_PROGRESS_STAGE_MATERIALIZE, NULL);
	if (ctx->residual_total == 0)
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
}

/*
 * fb_apply_begin
 *    Apply entry point.
 */

FbApplyContext *
fb_apply_begin(const FbRelationInfo *info,
			   TupleDesc tupdesc,
			   const FbReverseOpStream *stream)
{
	FbApplyContext *ctx;
	Snapshot snapshot;

	ctx = palloc0(sizeof(*ctx));
	ctx->info = info;
	ctx->tupdesc = tupdesc;
	ctx->phase = FB_APPLY_PHASE_SCAN;

	if (info->mode == FB_APPLY_KEYED)
		ctx->mode_state = fb_keyed_apply_begin(info, tupdesc, stream);
	else
		ctx->mode_state = fb_bag_apply_begin(info, tupdesc, stream);

	ctx->rel = relation_open(info->relid, AccessShareLock);
	ctx->estimated_rows = (ctx->rel->rd_rel->reltuples > 0) ?
		(uint64) ctx->rel->rd_rel->reltuples : 0;

	snapshot = GetActiveSnapshot();
	if (snapshot == NULL)
	{
		PushActiveSnapshot(GetLatestSnapshot());
		snapshot = GetActiveSnapshot();
		ctx->pushed_snapshot = true;
	}

	ctx->scan = table_beginscan(ctx->rel, snapshot, 0, NULL);
	ctx->slot = table_slot_create(ctx->rel, NULL);

	fb_progress_enter_stage(FB_PROGRESS_STAGE_APPLY, NULL);
	return ctx;
}

/*
 * fb_apply_next
 *    Apply entry point.
 */

HeapTuple
fb_apply_next(FbApplyContext *ctx)
{
	if (ctx == NULL)
		return NULL;

	if (ctx->phase == FB_APPLY_PHASE_SCAN)
	{
		while (table_scan_getnextslot(ctx->scan, ForwardScanDirection, ctx->slot))
		{
			HeapTuple tuple;
			HeapTuple result;

			tuple = ExecCopySlotHeapTuple(ctx->slot);
			ExecClearTuple(ctx->slot);

			ctx->scanned_rows++;
			if (ctx->estimated_rows > 0)
				fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
										   fb_progress_map_subrange(0, 100,
																ctx->scanned_rows,
																ctx->estimated_rows),
										   NULL);

			result = fb_apply_process_current_tuple(ctx, tuple);
			if (result != NULL)
				return result;
		}

		fb_apply_cleanup_resources(ctx);
		fb_apply_finish_scan_phase(ctx);
	}

	if (ctx->phase == FB_APPLY_PHASE_RESIDUAL)
	{
		HeapTuple result;

		if (ctx->info->mode == FB_APPLY_KEYED)
			result = fb_keyed_apply_next_residual(ctx->mode_state);
		else
			result = fb_bag_apply_next_residual(ctx->mode_state);

		if (result == NULL)
		{
			ctx->phase = FB_APPLY_PHASE_DONE;
			fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
			return NULL;
		}

		ctx->residual_emitted++;
		fb_progress_update_fraction(FB_PROGRESS_STAGE_MATERIALIZE,
									ctx->residual_emitted,
									ctx->residual_total,
									NULL);
		return result;
	}

	return NULL;
}

/*
 * fb_apply_end
 *    Apply entry point.
 */

void
fb_apply_end(FbApplyContext *ctx)
{
	if (ctx == NULL)
		return;

	fb_apply_cleanup_resources(ctx);

	if (ctx->mode_state != NULL)
	{
		if (ctx->info != NULL && ctx->info->mode == FB_APPLY_KEYED)
			fb_keyed_apply_end(ctx->mode_state);
		else
			fb_bag_apply_end(ctx->mode_state);
		ctx->mode_state = NULL;
	}

	ctx->phase = FB_APPLY_PHASE_DONE;
}
