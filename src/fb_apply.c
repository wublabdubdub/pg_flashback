/*
 * fb_apply.c
 *    Streaming apply coordinator for flashback query results.
 */

#include "postgres.h"

#include "access/relation.h"
#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"

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
	uint64 progress_stride;
	uint64 residual_total;
	uint64 residual_emitted;
	void *mode_state;
};

static void
fb_apply_slot_get_attrs(TupleTableSlot *slot,
						  const AttrNumber *attrs,
						  int attr_count)
{
	int max_attr = 0;
	int i;

	if (slot == NULL)
		return;

	if (attrs == NULL || attr_count <= 0)
	{
		slot_getallattrs(slot);
		return;
	}

	for (i = 0; i < attr_count; i++)
	{
		if (attrs[i] > max_attr)
			max_attr = attrs[i];
	}

	if (max_attr > 0)
		slot_getsomeattrs(slot, max_attr);
}

static char *
fb_apply_build_identity_values(TupleDesc tupdesc,
								 const Datum *values,
								 const bool *nulls,
								 const AttrNumber *attrs,
								 int attr_count)
{
	StringInfoData buf;
	int i;

	initStringInfo(&buf);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		bool include = (attrs == NULL);

		if (attr->attisdropped)
			continue;

		if (!include)
		{
			int key_index;

			for (key_index = 0; key_index < attr_count; key_index++)
			{
				if (attrs[key_index] == attr->attnum)
				{
					include = true;
					break;
				}
			}
		}

		if (!include)
			continue;

		if (buf.len > 0)
			appendStringInfoChar(&buf, '|');

		appendStringInfoString(&buf, NameStr(attr->attname));
		appendStringInfoChar(&buf, '=');

		if (nulls[i])
			appendStringInfoString(&buf, "NULL");
		else
		{
			Oid output_func;
			bool is_varlena;
			char *value_text;
			char *quoted;

			getTypeOutputInfo(attr->atttypid, &output_func, &is_varlena);
			value_text = OidOutputFunctionCall(output_func, values[i]);
			quoted = quote_literal_cstr(value_text);
			appendStringInfoString(&buf, quoted);
		}
	}

	return buf.data;
}

char *
fb_apply_build_key_identity_slot(const FbRelationInfo *info,
								   TupleTableSlot *slot,
								   TupleDesc tupdesc)
{
	if (info == NULL || slot == NULL || tupdesc == NULL)
		return NULL;

	fb_apply_slot_get_attrs(slot, info->key_attnums, info->key_natts);
	return fb_apply_build_identity_values(tupdesc,
										  slot->tts_values,
										  slot->tts_isnull,
										  info->key_attnums,
										  info->key_natts);
}

char *
fb_apply_build_row_identity_slot(TupleTableSlot *slot,
								   TupleDesc tupdesc)
{
	if (slot == NULL || tupdesc == NULL)
		return NULL;

	fb_apply_slot_get_attrs(slot, NULL, 0);
	return fb_apply_build_identity_values(tupdesc,
										  slot->tts_values,
										  slot->tts_isnull,
										  NULL,
										  0);
}

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

static bool
fb_apply_process_current_emit(FbApplyContext *ctx,
							  TupleTableSlot *slot,
							  FbApplyEmit *emit)
{
	if (ctx->info->mode == FB_APPLY_KEYED)
		return fb_keyed_apply_process_current_emit(ctx->mode_state, slot, emit);

	return fb_bag_apply_process_current_emit(ctx->mode_state, slot, emit);
}

static bool
fb_apply_next_residual_emit(FbApplyContext *ctx, FbApplyEmit *emit)
{
	if (ctx->info->mode == FB_APPLY_KEYED)
		return fb_keyed_apply_next_residual_emit(ctx->mode_state, emit);

	return fb_bag_apply_next_residual_emit(ctx->mode_state, emit);
}

static Datum
fb_apply_emit_as_datum(FbApplyContext *ctx, const FbApplyEmit *emit)
{
	switch (emit->kind)
	{
		case FB_APPLY_EMIT_SLOT:
			return ExecFetchSlotHeapTupleDatum(emit->slot);
		case FB_APPLY_EMIT_TUPLE:
			return heap_copy_tuple_as_datum(emit->tuple, ctx->tupdesc);
		case FB_APPLY_EMIT_NONE:
			break;
	}

	elog(ERROR, "fb apply emitted invalid row kind");
	return (Datum) 0;
}

static void
fb_apply_emit_to_tuplestore(Tuplestorestate *tupstore, const FbApplyEmit *emit)
{
	switch (emit->kind)
	{
		case FB_APPLY_EMIT_SLOT:
			tuplestore_puttupleslot(tupstore, emit->slot);
			return;
		case FB_APPLY_EMIT_TUPLE:
			tuplestore_puttuple(tupstore, emit->tuple);
			return;
		case FB_APPLY_EMIT_NONE:
			break;
	}

	elog(ERROR, "fb apply emitted invalid row kind");
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
			   const FbReverseOpSource *source)
{
	FbApplyContext *ctx;
	Snapshot snapshot;

	ctx = palloc0(sizeof(*ctx));
	ctx->info = info;
	ctx->tupdesc = tupdesc;
	ctx->phase = FB_APPLY_PHASE_SCAN;

	if (info->mode == FB_APPLY_KEYED)
		ctx->mode_state = fb_keyed_apply_begin(info, tupdesc, source);
	else
		ctx->mode_state = fb_bag_apply_begin(info, tupdesc, source);

	ctx->rel = relation_open(info->relid, AccessShareLock);
	ctx->estimated_rows = (ctx->rel->rd_rel->reltuples > 0) ?
		(uint64) ctx->rel->rd_rel->reltuples : 0;
	ctx->progress_stride = (ctx->estimated_rows > 0) ?
		Max((uint64) 1, ctx->estimated_rows / 1024) : 1;

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

bool
fb_apply_next(FbApplyContext *ctx, Datum *result)
{
	if (ctx == NULL)
		return false;

	if (ctx->phase == FB_APPLY_PHASE_SCAN)
	{
		while (table_scan_getnextslot(ctx->scan, ForwardScanDirection, ctx->slot))
		{
			FbApplyEmit emit = {0};
			bool emitted;

			ctx->scanned_rows++;
			if (ctx->estimated_rows > 0 &&
				(ctx->scanned_rows == 1 ||
				 ctx->scanned_rows == ctx->estimated_rows ||
				 (ctx->scanned_rows % ctx->progress_stride) == 0))
				fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
										   fb_progress_map_subrange(0, 100,
																ctx->scanned_rows,
																ctx->estimated_rows),
										   NULL);

			emitted = fb_apply_process_current_emit(ctx, ctx->slot, &emit);
			if (emitted)
				*result = fb_apply_emit_as_datum(ctx, &emit);
			ExecClearTuple(ctx->slot);
			if (emitted)
				return true;
		}

		fb_apply_cleanup_resources(ctx);
		fb_apply_finish_scan_phase(ctx);
	}

	if (ctx->phase == FB_APPLY_PHASE_RESIDUAL)
	{
		FbApplyEmit emit = {0};
		bool emitted;

		emitted = fb_apply_next_residual_emit(ctx, &emit);

		if (!emitted)
		{
			ctx->phase = FB_APPLY_PHASE_DONE;
			fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
			return false;
		}

		*result = fb_apply_emit_as_datum(ctx, &emit);
		ctx->residual_emitted++;
		fb_progress_update_fraction(FB_PROGRESS_STAGE_MATERIALIZE,
									ctx->residual_emitted,
									ctx->residual_total,
									NULL);
		return true;
	}

	return false;
}

void
fb_apply_materialize(FbApplyContext *ctx, Tuplestorestate *tupstore)
{
	if (ctx == NULL || tupstore == NULL)
		return;

	if (ctx->phase == FB_APPLY_PHASE_SCAN)
	{
		while (table_scan_getnextslot(ctx->scan, ForwardScanDirection, ctx->slot))
		{
			FbApplyEmit emit = {0};
			bool emitted;

			ctx->scanned_rows++;
			if (ctx->estimated_rows > 0 &&
				(ctx->scanned_rows == 1 ||
				 ctx->scanned_rows == ctx->estimated_rows ||
				 (ctx->scanned_rows % ctx->progress_stride) == 0))
				fb_progress_update_percent(FB_PROGRESS_STAGE_APPLY,
										   fb_progress_map_subrange(0, 100,
																ctx->scanned_rows,
																ctx->estimated_rows),
										   NULL);

			emitted = fb_apply_process_current_emit(ctx, ctx->slot, &emit);
			if (emitted)
				fb_apply_emit_to_tuplestore(tupstore, &emit);
			ExecClearTuple(ctx->slot);
		}

		fb_apply_cleanup_resources(ctx);
		fb_apply_finish_scan_phase(ctx);
	}

	if (ctx->phase == FB_APPLY_PHASE_RESIDUAL)
	{
		FbApplyEmit emit = {0};

		while (fb_apply_next_residual_emit(ctx, &emit))
		{
			fb_apply_emit_to_tuplestore(tupstore, &emit);
			ctx->residual_emitted++;
			fb_progress_update_fraction(FB_PROGRESS_STAGE_MATERIALIZE,
										ctx->residual_emitted,
										ctx->residual_total,
										NULL);
			emit.kind = FB_APPLY_EMIT_NONE;
			emit.slot = NULL;
			emit.tuple = NULL;
		}

		ctx->phase = FB_APPLY_PHASE_DONE;
		fb_progress_update_percent(FB_PROGRESS_STAGE_MATERIALIZE, 100, NULL);
	}
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
