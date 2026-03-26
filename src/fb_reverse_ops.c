/*
 * fb_reverse_ops.c
 *    Forward-op to reverse-op conversion.
 */

#include "postgres.h"

#include "utils/builtins.h"

#include "fb_memory.h"
#include "fb_progress.h"
#include "fb_replay.h"
#include "fb_reverse_ops.h"

/*
 * fb_reverse_stream_append
 *    Reverse-op helper.
 */

static void
fb_reverse_stream_append(FbReverseOpStream *stream, const FbReverseOp *op)
{
	uint32 old_capacity;

	if (stream->count == stream->capacity)
	{
		old_capacity = stream->capacity;
		stream->capacity = (stream->capacity == 0) ? 32 : stream->capacity * 2;
		fb_memory_charge_bytes(&stream->tracked_bytes,
							   stream->memory_limit_bytes,
							   sizeof(FbReverseOp) * (stream->capacity - old_capacity),
							   "ReverseOp array");
		if (stream->ops == NULL)
			stream->ops = palloc0(sizeof(FbReverseOp) * stream->capacity);
		else
		{
			stream->ops = repalloc(stream->ops,
								   sizeof(FbReverseOp) * stream->capacity);
			MemSet(stream->ops + old_capacity, 0,
				   sizeof(FbReverseOp) * (stream->capacity - old_capacity));
		}
	}

	stream->ops[stream->count++] = *op;
}

/*
 * fb_reverse_op_cmp
 *    Reverse-op helper.
 */

static int
fb_reverse_op_cmp(const void *lhs, const void *rhs)
{
	const FbReverseOp *left = (const FbReverseOp *) lhs;
	const FbReverseOp *right = (const FbReverseOp *) rhs;

	if (left->commit_lsn < right->commit_lsn)
		return 1;
	if (left->commit_lsn > right->commit_lsn)
		return -1;
	if (left->record_lsn < right->record_lsn)
		return 1;
	if (left->record_lsn > right->record_lsn)
		return -1;
	return 0;
}

/*
 * fb_build_forward_ops
 *    Reverse-op entry point.
 */

void
fb_build_forward_ops(const FbRelationInfo *info,
					 const FbWalRecordIndex *index,
					 TupleDesc tupdesc,
					 FbForwardOpStream *stream)
{
	FbReplayResult replay_result;

	fb_replay_build_forward_ops(info, index, tupdesc, &replay_result, stream);
	stream->tracked_bytes = replay_result.tracked_bytes;
	stream->memory_limit_bytes = replay_result.memory_limit_bytes;
}

/*
 * fb_build_reverse_ops
 *    Reverse-op entry point.
 */

void
fb_build_reverse_ops(const FbForwardOpStream *forward,
					 FbReverseOpStream *reverse)
{
	uint32 i;

	MemSet(reverse, 0, sizeof(*reverse));
	reverse->tracked_bytes = forward->tracked_bytes;
	reverse->memory_limit_bytes = forward->memory_limit_bytes;
	fb_progress_enter_stage(FB_PROGRESS_STAGE_BUILD_REVERSE, NULL);

	for (i = 0; i < forward->count; i++)
	{
		const FbForwardOp *forward_op = &forward->ops[i];
		FbReverseOp reverse_op;

		MemSet(&reverse_op, 0, sizeof(reverse_op));
		reverse_op.xid = forward_op->xid;
		reverse_op.commit_ts = forward_op->commit_ts;
		reverse_op.commit_lsn = forward_op->commit_lsn;
		reverse_op.record_lsn = forward_op->record_lsn;

		switch (forward_op->type)
		{
			case FB_FORWARD_INSERT:
				reverse_op.type = FB_REVERSE_REMOVE;
				reverse_op.new_row = forward_op->new_row;
				break;
			case FB_FORWARD_DELETE:
				reverse_op.type = FB_REVERSE_ADD;
				reverse_op.old_row = forward_op->old_row;
				break;
			case FB_FORWARD_UPDATE:
				reverse_op.type = FB_REVERSE_REPLACE;
				reverse_op.old_row = forward_op->old_row;
				reverse_op.new_row = forward_op->new_row;
				break;
		}

		fb_reverse_stream_append(reverse, &reverse_op);
		fb_progress_update_fraction(FB_PROGRESS_STAGE_BUILD_REVERSE,
									 (uint64) i + 1,
									 forward->count,
									 NULL);
	}

	if (reverse->count > 1)
		qsort(reverse->ops, reverse->count, sizeof(FbReverseOp),
			  fb_reverse_op_cmp);

	fb_progress_update_percent(FB_PROGRESS_STAGE_BUILD_REVERSE, 100, NULL);
}

/*
 * fb_forward_ops_debug_summary
 *    Reverse-op entry point.
 */

char *
fb_forward_ops_debug_summary(const FbForwardOpStream *stream)
{
	return psprintf("forward_ops=%u", stream->count);
}

/*
 * fb_reverse_ops_debug_summary
 *    Reverse-op entry point.
 */

char *
fb_reverse_ops_debug_summary(const FbReverseOpStream *stream)
{
	return psprintf("reverse_ops=%u", stream->count);
}
