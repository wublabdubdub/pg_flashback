/*
 * fb_replay.h
 *    Page replay and forward-op extraction interfaces.
 */

#ifndef FB_REPLAY_H
#define FB_REPLAY_H

#include "utils/relcache.h"

#include "fb_common.h"
#include "fb_reverse_ops.h"
#include "fb_wal.h"

/*
 * FbReplayResult
 *    Stores replay results.
 */

typedef struct FbReplayResult
{
	uint64 records_replayed;
	uint64 blocks_materialized;
	uint64 replay_errors;
	uint64 target_insert_count;
	uint64 target_delete_count;
	uint64 target_update_count;
	uint64 tracked_bytes;
	uint64 memory_limit_bytes;
	uint32 precomputed_missing_blocks;
	uint32 discover_rounds;
	uint32 discover_skipped;
	uint32 summary_anchor_hits;
	uint32 summary_anchor_fallback;
	uint32 summary_anchor_segments_read;
} FbReplayResult;

typedef struct FbReplayDiscoverState FbReplayDiscoverState;
typedef struct FbReplayWarmState FbReplayWarmState;

FbReplayDiscoverState *fb_replay_discover(const FbRelationInfo *info,
										  const FbWalRecordIndex *index);
void fb_replay_discover_destroy(FbReplayDiscoverState *state);

FbReplayWarmState *fb_replay_warm(const FbRelationInfo *info,
								  const FbWalRecordIndex *index,
								  const FbReplayDiscoverState *discover,
								  FbReplayResult *result);
void fb_replay_warm_destroy(FbReplayWarmState *state);

void fb_replay_final_build_reverse_source(const FbRelationInfo *info,
										  const FbWalRecordIndex *index,
										  TupleDesc tupdesc,
										  const FbReplayWarmState *warm,
										  FbReplayResult *result,
										  FbReverseOpSource *source);

/*
 * fb_replay_execute
 *    Replay API.
 */

void fb_replay_execute(const FbRelationInfo *info,
					   const FbWalRecordIndex *index,
					   FbReplayResult *result);
/*
 * fb_replay_build_reverse_source
 *    Replay API.
 */

void fb_replay_build_reverse_source(const FbRelationInfo *info,
									  const FbWalRecordIndex *index,
									  TupleDesc tupdesc,
									  FbReplayResult *result,
									  FbReverseOpSource *source);

#endif
