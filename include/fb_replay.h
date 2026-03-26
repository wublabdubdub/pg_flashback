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
} FbReplayResult;

/*
 * fb_replay_execute
 *    Replay API.
 */

void fb_replay_execute(const FbRelationInfo *info,
					   const FbWalRecordIndex *index,
					   FbReplayResult *result);
/*
 * fb_replay_build_forward_ops
 *    Replay API.
 */

void fb_replay_build_forward_ops(const FbRelationInfo *info,
								 const FbWalRecordIndex *index,
								 TupleDesc tupdesc,
								 FbReplayResult *result,
								 FbForwardOpStream *stream);

#endif
