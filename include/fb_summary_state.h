/*
 * fb_summary_state.h
 *    State-file helpers for external summary daemon integration.
 */

#ifndef FB_SUMMARY_STATE_H
#define FB_SUMMARY_STATE_H

#include "postgres.h"

typedef struct FbSummaryDaemonState
{
	bool present;
	bool stale;
	bool service_enabled;
	int daemon_pid;
	int registered_workers;
	int active_workers;
	int queue_capacity;
	int hot_window;
	int pending_hot;
	int pending_cold;
	int running_hot;
	int running_cold;
	TimeLineID snapshot_timeline_id;
	XLogSegNo snapshot_oldest_segno;
	XLogSegNo snapshot_newest_segno;
	int snapshot_hot_candidates;
	int snapshot_cold_candidates;
	uint64 scan_count;
	uint64 enqueue_count;
	uint64 build_count;
	uint64 cleanup_count;
	TimestampTz last_scan_at;
	TimestampTz throughput_window_started_at;
	TimestampTz last_build_at;
	uint64 throughput_window_builds;
} FbSummaryDaemonState;

typedef struct FbSummaryQueryHint
{
	bool present;
	TimestampTz observed_at;
	uint32 summary_span_fallback_segments;
	uint32 metadata_fallback_segments;
} FbSummaryQueryHint;

bool fb_summary_state_load(FbSummaryDaemonState *state);
bool fb_summary_debug_state_load(FbSummaryDaemonState *state);
bool fb_summary_query_hint_load(FbSummaryQueryHint *hint);
void fb_summary_query_hint_write(TimestampTz observed_at,
								 uint32 summary_span_fallback_segments,
								 uint32 metadata_fallback_segments);

#endif
