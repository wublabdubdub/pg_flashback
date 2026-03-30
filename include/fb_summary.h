/*
 * fb_summary.h
 *    WAL segment summary sidecar APIs.
 */

#ifndef FB_SUMMARY_H
#define FB_SUMMARY_H

#include "postgres.h"

#include "access/xlogdefs.h"

#include "fb_common.h"

typedef struct FbSummaryBuildCandidate
{
	char path[MAXPGPATH];
	TimeLineID timeline_id;
	XLogSegNo segno;
	off_t bytes;
	int wal_seg_size;
	int source_kind;
	bool has_next_segment;
	char next_path[MAXPGPATH];
	TimeLineID next_timeline_id;
	XLogSegNo next_segno;
	off_t next_bytes;
} FbSummaryBuildCandidate;

bool fb_summary_segment_matches(const char *path,
								off_t bytes,
								TimeLineID timeline_id,
								XLogSegNo segno,
								int wal_seg_size,
								int source_kind,
								const FbRelationInfo *info,
								bool *summary_available,
								bool *hit);

int fb_summary_collect_build_candidates(FbSummaryBuildCandidate **candidates_out,
										 bool skip_unstable_tail);
void fb_summary_free_build_candidates(FbSummaryBuildCandidate *candidates);
bool fb_summary_candidate_summary_exists(const FbSummaryBuildCandidate *candidate);
bool fb_summary_build_candidate(const FbSummaryBuildCandidate *candidate);
uint64 fb_summary_candidate_identity_hash(const FbSummaryBuildCandidate *candidate);
uint64 fb_summary_meta_summary_size_bytes(uint32 *file_count_out);

int fb_summary_build_available_debug_impl(void);
char *fb_summary_meta_stats_cstring(void);

#endif
