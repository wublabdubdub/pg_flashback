/*
 * fb_summary.h
 *    WAL segment summary sidecar APIs.
 */

#ifndef FB_SUMMARY_H
#define FB_SUMMARY_H

#include "postgres.h"

#include "access/xlogdefs.h"

#include "fb_common.h"

typedef struct FbSummaryQueryCache FbSummaryQueryCache;

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

typedef struct FbSummarySpan
{
	XLogRecPtr start_lsn;
	XLogRecPtr end_lsn;
	uint16 flags;
} FbSummarySpan;

typedef struct FbSummaryXidOutcome
{
	TransactionId xid;
	uint8 status;
	TimestampTz commit_ts;
	XLogRecPtr commit_lsn;
} FbSummaryXidOutcome;

typedef struct FbSummaryXidAssignment
{
	TransactionId subxid;
	TransactionId top_xid;
} FbSummaryXidAssignment;

typedef struct FbSummaryUnsafeFact
{
	uint8 reason;
	uint8 scope;
	uint8 storage_op;
	uint8 match_kind;
	TransactionId xid;
	XLogRecPtr record_lsn;
	RelFileLocator locator;
	Oid db_oid;
	Oid rel_oid;
} FbSummaryUnsafeFact;

typedef struct FbSummaryBlockAnchor
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blkno;
	XLogRecPtr anchor_lsn;
	uint16 flags;
} FbSummaryBlockAnchor;

typedef struct FbSummaryPayloadLocator
{
	XLogRecPtr record_start_lsn;
	uint8 kind;
	uint8 flags;
} FbSummaryPayloadLocator;

FbSummaryQueryCache *fb_summary_query_cache_create(MemoryContext mcxt);
bool fb_summary_segment_lookup_spans_cached(const char *path,
											off_t bytes,
											TimeLineID timeline_id,
											XLogSegNo segno,
											int wal_seg_size,
											int source_kind,
											const FbRelationInfo *info,
											FbSummaryQueryCache *cache,
											const FbSummarySpan **spans_out,
											uint32 *span_count_out);
bool fb_summary_segment_lookup_xid_outcomes_cached(const char *path,
												   off_t bytes,
												   TimeLineID timeline_id,
												   XLogSegNo segno,
												   int wal_seg_size,
												   int source_kind,
												   FbSummaryQueryCache *cache,
												   FbSummaryXidOutcome **outcomes_out,
												   uint32 *outcome_count_out);
bool fb_summary_segment_lookup_xid_outcome_for_xid_cached(const char *path,
														  off_t bytes,
														  TimeLineID timeline_id,
														  XLogSegNo segno,
														  int wal_seg_size,
														  int source_kind,
														  TransactionId xid,
														  FbSummaryQueryCache *cache,
														  FbSummaryXidOutcome *outcome_out,
														  bool *found_out);
bool fb_summary_segment_lookup_xid_outcome_slice_cached(const char *path,
														off_t bytes,
														TimeLineID timeline_id,
														XLogSegNo segno,
														int wal_seg_size,
														int source_kind,
														FbSummaryQueryCache *cache,
														const FbSummaryXidOutcome **outcomes_out,
														uint32 *outcome_count_out);
bool fb_summary_segment_lookup_xid_assignment_slice_cached(const char *path,
														   off_t bytes,
														   TimeLineID timeline_id,
														   XLogSegNo segno,
														   int wal_seg_size,
														   int source_kind,
														   FbSummaryQueryCache *cache,
														   const FbSummaryXidAssignment **assignments_out,
														   uint32 *assignment_count_out);
bool fb_summary_segment_lookup_touched_xids_cached(const char *path,
												   off_t bytes,
												   TimeLineID timeline_id,
												   XLogSegNo segno,
												   int wal_seg_size,
												   int source_kind,
												   const FbRelationInfo *info,
												   FbSummaryQueryCache *cache,
												   TransactionId **xids_out,
												   uint32 *xid_count_out);
bool fb_summary_segment_lookup_unsafe_facts_cached(const char *path,
												   off_t bytes,
												   TimeLineID timeline_id,
												   XLogSegNo segno,
												   int wal_seg_size,
												   int source_kind,
												   const FbRelationInfo *info,
												   FbSummaryQueryCache *cache,
												   FbSummaryUnsafeFact **facts_out,
												   uint32 *fact_count_out);
bool fb_summary_segment_lookup_block_anchors_cached(const char *path,
													off_t bytes,
													TimeLineID timeline_id,
													XLogSegNo segno,
													int wal_seg_size,
													int source_kind,
													const FbRelationInfo *info,
													FbSummaryQueryCache *cache,
													FbSummaryBlockAnchor **anchors_out,
													uint32 *anchor_count_out);
bool fb_summary_segment_lookup_payload_locators_cached(const char *path,
													   off_t bytes,
													   TimeLineID timeline_id,
													   XLogSegNo segno,
													   int wal_seg_size,
													   int source_kind,
													   const FbRelationInfo *info,
													   FbSummaryQueryCache *cache,
													   FbSummaryPayloadLocator **locators_out,
													   uint32 *locator_count_out);
uint64 fb_summary_query_cache_payload_locator_public_builds(FbSummaryQueryCache *cache);
uint64 fb_summary_query_cache_span_public_builds(FbSummaryQueryCache *cache);

bool fb_summary_segment_matches(const char *path,
								off_t bytes,
								TimeLineID timeline_id,
								XLogSegNo segno,
								int wal_seg_size,
								int source_kind,
								const FbRelationInfo *info,
								bool *summary_available,
								bool *hit);
bool fb_summary_segment_lookup_spans(const char *path,
									 off_t bytes,
									 TimeLineID timeline_id,
									 XLogSegNo segno,
									 int wal_seg_size,
									 int source_kind,
									 const FbRelationInfo *info,
									 const FbSummarySpan **spans_out,
									 uint32 *span_count_out);
bool fb_summary_segment_lookup_xid_outcomes(const char *path,
											off_t bytes,
											TimeLineID timeline_id,
											XLogSegNo segno,
											int wal_seg_size,
											int source_kind,
											FbSummaryXidOutcome **outcomes_out,
											uint32 *outcome_count_out);

int fb_summary_collect_build_candidates(FbSummaryBuildCandidate **candidates_out,
										 bool skip_unstable_tail);
void fb_summary_free_build_candidates(FbSummaryBuildCandidate *candidates);
bool fb_summary_candidate_summary_exists(const FbSummaryBuildCandidate *candidate);
bool fb_summary_candidate_time_bounds(const FbSummaryBuildCandidate *candidate,
									  TimestampTz *oldest_xact_ts_out,
									  TimestampTz *newest_xact_ts_out);
bool fb_summary_build_candidate(const FbSummaryBuildCandidate *candidate);
uint64 fb_summary_candidate_identity_hash(const FbSummaryBuildCandidate *candidate);
uint64 fb_summary_meta_summary_size_bytes(uint32 *file_count_out);

int fb_summary_build_available_debug_impl(void);
char *fb_summary_meta_stats_cstring(void);

#endif
