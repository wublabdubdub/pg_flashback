DROP VIEW IF EXISTS pg_flashback_summary_progress;
DROP VIEW IF EXISTS pg_flashback_summary_service_debug;

DROP FUNCTION IF EXISTS fb_summary_progress_internal();
DROP FUNCTION IF EXISTS fb_summary_service_debug_internal();

CREATE FUNCTION fb_summary_progress_internal()
RETURNS TABLE (
	service_enabled boolean,
	timeline_id integer,
	stable_oldest_segno bigint,
	stable_newest_segno bigint,
	stable_oldest_ts timestamptz,
	stable_newest_ts timestamptz,
	near_contiguous_through_ts timestamptz,
	far_contiguous_until_ts timestamptz,
	first_gap_from_newest_segno bigint,
	first_gap_from_newest_ts timestamptz,
	first_gap_from_oldest_segno bigint,
	first_gap_from_oldest_ts timestamptz,
	completed_segments bigint,
	missing_segments bigint,
	progress_pct double precision,
	last_query_observed_at timestamptz,
	last_query_summary_ready boolean,
	last_query_summary_span_fallback_segments bigint,
	last_query_metadata_fallback_segments bigint,
	estimated_completion_at timestamptz,
	state_source text,
	daemon_state_present boolean,
	daemon_state_stale boolean,
	daemon_state_published_at timestamptz
)
AS 'MODULE_PATHNAME', 'fb_summary_progress_internal'
LANGUAGE C;

CREATE FUNCTION fb_summary_service_debug_internal()
RETURNS TABLE (
	service_enabled boolean,
	launcher_pid integer,
	registered_workers integer,
	active_workers integer,
	queue_capacity integer,
	hot_window integer,
	pending_hot integer,
	pending_cold integer,
	running_hot integer,
	running_cold integer,
	snapshot_timeline_id integer,
	snapshot_oldest_segno bigint,
	snapshot_newest_segno bigint,
	snapshot_hot_candidates integer,
	snapshot_cold_candidates integer,
	stable_candidates bigint,
	completed_summaries bigint,
	missing_summaries bigint,
	hot_missing_summaries bigint,
	cold_missing_summaries bigint,
	summary_files bigint,
	summary_bytes bigint,
	scan_count bigint,
	enqueue_count bigint,
	build_count bigint,
	cleanup_count bigint,
	last_scan_at timestamptz,
	state_source text,
	daemon_state_present boolean,
	daemon_state_stale boolean,
	daemon_state_published_at timestamptz
)
AS 'MODULE_PATHNAME', 'fb_summary_service_debug_internal'
LANGUAGE C;

CREATE VIEW pg_flashback_summary_progress AS
SELECT *
FROM fb_summary_progress_internal();

CREATE VIEW pg_flashback_summary_service_debug AS
SELECT *
FROM fb_summary_service_debug_internal();

COMMENT ON VIEW pg_flashback_summary_progress IS
'Show user-facing summary coverage progress, status source, external daemon freshness, and the latest query-side summary fallback status.';

COMMENT ON VIEW pg_flashback_summary_service_debug IS
'Show internal summary service state such as state source, external daemon freshness, launcher, workers, queue counters, and summary storage stats.';
