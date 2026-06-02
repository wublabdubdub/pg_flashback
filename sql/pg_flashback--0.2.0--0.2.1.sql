DROP VIEW IF EXISTS pg_flashback_summary_progress;

DROP FUNCTION IF EXISTS fb_summary_progress_internal();

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

CREATE VIEW pg_flashback_summary_progress AS
SELECT *
FROM fb_summary_progress_internal();

COMMENT ON VIEW pg_flashback_summary_progress IS
'Show user-facing summary coverage progress, status source, external daemon freshness, and the latest query-side summary fallback status.';
