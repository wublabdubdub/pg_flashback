CREATE FUNCTION fb_version()
RETURNS text
AS 'MODULE_PATHNAME', 'fb_version'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_check_relation(regclass)
RETURNS text
AS 'MODULE_PATHNAME', 'fb_check_relation'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_pg_flashback_support(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'fb_pg_flashback_support'
LANGUAGE C;

CREATE FUNCTION pg_flashback(anyelement, text)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'pg_flashback'
LANGUAGE C
SUPPORT fb_pg_flashback_support;

CREATE FUNCTION pg_flashback_debug_unresolv_xid(regclass, timestamptz)
RETURNS TABLE (
	xid bigint,
	xid_role text,
	resolved_by text,
	fallback_reason text,
	top_xid bigint,
	commit_ts timestamptz,
	summary_missing_segments integer,
	fallback_windows integer,
	diag text
)
AS 'MODULE_PATHNAME', 'pg_flashback_debug_unresolv_xid'
LANGUAGE C
STRICT;

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

COMMENT ON FUNCTION fb_version() IS
'Return current pg_flashback extension version.';

COMMENT ON FUNCTION fb_check_relation(regclass) IS
'Inspect current scaffold mode selection for a relation.';

COMMENT ON FUNCTION pg_flashback(anyelement, text) IS
'Return flashback rows directly for NULL::schema.table and target timestamp text, without result-table materialization or AS t(...).';

COMMENT ON FUNCTION pg_flashback_debug_unresolv_xid(regclass, timestamptz) IS
'Show xid-level fallback and unresolved diagnostics for a flashback target, so copied logs and WAL files can be analyzed offline.';

COMMENT ON VIEW pg_flashback_summary_progress IS
'Show user-facing summary coverage progress, status source, external daemon freshness, and the latest query-side summary fallback status.';
