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

CREATE FUNCTION pg_flashback_to(regclass, text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_flashback_rewind'
LANGUAGE C
STRICT;

CREATE FUNCTION pg_flashback(anyelement, text)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'pg_flashback'
LANGUAGE C
SUPPORT fb_pg_flashback_support;

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
	progress_pct double precision
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
	last_scan_at timestamptz
)
AS 'MODULE_PATHNAME', 'fb_summary_service_debug_internal'
LANGUAGE C;

CREATE VIEW pg_flashback_summary_progress AS
SELECT *
FROM fb_summary_progress_internal();

CREATE VIEW pg_flashback_summary_service_debug AS
SELECT *
FROM fb_summary_service_debug_internal();

COMMENT ON FUNCTION fb_version() IS
'Return current pg_flashback development version.';

COMMENT ON FUNCTION fb_check_relation(regclass) IS
'Inspect current scaffold mode selection for a relation.';

COMMENT ON FUNCTION pg_flashback_to(regclass, text) IS
'Rewind the keyed source table itself back to the target timestamp by applying batched UPDATE/INSERT/DELETE operations in place.';

COMMENT ON FUNCTION pg_flashback(anyelement, text) IS
'Return flashback rows directly for NULL::schema.table and target timestamp text, without result-table materialization or AS t(...).';

COMMENT ON VIEW pg_flashback_summary_progress IS
'Show user-facing summary coverage progress, including stable-window frontiers and first-gap coordinates.';

COMMENT ON VIEW pg_flashback_summary_service_debug IS
'Show internal summary service state such as launcher, workers, queue counters, and summary storage stats.';
