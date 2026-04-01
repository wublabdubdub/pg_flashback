DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

DO $$
BEGIN
	IF to_regprocedure('fb_summary_service_plan_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_service_plan_debug()';
	END IF;
	IF to_regprocedure('fb_summary_cleanup_plan_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_cleanup_plan_debug()';
	END IF;
	IF to_regprocedure('fb_summary_build_available_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_build_available_debug()';
	END IF;
END;
$$;

CREATE FUNCTION fb_summary_service_plan_debug()
RETURNS TABLE (
	recent_rank integer,
	hot_window integer,
	queue_kind text,
	timeline_id integer,
	segno bigint,
	summary_exists boolean
)
AS '$libdir/pg_flashback', 'fb_summary_service_plan_debug'
LANGUAGE C;

CREATE FUNCTION fb_summary_cleanup_plan_debug()
RETURNS TABLE (
	timeline_id integer,
	segno bigint,
	summary_exists boolean,
	protected_by_snapshot boolean,
	protected_by_active boolean
)
AS '$libdir/pg_flashback', 'fb_summary_cleanup_plan_debug'
LANGUAGE C;

CREATE FUNCTION fb_summary_build_available_debug()
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_build_available_debug'
LANGUAGE C;

SELECT pg_switch_wal() IS NOT NULL;
SELECT pg_switch_wal() IS NOT NULL;
SELECT pg_switch_wal() IS NOT NULL;

DO $$
DECLARE
	deadline timestamptz := clock_timestamp() + interval '10 seconds';
	ready boolean := false;
BEGIN
	LOOP
		SELECT count(*) FILTER (WHERE summary_exists) > 0
		INTO ready
		FROM fb_summary_cleanup_plan_debug();

		EXIT WHEN ready OR clock_timestamp() >= deadline;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SELECT count(*) > 0 AS has_stable_candidates
FROM fb_summary_service_plan_debug();

SELECT count(*) FILTER (WHERE queue_kind = 'hot' AND recent_rank > hot_window) = 0
	   AS hot_is_recent,
	   count(*) FILTER (WHERE queue_kind = 'cold' AND recent_rank <= hot_window) = 0
	   AS cold_is_older
FROM fb_summary_service_plan_debug();

SELECT count(*) FILTER (WHERE NOT protected_by_snapshot) = 0
	   AS snapshot_rows_protected,
	   count(*) FILTER (WHERE protected_by_active AND NOT protected_by_snapshot) = 0
	   AS active_rows_protected
FROM fb_summary_cleanup_plan_debug();

SELECT to_regclass('pg_flashback_summary_progress') IS NOT NULL
	   AS has_user_progress_view,
	   to_regclass('pg_flashback_summary_service_debug') IS NOT NULL
	   AS has_debug_progress_view;

SELECT count(*) = 19 AS sane_user_columns
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'pg_flashback_summary_progress';

SELECT stable_oldest_segno <= stable_newest_segno AS sane_stable_segno_range,
	   stable_oldest_ts IS NULL OR stable_newest_ts IS NULL OR
	   stable_oldest_ts <= stable_newest_ts AS sane_stable_ts_range,
	   near_contiguous_through_ts IS NULL OR stable_oldest_ts IS NULL OR
	   near_contiguous_through_ts >= stable_oldest_ts AS sane_near_frontier,
	   far_contiguous_until_ts IS NULL OR stable_newest_ts IS NULL OR
	   far_contiguous_until_ts <= stable_newest_ts AS sane_far_frontier,
	   missing_segments = 0 OR first_gap_from_newest_segno IS NOT NULL
	   AS sane_newest_gap,
	   missing_segments = 0 OR first_gap_from_oldest_segno IS NOT NULL
	   AS sane_oldest_gap
FROM pg_flashback_summary_progress;

CREATE TEMP TABLE fb_summary_gap_fixture (
	seg_name text NOT NULL,
	segno bigint NOT NULL
);

DO $$
DECLARE
	data_dir text := current_setting('data_directory');
	archive_dir text := '/tmp/fb_summary_progress_gap_archive';
	pgwal_dir text := '/tmp/fb_summary_progress_gap_pgwal';
	rec record;
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || archive_dir || ' ' || pgwal_dir ||
		' && mkdir -p ' || archive_dir || ' ' || pgwal_dir ||
		' && rm -f ' || data_dir || '/pg_flashback/meta/summary/*' ||
		' ' || data_dir || '/pg_flashback/recovered_wal/*');

	INSERT INTO fb_summary_gap_fixture(seg_name, segno)
	WITH wal AS (
		SELECT name,
			   (('x' || substr(name, 9, 8))::bit(32)::bigint *
				(4294967296::bigint / pg_size_bytes(current_setting('wal_segment_size'))) +
				('x' || substr(name, 17, 8))::bit(32)::bigint) AS segno,
			   lead(
				   (('x' || substr(name, 9, 8))::bit(32)::bigint *
					(4294967296::bigint / pg_size_bytes(current_setting('wal_segment_size'))) +
					('x' || substr(name, 17, 8))::bit(32)::bigint),
				   1
			   ) OVER (ORDER BY name) AS next_segno_1,
			   lead(
				   (('x' || substr(name, 9, 8))::bit(32)::bigint *
					(4294967296::bigint / pg_size_bytes(current_setting('wal_segment_size'))) +
					('x' || substr(name, 17, 8))::bit(32)::bigint),
				   2
			   ) OVER (ORDER BY name) AS next_segno_2
		FROM pg_ls_dir(data_dir || '/pg_wal') AS d(name)
		WHERE d.name ~ '^[0-9A-F]{24}$'
	),
	pick AS (
		SELECT segno AS start_segno
		FROM wal
		WHERE next_segno_1 = segno + 1
		  AND next_segno_2 = segno + 2
		ORDER BY segno DESC
		LIMIT 1
	)
	SELECT wal.name, wal.segno
	FROM wal, pick
	WHERE wal.segno BETWEEN pick.start_segno AND pick.start_segno + 2
	ORDER BY wal.segno;

	IF (SELECT count(*) FROM fb_summary_gap_fixture) <> 3 THEN
		RAISE EXCEPTION 'failed to prepare three consecutive WAL segments for summary progress gap fixture';
	END IF;

	FOR rec IN
		SELECT seg_name
		FROM fb_summary_gap_fixture
		ORDER BY segno
	LOOP
		EXECUTE format(
			'COPY (SELECT '''') TO PROGRAM %L',
			'cp ' || quote_literal(data_dir || '/pg_wal/' || rec.seg_name) || ' ' ||
			quote_literal(archive_dir || '/' || rec.seg_name));
	END LOOP;
END;
$$;

SET pg_flashback.archive_dest = '/tmp/fb_summary_progress_gap_archive';
SET pg_flashback.debug_pg_wal_dir = '/tmp/fb_summary_progress_gap_pgwal';

SELECT count(*) = 3 AS prepared_gap_fixture
FROM fb_summary_gap_fixture;

SELECT fb_summary_build_available_debug() > 0 AS built_gap_fixture_summary;

SELECT missing_segments = 0 AS fixture_starts_without_gap,
	   first_gap_from_newest_segno IS NULL AS fixture_has_no_newest_gap,
	   first_gap_from_oldest_segno IS NULL AS fixture_has_no_oldest_gap
FROM pg_flashback_summary_progress;

DO $$
DECLARE
	archive_dir text := '/tmp/fb_summary_progress_gap_archive';
	deleted_name text;
BEGIN
	SELECT seg_name
	INTO deleted_name
	FROM fb_summary_gap_fixture
	ORDER BY segno
	OFFSET 1
	LIMIT 1;

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -f ' || quote_literal(archive_dir || '/' || deleted_name));
END;
$$;

SELECT missing_segments = 1 AS tracks_deleted_wal_gap,
	   first_gap_from_newest_segno = (SELECT segno FROM fb_summary_gap_fixture ORDER BY segno OFFSET 1 LIMIT 1)
	   AS newest_gap_matches_deleted,
	   first_gap_from_oldest_segno = (SELECT segno FROM fb_summary_gap_fixture ORDER BY segno OFFSET 1 LIMIT 1)
	   AS oldest_gap_matches_deleted,
	   progress_pct < 100.0 AS progress_pct_drops
FROM pg_flashback_summary_progress;

DROP TABLE IF EXISTS fb_summary_query_status_target;
DROP TABLE IF EXISTS fb_summary_query_status_mark;

CREATE TABLE fb_summary_query_status_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_summary_query_status_target VALUES (0, 'seed');

CREATE TABLE fb_summary_query_status_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_summary_query_status_mark VALUES (clock_timestamp());

INSERT INTO fb_summary_query_status_target VALUES (1, 'alpha');
UPDATE fb_summary_query_status_target
SET payload = 'alpha-updated'
WHERE id = 1;
DELETE FROM fb_summary_query_status_target
WHERE id = 0;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dest = %L', current_setting('data_directory') || '/pg_wal');
	EXECUTE format('SET pg_flashback.debug_pg_wal_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS built_query_status_summary;

DO $$
DECLARE
	data_dir text := current_setting('data_directory');
BEGIN
	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -f ' || quote_literal(data_dir || '/pg_flashback/meta/summary/*'));
END;
$$;

SET pg_flashback.show_progress = off;

SELECT count(*) = 1 AS query_after_summary_rm_returns_rows
FROM pg_flashback(
	NULL::public.fb_summary_query_status_target,
	(SELECT target_ts::text FROM fb_summary_query_status_mark)
);

SELECT last_query_observed_at IS NOT NULL AS query_status_records_timestamp,
	   last_query_summary_ready = false AS query_status_reports_degraded,
	   last_query_summary_span_fallback_segments > 0 AS query_status_tracks_span_fallback,
	   last_query_metadata_fallback_segments > 0 AS query_status_tracks_metadata_fallback
FROM pg_flashback_summary_progress;

SELECT fb_summary_build_available_debug() > 0 AS rebuilt_query_status_summary;

SELECT count(*) = 1 AS query_after_summary_rebuild_returns_rows
FROM pg_flashback(
	NULL::public.fb_summary_query_status_target,
	(SELECT target_ts::text FROM fb_summary_query_status_mark)
);

SELECT last_query_summary_ready = true AS query_status_recovers_ready,
	   coalesce(last_query_summary_span_fallback_segments, -1) = 0 AS query_status_clears_span_fallback,
	   coalesce(last_query_metadata_fallback_segments, -1) = 0 AS query_status_clears_metadata_fallback
FROM pg_flashback_summary_progress;

SELECT pending_hot + pending_cold + running_hot + running_cold <= queue_capacity
	   AS sane_queue,
	   summary_files >= 0 AND summary_bytes >= 0
	   AS sane_summary_storage
FROM pg_flashback_summary_service_debug;
