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

CREATE TABLE fb_wal_scan_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE TABLE fb_wal_scan_mark (
	target_ts timestamptz NOT NULL
);

CREATE TABLE fb_wal_scan_switch_mark (
	target_ts timestamptz NOT NULL
);

CREATE TABLE fb_wal_scan_noise (
	id bigserial PRIMARY KEY,
	payload text
);

INSERT INTO fb_wal_scan_target VALUES (0, repeat('seed', 128));
INSERT INTO fb_wal_scan_mark VALUES (clock_timestamp());

BEGIN;
INSERT INTO fb_wal_scan_target VALUES (1, repeat('commit', 128));
COMMIT;

BEGIN;
INSERT INTO fb_wal_scan_target VALUES (2, repeat('abort', 128));
ROLLBACK;

SET pg_flashback.parallel_segment_scan = off;
SELECT fb_scan_wal_debug('fb_wal_scan_target'::regclass,
						 (SELECT target_ts FROM fb_wal_scan_mark)) AS summary
\gset

SELECT :'summary' LIKE '%parallel=off%' AS parallel_off,
	   :'summary' LIKE '%prefilter=off%' AS prefilter_off,
	   :'summary' ~ 'visited_segments=[0-9]+/[0-9]+' AS visited_seen,
	   :'summary' LIKE '%complete=true%' AS complete_seen;

SET pg_flashback.parallel_segment_scan = on;
SELECT fb_scan_wal_debug('fb_wal_scan_target'::regclass,
						 (SELECT target_ts FROM fb_wal_scan_mark)) AS summary
\gset

SELECT :'summary' LIKE '%parallel=on%' AS parallel_on,
	   :'summary' LIKE '%prefilter=on%' AS prefilter_on,
	   :'summary' ~ 'visited_segments=[0-9]+/[0-9]+' AS visited_seen,
	   :'summary' LIKE '%complete=true%' AS complete_seen;

INSERT INTO fb_wal_scan_switch_mark VALUES (clock_timestamp());

DO $$
DECLARE
	i integer;
BEGIN
	FOR i IN 1..6 LOOP
		INSERT INTO fb_wal_scan_noise (payload)
		SELECT repeat(md5((i * 100000 + g)::text), 128)
		FROM generate_series(1, 256) AS g;
		PERFORM pg_switch_wal();
	END LOOP;
END;
$$;

BEGIN;
INSERT INTO fb_wal_scan_target VALUES (3, repeat('switched', 128));
COMMIT;

WITH scans AS (
	SELECT 'off' AS mode,
		   fb_scan_wal_debug('fb_wal_scan_target'::regclass,
							(SELECT target_ts FROM fb_wal_scan_switch_mark)) AS summary
	FROM (SELECT set_config('pg_flashback.parallel_segment_scan', 'off', false)) AS _
	UNION ALL
	SELECT 'on' AS mode,
		   fb_scan_wal_debug('fb_wal_scan_target'::regclass,
							(SELECT target_ts FROM fb_wal_scan_switch_mark)) AS summary
	FROM (SELECT set_config('pg_flashback.parallel_segment_scan', 'on', false)) AS _
),
parsed AS (
	SELECT mode,
		   ((regexp_match(summary, 'visited_segments=([0-9]+)/([0-9]+)'))[1])::int AS visited,
		   ((regexp_match(summary, 'visited_segments=([0-9]+)/([0-9]+)'))[2])::int AS total
	FROM scans
)
SELECT (SELECT visited FROM parsed WHERE mode = 'on') <
	   (SELECT visited FROM parsed WHERE mode = 'off') AS on_skips_segments,
	   (SELECT total FROM parsed WHERE mode = 'on') =
	   (SELECT total FROM parsed WHERE mode = 'off') AS same_total;
