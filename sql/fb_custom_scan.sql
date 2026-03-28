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

SET pg_flashback.show_progress = off;

DROP TABLE IF EXISTS fb_custom_scan_probe;
DROP TABLE IF EXISTS fb_custom_scan_mark;
DROP TABLE IF EXISTS fb_custom_scan_target;

CREATE TABLE fb_custom_scan_target (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CHECKPOINT;

INSERT INTO fb_custom_scan_target
SELECT g,
	   (
		   SELECT string_agg(md5((g * 1000 + s)::text), '' ORDER BY s)
		   FROM generate_series(1, 32) AS s
	   )
FROM generate_series(1, 20000) AS g;

CREATE TABLE fb_custom_scan_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_custom_scan_mark VALUES (clock_timestamp());

UPDATE fb_custom_scan_target
SET payload = repeat('q', 1024)
WHERE (id % 2) = 0;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*)
FROM pg_flashback(
	NULL::public.fb_custom_scan_target,
	(SELECT target_ts::text FROM fb_custom_scan_mark)
);

CREATE TABLE fb_custom_scan_probe (
	result_count bigint NOT NULL,
	temp_bytes_delta bigint NOT NULL
);

DO $$
DECLARE
	before_bytes bigint;
	after_bytes bigint;
	result_count bigint;
BEGIN
	PERFORM pg_stat_force_next_flush();
	PERFORM pg_stat_clear_snapshot();

	SELECT temp_bytes
	INTO before_bytes
	FROM pg_stat_database
	WHERE datname = current_database();

	PERFORM set_config('work_mem', '64kB', true);

	SELECT count(*)
	INTO result_count
	FROM pg_flashback(
		NULL::public.fb_custom_scan_target,
		(SELECT target_ts::text FROM fb_custom_scan_mark)
	);

	PERFORM pg_stat_force_next_flush();
	PERFORM pg_stat_clear_snapshot();

	SELECT temp_bytes
	INTO after_bytes
	FROM pg_stat_database
	WHERE datname = current_database();

	INSERT INTO fb_custom_scan_probe
	VALUES (result_count, GREATEST(after_bytes - before_bytes, 0));
END;
$$;

SELECT result_count = 20000 AS count_ok,
	   temp_bytes_delta = 0 AS no_temp_spill
FROM fb_custom_scan_probe;
