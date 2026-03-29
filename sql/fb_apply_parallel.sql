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
	IF to_regprocedure('fb_apply_debug(regclass,timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_apply_debug(regclass, timestamptz)';
	END IF;
END;
$$;

CREATE FUNCTION fb_apply_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_apply_debug'
LANGUAGE C
STRICT;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

SET pg_flashback.show_progress = off;
SET max_parallel_workers_per_gather = 4;

DROP TABLE IF EXISTS fb_apply_parallel_target;
DROP TABLE IF EXISTS fb_apply_parallel_mark;

CREATE TABLE fb_apply_parallel_target (
	id integer PRIMARY KEY,
	payload text
);

INSERT INTO fb_apply_parallel_target
SELECT g,
	   array_to_string(ARRAY(
		   SELECT md5((g * 1000 + s)::text)
		   FROM generate_series(1, 96) AS s
	   ), '')
FROM generate_series(1, 12000) AS g;

CHECKPOINT;

CREATE TABLE fb_apply_parallel_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_apply_parallel_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

UPDATE fb_apply_parallel_target
SET payload = array_to_string(ARRAY(
		SELECT md5((id * 10000 + s)::text)
		FROM generate_series(1, 96) AS s
	), '')
WHERE id <= 3000;

DELETE FROM fb_apply_parallel_target
WHERE id > 3000 AND id <= 6000;

INSERT INTO fb_apply_parallel_target
SELECT g,
	   array_to_string(ARRAY(
		   SELECT md5((g * 2000 + s)::text)
		   FROM generate_series(1, 96) AS s
	   ), '')
FROM generate_series(12001, 15000) AS g;

VACUUM ANALYZE fb_apply_parallel_target;

SET pg_flashback.parallel_workers = 0;

CREATE TEMP TABLE fb_apply_parallel_serial_result AS
SELECT *
FROM pg_flashback(
	NULL::fb_apply_parallel_target,
	(SELECT target_ts FROM fb_apply_parallel_mark)::text
);

SET pg_flashback.parallel_workers = 4;

SELECT fb_apply_debug(
	'fb_apply_parallel_target'::regclass,
	(SELECT target_ts FROM fb_apply_parallel_mark)
) AS apply_parallel_summary
\gset

SELECT :'apply_parallel_summary' LIKE '%mode=keyed%' AS apply_mode_keyed,
	   :'apply_parallel_summary' LIKE '%apply_parallel=on%' AS apply_parallel_on,
	   :'apply_parallel_summary' LIKE '%parallel_logs=4%' AS apply_parallel_logs_four,
	   :'apply_parallel_summary' LIKE '%fast_path=off%' AS apply_fast_path_off;

CREATE TEMP TABLE fb_apply_parallel_parallel_result AS
SELECT *
FROM pg_flashback(
	NULL::fb_apply_parallel_target,
	(SELECT target_ts FROM fb_apply_parallel_mark)::text
);

SELECT count(*) AS diff_count
FROM (
	(SELECT * FROM fb_apply_parallel_serial_result
	 EXCEPT ALL
	 SELECT * FROM fb_apply_parallel_parallel_result)
	UNION ALL
	(SELECT * FROM fb_apply_parallel_parallel_result
	 EXCEPT ALL
	 SELECT * FROM fb_apply_parallel_serial_result)
) diff;
