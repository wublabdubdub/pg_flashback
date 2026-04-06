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
SET pg_flashback.parallel_workers = 0;

DO $$
BEGIN
	IF to_regprocedure('fb_summary_build_available_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_build_available_debug()';
	END IF;
	IF to_regprocedure('fb_recordref_debug(regclass, timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_debug(regclass, timestamptz)';
	END IF;
END;
$$;

CREATE FUNCTION fb_summary_build_available_debug()
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_build_available_debug'
LANGUAGE C;

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_debug'
LANGUAGE C
STRICT;

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
	temp_bytes_delta bigint NOT NULL,
	executor_growth_bytes bigint NOT NULL
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
	VALUES (result_count, GREATEST(after_bytes - before_bytes, 0), 0);
END;
$$;

SELECT result_count = 20000 AS count_ok,
	   temp_bytes_delta = 0 AS no_temp_spill
FROM fb_custom_scan_probe;

TRUNCATE fb_custom_scan_probe;

DO $$
DECLARE
	cur refcursor;
	before_bytes bigint;
	after_bytes bigint;
BEGIN
	cur := 'fb_flashback_mem_cur';

	OPEN cur FOR
	SELECT *
	FROM pg_flashback(
		NULL::public.fb_custom_scan_target,
		(SELECT target_ts::text FROM fb_custom_scan_mark)
	);

	MOVE FORWARD 1000 IN cur;

	SELECT COALESCE(MAX(used_bytes), 0)
	INTO before_bytes
	FROM pg_get_backend_memory_contexts()
	WHERE name = 'ExecutorState';

	MOVE FORWARD 8000 IN cur;

	SELECT COALESCE(MAX(used_bytes), 0)
	INTO after_bytes
	FROM pg_get_backend_memory_contexts()
	WHERE name = 'ExecutorState';

	CLOSE cur;

	INSERT INTO fb_custom_scan_probe
	VALUES (0, 0, GREATEST(after_bytes - before_bytes, 0));
END;
$$;

SELECT executor_growth_bytes <= 2097152 AS bounded_executor_growth
FROM fb_custom_scan_probe;

DROP TABLE IF EXISTS fb_custom_scan_summary_target;
DROP TABLE IF EXISTS fb_custom_scan_summary_noise;
DROP TABLE IF EXISTS fb_custom_scan_summary_mark;

CREATE TABLE fb_custom_scan_summary_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE TABLE fb_custom_scan_summary_noise (
	id bigserial PRIMARY KEY,
	payload text
);

ALTER TABLE fb_custom_scan_summary_noise
	ALTER COLUMN payload SET STORAGE EXTERNAL;

CHECKPOINT;

INSERT INTO fb_custom_scan_summary_target
SELECT g,
	   repeat(md5(g::text), 8)
FROM generate_series(1, 5000) AS g;

CREATE TABLE fb_custom_scan_summary_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_custom_scan_summary_mark VALUES (clock_timestamp());

DO $$
DECLARE
	i integer;
BEGIN
	PERFORM pg_sleep(1.1);

	FOR i IN 1..48 LOOP
		IF i <= 15 THEN
			INSERT INTO fb_custom_scan_summary_target
			VALUES (5000 + i, repeat(md5((5000 + i)::text), 8));
		END IF;

		IF i <= 9 THEN
			DELETE FROM fb_custom_scan_summary_target
			WHERE id = i;
		END IF;

		UPDATE fb_custom_scan_summary_target
		SET payload = format('u-%s', i)
		WHERE id = 1000 + i;

		INSERT INTO fb_custom_scan_summary_noise(payload)
		SELECT array_to_string(ARRAY(
			SELECT md5((i * 100000 + s)::text)
			FROM generate_series(1, 768) AS s
		), '');
	END LOOP;
END;
$$;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS built_summary;

SELECT fb_recordref_debug(
	'fb_custom_scan_summary_target'::regclass,
	(SELECT target_ts FROM fb_custom_scan_summary_mark)
) AS fb_custom_scan_summary_debug
\gset

SELECT substring(:'fb_custom_scan_summary_debug' FROM 'summary_span_windows=([0-9]+)')::int > 0
	   AS uses_summary_spans,
	   substring(:'fb_custom_scan_summary_debug' FROM 'summary_xid_hits=([0-9]+)')::int > 0
	   AS uses_summary_xids;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*)
FROM pg_flashback(
	NULL::public.fb_custom_scan_summary_target,
	(SELECT target_ts::text FROM fb_custom_scan_summary_mark)
);

CREATE TEMP TABLE fb_custom_scan_summary_truth AS
SELECT *
FROM pg_flashback(
	NULL::public.fb_custom_scan_summary_target,
	(SELECT target_ts::text FROM fb_custom_scan_summary_mark)
);

SELECT (SELECT count(*)
		FROM pg_flashback(
			NULL::public.fb_custom_scan_summary_target,
			(SELECT target_ts::text FROM fb_custom_scan_summary_mark)
		)) = (SELECT count(*) FROM fb_custom_scan_summary_truth) AS count_matches_truth,
	   (SELECT count(*) FROM fb_custom_scan_summary_truth) = 5000 AS truth_count_ok,
	   (SELECT count(*) FROM fb_custom_scan_summary_target) <>
	   (SELECT count(*) FROM fb_custom_scan_summary_truth) AS current_differs_from_truth;
