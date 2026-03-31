DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

SET pg_flashback.show_progress = off;
SET pg_flashback.parallel_workers = 0;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

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
LANGUAGE C;

DROP TABLE IF EXISTS fb_summary_overlap_toast_target;
DROP TABLE IF EXISTS fb_summary_overlap_toast_mark;
DROP TABLE IF EXISTS fb_summary_overlap_toast_truth;
DROP TABLE IF EXISTS fb_summary_overlap_toast_result;

CREATE TABLE fb_summary_overlap_toast_target (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CREATE TABLE fb_summary_overlap_toast_mark (
	target_ts timestamptz NOT NULL
);

CREATE TABLE fb_summary_overlap_toast_truth (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CREATE TABLE fb_summary_overlap_toast_result (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CHECKPOINT;

INSERT INTO fb_summary_overlap_toast_target
SELECT g,
	   (SELECT string_agg(md5((g * 100000 + s)::text), '')
		FROM generate_series(1, 128) AS s)
FROM generate_series(1, 2000) AS g;

INSERT INTO fb_summary_overlap_toast_mark
VALUES (clock_timestamp());

INSERT INTO fb_summary_overlap_toast_truth
SELECT *
FROM fb_summary_overlap_toast_target;

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

UPDATE fb_summary_overlap_toast_target
SET payload = (SELECT string_agg(md5((id * 200000 + s)::text), '')
			   FROM generate_series(1, 96) AS s)
WHERE id BETWEEN 1 AND 1500;

UPDATE fb_summary_overlap_toast_target
SET payload = repeat('small', 8)
WHERE id BETWEEN 1001 AND 2000;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS built_summary;

SELECT substring(
		   fb_recordref_debug(
			   'fb_summary_overlap_toast_target'::regclass,
			   (SELECT target_ts FROM fb_summary_overlap_toast_mark)
		   )
		   FROM 'summary_span_windows=([0-9]+)')::int > 0
	   AS uses_summary_spans;

INSERT INTO fb_summary_overlap_toast_result
SELECT *
FROM pg_flashback(
	NULL::public.fb_summary_overlap_toast_target,
	(SELECT target_ts::text FROM fb_summary_overlap_toast_mark)
);

SELECT (SELECT count(*) FROM fb_summary_overlap_toast_truth) AS truth_count,
	   (SELECT count(*) FROM fb_summary_overlap_toast_result) AS result_count,
	   (SELECT count(*)
		FROM (
			(SELECT * FROM fb_summary_overlap_toast_truth
			 EXCEPT ALL
			 SELECT * FROM fb_summary_overlap_toast_result)
			UNION ALL
			(SELECT * FROM fb_summary_overlap_toast_result
			 EXCEPT ALL
			 SELECT * FROM fb_summary_overlap_toast_truth)
		) AS diff_rows) AS diff_count;

DROP TABLE fb_summary_overlap_toast_result;
