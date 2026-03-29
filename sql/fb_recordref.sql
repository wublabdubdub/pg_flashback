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
	IF to_regprocedure('fb_recordref_debug(regclass,timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_debug(regclass, timestamptz)';
	END IF;
END;
$$;

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_debug'
LANGUAGE C
STRICT;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_recordref_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_target VALUES (0, 10);
UPDATE fb_recordref_target SET payload = 11 WHERE id = 0;

CREATE TABLE fb_recordref_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

CHECKPOINT;

INSERT INTO fb_recordref_target VALUES (1, 20);
UPDATE fb_recordref_target SET payload = 21 WHERE id = 1;
DELETE FROM fb_recordref_target WHERE id = 0;

BEGIN;
INSERT INTO fb_recordref_target VALUES (2, 30);
ROLLBACK;

SET pg_flashback.parallel_workers = 0;

SELECT fb_recordref_debug(
	'fb_recordref_target'::regclass,
	(SELECT target_ts FROM fb_recordref_mark)
) AS serial_summary
\gset

SET pg_flashback.parallel_workers = 4;

SELECT fb_recordref_debug(
	'fb_recordref_target'::regclass,
	(SELECT target_ts FROM fb_recordref_mark)
) AS parallel_summary
\gset

SELECT :'serial_summary' LIKE '%parallel=off%' AS parallel_off,
	   :'serial_summary' LIKE '%prefilter=on%' AS prefilter_on,
	   :'serial_summary' LIKE '%commits=%' AS serial_commits_present,
	   :'serial_summary' LIKE '%aborts=%' AS serial_aborts_present;

SELECT :'parallel_summary' LIKE '%parallel=on%' AS parallel_on,
	   :'parallel_summary' LIKE '%prefilter=on%' AS parallel_prefilter_on,
	   :'parallel_summary' LIKE '%commits=%' AS parallel_commits_present,
	   :'parallel_summary' LIKE '%aborts=%' AS parallel_aborts_present;

SELECT regexp_replace(:'serial_summary', ' parallel=.*$', '') =
	   regexp_replace(:'parallel_summary', ' parallel=.*$', '') AS serial_parallel_contract_equal;

CREATE TABLE fb_recordref_unsafe (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_unsafe VALUES (1, 10);

CREATE TABLE fb_recordref_unsafe_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_unsafe_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

TRUNCATE fb_recordref_unsafe;

SET pg_flashback.parallel_workers = 0;

SELECT fb_recordref_debug(
	'fb_recordref_unsafe'::regclass,
	(SELECT target_ts FROM fb_recordref_unsafe_mark)
) AS serial_unsafe_summary
\gset

SET pg_flashback.parallel_workers = 4;

SELECT fb_recordref_debug(
	'fb_recordref_unsafe'::regclass,
	(SELECT target_ts FROM fb_recordref_unsafe_mark)
) AS parallel_unsafe_summary
\gset

SELECT :'serial_unsafe_summary' LIKE '%unsafe=true%' AS serial_unsafe_true,
	   :'serial_unsafe_summary' LIKE '%reason=%' AS serial_reason_present;

SELECT :'parallel_unsafe_summary' LIKE '%unsafe=true%' AS parallel_unsafe_true,
	   :'parallel_unsafe_summary' LIKE '%reason=%' AS parallel_reason_present;

SELECT regexp_replace(:'serial_unsafe_summary', ' parallel=.*$', '') =
	   regexp_replace(:'parallel_unsafe_summary', ' parallel=.*$', '') AS unsafe_serial_parallel_contract_equal;
