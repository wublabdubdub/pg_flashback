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

SET pg_flashback.show_progress = off;

DROP TABLE IF EXISTS fb_wal_parallel_payload_target;
DROP TABLE IF EXISTS fb_wal_parallel_payload_noise;
DROP TABLE IF EXISTS fb_wal_parallel_payload_mark;
DROP TABLE IF EXISTS fb_wal_parallel_payload_dense;
DROP TABLE IF EXISTS fb_wal_parallel_payload_dense_mark;

CREATE TABLE fb_wal_parallel_payload_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE TABLE fb_wal_parallel_payload_noise (
	id bigserial PRIMARY KEY,
	payload text
);

CREATE TABLE fb_wal_parallel_payload_dense (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_wal_parallel_payload_target VALUES (1, repeat('a', 1000));

INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n1');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n2');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n3');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n4');
SELECT pg_switch_wal() IS NOT NULL AS switched;

CREATE TABLE fb_wal_parallel_payload_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_wal_parallel_payload_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

UPDATE fb_wal_parallel_payload_target
SET payload = repeat('b', 1000)
WHERE id = 1;

INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n5');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n6');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n7');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n8');
SELECT pg_switch_wal() IS NOT NULL AS switched;

INSERT INTO fb_wal_parallel_payload_target VALUES (2, repeat('c', 1000));

INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n9');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n10');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n11');
SELECT pg_switch_wal() IS NOT NULL AS switched;
INSERT INTO fb_wal_parallel_payload_noise(payload) VALUES ('n12');
SELECT pg_switch_wal() IS NOT NULL AS switched;

DELETE FROM fb_wal_parallel_payload_target WHERE id = 1;

SET pg_flashback.parallel_workers = 0;

SELECT fb_recordref_debug(
	'fb_wal_parallel_payload_target'::regclass,
	(SELECT target_ts FROM fb_wal_parallel_payload_mark)
) AS serial_summary
\gset

SET pg_flashback.parallel_workers = 4;

SELECT GREATEST(
		   current_setting('max_worker_processes')::int
		   - COALESCE(NULLIF(current_setting('io_workers', true), ''), '0')::int
		   - 1
		   - CASE
				 WHEN position('pg_flashback' in current_setting('shared_preload_libraries')) > 0
					  AND current_setting('pg_flashback.summary_service') = 'on'
				 THEN 1 + LEAST(
							current_setting('pg_flashback.summary_workers')::int,
							GREATEST(current_setting('max_worker_processes')::int - 5, 1)
						)
				 ELSE 0
			 END
		   - 1,
		   1
	   ) AS wal_parallel_worker_budget
\gset

SELECT fb_recordref_debug(
	'fb_wal_parallel_payload_target'::regclass,
	(SELECT target_ts FROM fb_wal_parallel_payload_mark)
) AS parallel_summary
\gset

SELECT regexp_replace(:'serial_summary', ' parallel=.*$', '') =
	   regexp_replace(:'parallel_summary', ' parallel=.*$', '') AS same_core_summary;

SELECT :'serial_summary' LIKE '%parallel=off%' AS serial_parallel_off,
	   :'serial_summary' LIKE '%prefilter=on%' AS serial_prefilter_on,
	   :'serial_summary' LIKE '%payload_windows=%' AS serial_payload_windows,
	   :'serial_summary' LIKE '%payload_parallel_workers=0%' AS serial_payload_workers_zero;

SELECT :'parallel_summary' LIKE '%parallel=on%' AS parallel_parallel_on,
	   :'parallel_summary' LIKE '%prefilter=on%' AS parallel_prefilter_on,
	   :'parallel_summary' LIKE '%payload_windows=%' AS parallel_payload_windows,
	   CASE
		   WHEN :'wal_parallel_worker_budget'::int > 1
		   THEN substring(:'parallel_summary' FROM 'payload_parallel_workers=([0-9]+)')::int > 0
		   ELSE substring(:'parallel_summary' FROM 'payload_parallel_workers=([0-9]+)')::int = 0
	   END AS parallel_payload_workers_match_budget;

CREATE TEMP TABLE fb_wal_parallel_payload_serial_result AS
SELECT *
FROM pg_flashback(
	NULL::fb_wal_parallel_payload_target,
	(SELECT target_ts FROM fb_wal_parallel_payload_mark)::text
);

SET pg_flashback.parallel_workers = 4;

CREATE TEMP TABLE fb_wal_parallel_payload_parallel_result AS
SELECT *
FROM pg_flashback(
	NULL::fb_wal_parallel_payload_target,
	(SELECT target_ts FROM fb_wal_parallel_payload_mark)::text
);

SELECT count(*) AS diff_count
FROM (
	(SELECT * FROM fb_wal_parallel_payload_serial_result
	 EXCEPT ALL
	 SELECT * FROM fb_wal_parallel_payload_parallel_result)
	UNION ALL
	(SELECT * FROM fb_wal_parallel_payload_parallel_result
	 EXCEPT ALL
	 SELECT * FROM fb_wal_parallel_payload_serial_result)
) diff;

SELECT count(*) AS restored_rows,
	   min(id) AS min_id,
	   max(id) AS max_id,
	   min(left(payload, 1)) AS first_payload_char,
	   max(length(payload)) AS payload_len
FROM fb_wal_parallel_payload_parallel_result;

CHECKPOINT;

INSERT INTO fb_wal_parallel_payload_dense VALUES (0, repeat('z', 2500));

CREATE TABLE fb_wal_parallel_payload_dense_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_wal_parallel_payload_dense_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

INSERT INTO fb_wal_parallel_payload_dense
SELECT g,
	   array_to_string(ARRAY(
		   SELECT md5((g * 1000 + s)::text)
		   FROM generate_series(1, 80) AS s
	   ), '')
FROM generate_series(1, 30000) AS g;

SET pg_flashback.parallel_workers = 4;

SELECT fb_recordref_debug(
	'fb_wal_parallel_payload_dense'::regclass,
	(SELECT target_ts FROM fb_wal_parallel_payload_dense_mark)
) AS dense_parallel_summary
\gset

SELECT substring(:'dense_parallel_summary' FROM 'payload_windows=([0-9]+)')::int >= 1
	   AS dense_payload_windows_tracked,
	   CASE
		   WHEN :'wal_parallel_worker_budget'::int > 1
		   THEN substring(:'dense_parallel_summary' FROM 'payload_parallel_workers=([0-9]+)')::int > 0
		   ELSE substring(:'dense_parallel_summary' FROM 'payload_parallel_workers=([0-9]+)')::int = 0
	   END AS dense_payload_workers_match_budget;
SELECT :'dense_parallel_summary' LIKE '%payload_sparse_reader_resets=%'
	   AS tracks_sparse_reader_resets,
	   :'dense_parallel_summary' LIKE '%payload_sparse_reader_reuses=%'
	   AS tracks_sparse_reader_reuses;
