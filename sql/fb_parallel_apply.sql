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

SET pg_flashback.show_progress = on;
SET pg_flashback.parallel_apply_workers = 2;

CREATE TABLE fb_parallel_target (
	id integer PRIMARY KEY,
	note text
);

CHECKPOINT;

INSERT INTO fb_parallel_target VALUES
	(1, 'baseline'),
	(2, 'baseline'),
	(3, 'baseline'),
	(4, 'baseline'),
	(5, 'baseline');

CREATE TABLE fb_parallel_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_parallel_mark VALUES (clock_timestamp());

UPDATE fb_parallel_target
SET note = 'after-target'
WHERE id = 1;

DELETE FROM fb_parallel_target
WHERE id = 2;

INSERT INTO fb_parallel_target VALUES (6, 'after-target');

BEGIN;

SELECT pg_flashback(
	'fb_parallel_result',
	'fb_parallel_target',
	(SELECT target_ts::text FROM fb_parallel_mark)
);

ROLLBACK;

SELECT to_regclass('fb_parallel_result');

SELECT relpersistence
FROM pg_class
WHERE oid = 'fb_parallel_result'::regclass;

SELECT * FROM fb_parallel_result ORDER BY id;

SELECT 1 AS done;

SET client_min_messages = notice;
