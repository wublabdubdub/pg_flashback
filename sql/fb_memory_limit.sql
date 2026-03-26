DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

SET pg_flashback.memory_limit_kb = 6;

CREATE TABLE fb_memory_limit_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_memory_limit_target VALUES (0, 'seed');

CREATE TABLE fb_memory_limit_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_memory_limit_mark VALUES (clock_timestamp());

INSERT INTO fb_memory_limit_target VALUES (1, 'alpha');
UPDATE fb_memory_limit_target SET payload = 'alpha-updated' WHERE id = 1;
DELETE FROM fb_memory_limit_target WHERE id = 0;

SELECT *
FROM pg_flashback(
	NULL::public.fb_memory_limit_target,
	(SELECT target_ts::text FROM fb_memory_limit_mark)
);

SET pg_flashback.memory_limit_kb = 2048;

CREATE TABLE fb_memory_limit_apply_target (
	id integer PRIMARY KEY,
	payload text
);

INSERT INTO fb_memory_limit_apply_target
SELECT i, repeat(md5(i::text), 8)
FROM generate_series(1, 4000) AS g(i);

CHECKPOINT;

CREATE TABLE fb_memory_limit_apply_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_memory_limit_apply_mark VALUES (clock_timestamp());

UPDATE fb_memory_limit_apply_target
SET payload = payload || 'x'
WHERE id = 1;

SET pg_flashback.memory_limit_kb = 2048;
SET client_min_messages = warning;

WITH flashback AS (
	SELECT *
	FROM pg_flashback(
		NULL::public.fb_memory_limit_apply_target,
		(SELECT target_ts::text FROM fb_memory_limit_apply_mark)
	)
)
SELECT count(*) AS result_count,
	   min(id) AS min_id,
	   max(id) AS max_id,
	   max(payload) FILTER (WHERE id = 1) = repeat(md5('1'), 8) AS restored_seed
FROM flashback;
