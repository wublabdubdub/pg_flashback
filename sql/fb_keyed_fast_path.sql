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

DROP TABLE IF EXISTS fb_keyed_fast_path_mark;
DROP TABLE IF EXISTS fb_keyed_fast_path_target;

CREATE TABLE fb_keyed_fast_path_target (
	id bigint PRIMARY KEY,
	payload text NOT NULL,
	amount integer NOT NULL
);

CHECKPOINT;

INSERT INTO fb_keyed_fast_path_target VALUES
	(1, 'one', 10),
	(2, 'two', 20),
	(3, 'three', 30),
	(4, 'four', 40),
	(5, 'five', 50),
	(6, 'six', 60);

CREATE TABLE fb_keyed_fast_path_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_keyed_fast_path_mark VALUES (clock_timestamp());

UPDATE fb_keyed_fast_path_target
SET payload = 'two-new', amount = 200
WHERE id = 2;

DELETE FROM fb_keyed_fast_path_target
WHERE id = 4;

INSERT INTO fb_keyed_fast_path_target VALUES
	(7, 'seven', 70);

EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id = 2;

SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id IN (2, 4, 7)
ORDER BY id;

SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id IN (2, 4, 7)
ORDER BY id;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id BETWEEN 2 AND 4
ORDER BY id;

SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id BETWEEN 2 AND 4
ORDER BY id;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id > 2 AND id <= 5
ORDER BY id DESC
LIMIT 2;

SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id > 2 AND id <= 5
ORDER BY id DESC
LIMIT 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
	   length(payload) AS payload_len,
	   amount
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE 2 < id AND id < 6
ORDER BY id;

SELECT id,
	   length(payload) AS payload_len,
	   amount
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE 2 < id AND id < 6
ORDER BY id;

SELECT id,
	   length(payload) AS payload_len,
	   amount
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
WHERE id IN (2, 4)
ORDER BY id;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
ORDER BY id DESC
LIMIT 2;

SELECT *
FROM pg_flashback(
	NULL::public.fb_keyed_fast_path_target,
	(SELECT target_ts::text FROM fb_keyed_fast_path_mark)
)
ORDER BY id DESC
LIMIT 2;
