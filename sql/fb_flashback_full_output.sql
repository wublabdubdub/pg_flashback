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

CREATE TABLE fb_flashback_full_output_src (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_flashback_full_output_src VALUES
	(1, 'base-1'),
	(2, 'base-2');

CREATE TABLE fb_flashback_full_output_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_full_output_mark VALUES (clock_timestamp());

INSERT INTO fb_flashback_full_output_src VALUES (3, 'later-3');
UPDATE fb_flashback_full_output_src
SET payload = 'updated-1'
WHERE id = 1;
DELETE FROM fb_flashback_full_output_src
WHERE id = 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(
	NULL::public.fb_flashback_full_output_src,
	(SELECT target_ts::text FROM fb_flashback_full_output_mark)
);

EXPLAIN (VERBOSE, COSTS OFF)
CREATE TABLE fb_flashback_full_output_out AS
SELECT *
FROM pg_flashback(
	NULL::public.fb_flashback_full_output_src,
	(SELECT target_ts::text FROM fb_flashback_full_output_mark)
)
WITH NO DATA;

SELECT *
FROM pg_flashback(
	NULL::public.fb_flashback_full_output_src,
	(SELECT target_ts::text FROM fb_flashback_full_output_mark)
)
ORDER BY id;

CREATE TABLE fb_flashback_full_output_out AS
SELECT *
FROM pg_flashback(
	NULL::public.fb_flashback_full_output_src,
	(SELECT target_ts::text FROM fb_flashback_full_output_mark)
);

SELECT count(*) AS ctas_count,
	   min(id) AS ctas_min_id,
	   max(id) AS ctas_max_id
FROM fb_flashback_full_output_out;
