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

CREATE FUNCTION fb_progress_debug_set_clock(bigint[])
RETURNS void
AS '$libdir/pg_flashback', 'fb_progress_debug_set_clock'
LANGUAGE C STRICT;

CREATE FUNCTION fb_progress_debug_reset_clock()
RETURNS void
AS '$libdir/pg_flashback', 'fb_progress_debug_reset_clock'
LANGUAGE C;

CREATE TABLE fb_progress_target (
	id integer PRIMARY KEY,
	note text
);

CHECKPOINT;

INSERT INTO fb_progress_target VALUES
	(1, 'baseline'),
	(2, 'baseline'),
	(3, 'baseline'),
	(4, 'baseline'),
	(5, 'baseline');

CREATE TABLE fb_progress_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_progress_mark VALUES (clock_timestamp());

UPDATE fb_progress_target
SET note = 'after-target'
WHERE id = 1;

SELECT fb_progress_debug_set_clock(ARRAY[
	0,
	1000,
	2000,
	3000,
	4000,
	5000,
	6000,
	7000,
	8000,
	9000,
	10000,
	11000,
	12000,
	13000,
	14000,
	15000,
	16000,
	17000,
	18000,
	19000,
	20000,
	21000,
	22000,
	23000
]::bigint[]);

SELECT *
FROM pg_flashback(
	NULL::public.fb_progress_target,
	(SELECT target_ts::text FROM fb_progress_mark)
)
ORDER BY id;

SELECT fb_progress_debug_reset_clock();

SET pg_flashback.show_progress = off;

SELECT *
FROM pg_flashback(
	NULL::public.fb_progress_target,
	(SELECT target_ts::text FROM fb_progress_mark)
)
ORDER BY id;
