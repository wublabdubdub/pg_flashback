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

CREATE TABLE pg_flashback_target (
	id integer PRIMARY KEY,
	note text,
	amount integer
);

CHECKPOINT;

INSERT INTO pg_flashback_target VALUES
	(1, 'baseline-1', 10),
	(2, 'baseline-2', 20);

CREATE TABLE pg_flashback_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO pg_flashback_mark VALUES (clock_timestamp());

INSERT INTO pg_flashback_target VALUES (3, 'inserted-later', 30);
UPDATE pg_flashback_target
SET note = 'updated-later', amount = 99
WHERE id = 1;
DELETE FROM pg_flashback_target WHERE id = 2;

SET client_min_messages = warning;

SELECT *
FROM pg_flashback(
	NULL::public.pg_flashback_target,
	(SELECT target_ts::text FROM pg_flashback_mark)
)
ORDER BY id;

SELECT count(*) AS rerun_count,
	   sum(amount)::bigint AS rerun_sum
FROM pg_flashback(
	NULL::public.pg_flashback_target,
	(SELECT target_ts::text FROM pg_flashback_mark)
);
