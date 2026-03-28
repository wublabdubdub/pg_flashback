DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;
SET pg_flashback.spill_mode = 'disk';

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

SET pg_flashback.memory_limit = 512;

CREATE TABLE fb_spill_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_spill_target VALUES (1, 'baseline');

CREATE TABLE fb_spill_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_spill_mark VALUES (clock_timestamp());

DO $$
DECLARE
	i integer;
BEGIN
	FOR i IN 1..5000 LOOP
		UPDATE fb_spill_target
		SET payload = format('after-%s', i)
		WHERE id = 1;
	END LOOP;
END;
$$;

SET client_min_messages = warning;

WITH flashback AS (
	SELECT *
	FROM pg_flashback(
		NULL::public.fb_spill_target,
		(SELECT target_ts::text FROM fb_spill_mark)
	)
)
SELECT count(*) AS result_count,
	   min(id) AS min_id,
	   max(id) AS max_id,
	   max(payload) = 'baseline' AS restored_baseline
FROM flashback;

RESET client_min_messages;
