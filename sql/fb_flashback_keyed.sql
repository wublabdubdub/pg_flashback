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

CREATE TABLE fb_flashback_keyed_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_flashback_keyed_target VALUES (0, 'seed');

CREATE TABLE fb_flashback_keyed_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_keyed_mark VALUES (clock_timestamp());

INSERT INTO fb_flashback_keyed_target VALUES (1, 'alpha');
UPDATE fb_flashback_keyed_target SET payload = 'alpha-updated' WHERE id = 1;
DELETE FROM fb_flashback_keyed_target WHERE id = 0;

SELECT *
FROM pg_flashback(
	NULL::public.fb_flashback_keyed_target,
	(SELECT target_ts::text FROM fb_flashback_keyed_mark)
)
ORDER BY 1, 2;
