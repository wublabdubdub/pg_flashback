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

SELECT fb_recordref_debug(
	'fb_memory_limit_target'::regclass,
	(SELECT target_ts FROM fb_memory_limit_mark)
);
