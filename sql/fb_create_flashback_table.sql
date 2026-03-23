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

CREATE TABLE fb_create_flashback_table_target (
	id integer PRIMARY KEY,
	note text,
	amount integer
);

CHECKPOINT;

INSERT INTO fb_create_flashback_table_target VALUES
	(1, 'baseline-1', 10),
	(2, 'baseline-2', 20);

CREATE TABLE fb_create_flashback_table_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_create_flashback_table_mark VALUES (clock_timestamp());

INSERT INTO fb_create_flashback_table_target VALUES (3, 'inserted-later', 30);
UPDATE fb_create_flashback_table_target
SET note = 'updated-later', amount = 99
WHERE id = 1;
DELETE FROM fb_create_flashback_table_target WHERE id = 2;

SET client_min_messages = warning;

SELECT fb_create_flashback_table(
	'fb_created',
	'fb_create_flashback_table_target',
	(SELECT target_ts::text FROM fb_create_flashback_table_mark)
);

SELECT fb_wal_window_debug(
	(SELECT target_ts FROM fb_create_flashback_table_mark)
) ~ '^current_lsn=[0-9A-F]+/[0-9A-F]{8} current_wal=[0-9A-F]{24} start_lsn=[0-9A-F]+/[0-9A-F]{8} start_wal=[0-9A-F]{24} end_lsn=[0-9A-F]+/[0-9A-F]{8} end_wal=[0-9A-F]{24}$';

SELECT * FROM fb_created;

SELECT fb_create_flashback_table(
	'fb_created',
	'fb_create_flashback_table_target',
	(SELECT target_ts::text FROM fb_create_flashback_table_mark)
);
