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

SELECT pg_flashback(
	'fb_progress_result_on',
	'fb_progress_target',
	(SELECT target_ts::text FROM fb_progress_mark)
);

SELECT * FROM fb_progress_result_on ORDER BY id;

SET pg_flashback.show_progress = off;

SELECT pg_flashback(
	'fb_progress_result_off',
	'fb_progress_target',
	(SELECT target_ts::text FROM fb_progress_mark)
);

SELECT * FROM fb_progress_result_off ORDER BY id;
