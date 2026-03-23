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

CREATE TABLE fb_flashback_materialize_target (
	id integer PRIMARY KEY,
	note text,
	amount integer
);

CHECKPOINT;

INSERT INTO fb_flashback_materialize_target VALUES
	(1, 'baseline-1', 10),
	(2, 'baseline-2', 20);

CREATE TABLE fb_flashback_materialize_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_materialize_mark VALUES (clock_timestamp());

INSERT INTO fb_flashback_materialize_target VALUES (3, 'inserted-later', 30);
UPDATE fb_flashback_materialize_target
SET note = 'updated-later', amount = 99
WHERE id = 1;
DELETE FROM fb_flashback_materialize_target WHERE id = 2;

SELECT fb_flashback_materialize(
	'fb_flashback_materialize_target'::regclass,
	(SELECT target_ts FROM fb_flashback_materialize_mark)
);

TABLE fb_flashback_result;
