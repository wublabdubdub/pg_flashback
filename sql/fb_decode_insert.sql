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

CREATE TABLE fb_decode_insert_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_decode_insert_target VALUES (0, 'seed');

CREATE TABLE fb_decode_insert_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_decode_insert_mark VALUES (clock_timestamp());

INSERT INTO fb_decode_insert_target VALUES (1, 'alpha');

SELECT *
FROM fb_decode_insert_debug(
	'fb_decode_insert_target'::regclass,
	(SELECT target_ts FROM fb_decode_insert_mark)
)
ORDER BY 1;
