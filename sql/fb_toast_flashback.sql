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

CREATE TABLE fb_toast_flashback_target (
	id integer PRIMARY KEY,
	payload text,
	payload_keep text
);

ALTER TABLE fb_toast_flashback_target
	ALTER COLUMN payload SET STORAGE EXTERNAL;
ALTER TABLE fb_toast_flashback_target
	ALTER COLUMN payload_keep SET STORAGE EXTERNAL;

CHECKPOINT;

INSERT INTO fb_toast_flashback_target
VALUES (1, repeat('A', 12000), repeat('K', 12000));

CREATE TABLE fb_toast_flashback_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_toast_flashback_mark VALUES (clock_timestamp());

UPDATE fb_toast_flashback_target
SET payload = repeat('B', 12000)
WHERE id = 1;

SELECT id,
	   left(payload, 4) AS prefix,
	   left(payload_keep, 4) AS keep_prefix,
	   length(payload) AS payload_len,
	   length(payload_keep) AS keep_len
FROM pg_flashback(
	NULL::public.fb_toast_flashback_target,
	(SELECT target_ts::text FROM fb_toast_flashback_mark)
)
ORDER BY 1;
