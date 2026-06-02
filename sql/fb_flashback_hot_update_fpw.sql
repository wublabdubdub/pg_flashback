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

CREATE TABLE fb_flashback_hot_update_fpw_target (
	id integer PRIMARY KEY,
	pad text
) WITH (fillfactor = 10);

INSERT INTO fb_flashback_hot_update_fpw_target
VALUES (1, repeat('a', 800) || 'X' || repeat('b', 800));

VACUUM FREEZE fb_flashback_hot_update_fpw_target;

CHECKPOINT;

CREATE TABLE fb_flashback_hot_update_fpw_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_hot_update_fpw_mark VALUES (clock_timestamp());

UPDATE fb_flashback_hot_update_fpw_target
SET pad = repeat('a', 800) || 'Y' || repeat('b', 800)
WHERE id = 1;

SELECT id,
	   length(pad) AS pad_len,
	   substr(pad, 1, 4) AS prefix4,
	   substr(pad, 801, 3) AS middle3,
	   right(pad, 4) AS suffix4
FROM pg_flashback(
	NULL::public.fb_flashback_hot_update_fpw_target,
	(SELECT target_ts::text FROM fb_flashback_hot_update_fpw_mark)
)
ORDER BY 1;
