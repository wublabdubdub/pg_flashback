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

CREATE TABLE fb_flashback_bag_target (
	id integer,
	payload text
);

CHECKPOINT;

INSERT INTO fb_flashback_bag_target VALUES (1, 'seed');
INSERT INTO fb_flashback_bag_target VALUES (1, 'seed');
INSERT INTO fb_flashback_bag_target VALUES (2, 'keep');

CREATE TABLE fb_flashback_bag_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_flashback_bag_mark VALUES (clock_timestamp());

INSERT INTO fb_flashback_bag_target VALUES (1, 'seed');
UPDATE fb_flashback_bag_target SET payload = 'changed' WHERE id = 2;
DELETE FROM fb_flashback_bag_target
WHERE ctid = (
	SELECT ctid
	FROM fb_flashback_bag_target
	WHERE id = 1 AND payload = 'seed'
	LIMIT 1
);

SELECT pg_flashback(
	'fb_flashback_bag_result',
	'fb_flashback_bag_target',
	(SELECT target_ts::text FROM fb_flashback_bag_mark)
);

SELECT *
FROM fb_flashback_bag_result
ORDER BY 1, 2;
