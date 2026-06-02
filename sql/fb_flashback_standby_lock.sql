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
	EXECUTE format('SET pg_flashback.archive_dir = %L',
				   current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_flashback_standby_lock_target (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CREATE TABLE fb_flashback_standby_lock_mark (
	target_ts timestamptz NOT NULL
);

CREATE TABLE fb_flashback_standby_lock_truth (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

CREATE TABLE fb_flashback_standby_lock_result (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

INSERT INTO fb_flashback_standby_lock_target
SELECT g,
	   repeat(md5(g::text), 32)
FROM generate_series(1, 200) AS g;

INSERT INTO fb_flashback_standby_lock_mark
VALUES (clock_timestamp());

INSERT INTO fb_flashback_standby_lock_truth
SELECT *
FROM fb_flashback_standby_lock_target;

BEGIN;
LOCK TABLE fb_flashback_standby_lock_target IN ACCESS EXCLUSIVE MODE;
COMMIT;

INSERT INTO fb_flashback_standby_lock_result
SELECT *
FROM pg_flashback(
	NULL::public.fb_flashback_standby_lock_target,
	(SELECT target_ts::text
	 FROM fb_flashback_standby_lock_mark)
);

SELECT (SELECT count(*) FROM fb_flashback_standby_lock_truth) AS truth_count,
	   (SELECT count(*) FROM fb_flashback_standby_lock_result) AS result_count,
	   (SELECT count(*)
		FROM (
			(SELECT * FROM fb_flashback_standby_lock_truth
			 EXCEPT ALL
			 SELECT * FROM fb_flashback_standby_lock_result)
			UNION ALL
			(SELECT * FROM fb_flashback_standby_lock_result
			 EXCEPT ALL
			 SELECT * FROM fb_flashback_standby_lock_truth)
		) AS diff_rows) AS diff_count;
