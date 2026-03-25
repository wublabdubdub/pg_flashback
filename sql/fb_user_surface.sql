DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;

SELECT to_regprocedure('fb_runtime_dir_debug()') IS NULL AS no_runtime_debug,
	   to_regprocedure('fb_wal_source_debug()') IS NULL AS no_source_debug,
	   to_regprocedure('fb_scan_wal_debug(regclass,timestamptz)') IS NULL AS no_scan_debug,
	   to_regprocedure('fb_recordref_debug(regclass,timestamptz)') IS NULL AS no_recordref_debug,
	   to_regprocedure('fb_replay_debug(regclass,timestamptz)') IS NULL AS no_replay_debug,
	   to_regprocedure('fb_wal_window_debug(timestamptz)') IS NULL AS no_window_debug,
	   to_regprocedure('fb_decode_insert_debug(regclass,timestamptz)') IS NULL AS no_decode_debug,
	   to_regprocedure('fb_flashback_materialize(regclass,timestamptz,text)') IS NULL AS no_materialize_helper,
	   to_regprocedure('fb_internal_flashback(regclass,timestamptz)') IS NULL AS no_internal_srf;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_user_surface_target (
	id integer PRIMARY KEY,
	payload text,
	score integer
);

INSERT INTO fb_user_surface_target VALUES
	(1, 'seed-1', 10),
	(2, 'seed-2', 20);

CREATE TABLE fb_user_surface_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_user_surface_mark VALUES (clock_timestamp());

INSERT INTO fb_user_surface_target VALUES (3, 'inserted-later', 30);
UPDATE fb_user_surface_target
SET payload = 'updated-later',
	score = 99
WHERE id = 1;
DELETE FROM fb_user_surface_target WHERE id = 2;

SELECT pg_flashback(
	'fb_user_surface_result',
	'fb_user_surface_target',
	(SELECT target_ts::text FROM fb_user_surface_mark)
);

SELECT *
FROM fb_user_surface_result
ORDER BY id;
