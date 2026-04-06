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
	IF to_regprocedure('fb_runtime_create_test_artifacts_debug(integer)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_runtime_create_test_artifacts_debug(integer)';
	END IF;
	IF to_regprocedure('fb_runtime_remove_test_artifacts_debug(integer)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_runtime_remove_test_artifacts_debug(integer)';
	END IF;
END;
$$;

CREATE FUNCTION fb_runtime_create_test_artifacts_debug(integer)
RETURNS boolean
AS '$libdir/pg_flashback', 'fb_runtime_create_test_artifacts_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_runtime_remove_test_artifacts_debug(integer)
RETURNS boolean
AS '$libdir/pg_flashback', 'fb_runtime_remove_test_artifacts_debug'
LANGUAGE C
STRICT;

SET pg_flashback.show_progress = off;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

DROP TABLE IF EXISTS fb_runtime_cleanup_target;
DROP TABLE IF EXISTS fb_runtime_cleanup_mark;

CREATE TABLE fb_runtime_cleanup_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_runtime_cleanup_target VALUES (1, 'before');

CREATE TABLE fb_runtime_cleanup_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_runtime_cleanup_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

UPDATE fb_runtime_cleanup_target
SET payload = 'after'
WHERE id = 1;

SELECT fb_runtime_create_test_artifacts_debug(999999);
SELECT fb_runtime_create_test_artifacts_debug(pg_backend_pid());

SELECT count(*) AS stale_entries_before
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS t(name)
WHERE name LIKE 'fbspill-999999-%'
   OR name LIKE 'toast-retired-999999-%';

SELECT count(*) AS live_entries_before
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS t(name)
WHERE name LIKE format('fbspill-%s-%%', pg_backend_pid())
   OR name LIKE format('toast-retired-%s-%%', pg_backend_pid());

WITH flashback AS (
	SELECT *
	FROM pg_flashback(
		NULL::fb_runtime_cleanup_target,
		(SELECT target_ts::text FROM fb_runtime_cleanup_mark)
	)
)
SELECT count(*) AS result_count,
	   min(id) AS min_id,
	   max(payload) = 'before' AS restored_before
FROM flashback;

SELECT count(*) AS stale_entries_after
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS t(name)
WHERE name LIKE 'fbspill-999999-%'
   OR name LIKE 'toast-retired-999999-%';

SELECT count(*) AS live_entries_after
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS t(name)
WHERE name LIKE format('fbspill-%s-%%', pg_backend_pid())
   OR name LIKE format('toast-retired-%s-%%', pg_backend_pid());

SELECT fb_runtime_remove_test_artifacts_debug(pg_backend_pid());

SELECT count(*) AS live_entries_cleaned
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS t(name)
WHERE name LIKE format('fbspill-%s-%%', pg_backend_pid())
   OR name LIKE format('toast-retired-%s-%%', pg_backend_pid());

DROP TABLE IF EXISTS fb_runtime_error_target;
DROP TABLE IF EXISTS fb_runtime_error_mark;

CREATE TABLE fb_runtime_error_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_runtime_error_target VALUES (1, 'before');

CREATE TABLE fb_runtime_error_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_runtime_error_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

TRUNCATE fb_runtime_error_target;

DO $$
BEGIN
	BEGIN
		PERFORM count(*)
		FROM pg_flashback(
			NULL::fb_runtime_error_target,
			(SELECT target_ts::text FROM fb_runtime_error_mark)
		);
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'caught=%', SQLERRM;
	END;
END;
$$;

SELECT count(*) AS error_entries_after
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS t(name)
WHERE name LIKE format('fbspill-%s-%%', pg_backend_pid())
   OR name LIKE format('toast-retired-%s-%%', pg_backend_pid());
