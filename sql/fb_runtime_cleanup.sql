DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;

CREATE FUNCTION fb_runtime_touch_debug(text, text, integer, boolean)
RETURNS text
AS '$libdir/pg_flashback', 'fb_runtime_touch_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_runtime_cleanup_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_runtime_cleanup_debug'
LANGUAGE C;

CREATE FUNCTION fb_runtime_retention_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_runtime_retention_debug'
LANGUAGE C;

SELECT fb_runtime_retention_debug();

SET pg_flashback.runtime_retention = '1h';
SET pg_flashback.recovered_wal_retention = '2h';
SET pg_flashback.meta_retention = '3h';

SELECT fb_runtime_retention_debug();

CREATE TEMP TABLE fb_runtime_cleanup_marks (
	kind text PRIMARY KEY,
	basename text NOT NULL
);

INSERT INTO fb_runtime_cleanup_marks(kind, basename)
VALUES
	('runtime_dead_old', fb_runtime_touch_debug('runtime', 'old-runtime', 7200, false)),
	('runtime_live_old', fb_runtime_touch_debug('runtime', 'live-runtime', 7200, true)),
	('recovered_old', fb_runtime_touch_debug('recovered_wal', '0000000100000000000000AA', 10800, false)),
	('recovered_new', fb_runtime_touch_debug('recovered_wal', '0000000100000000000000AB', 30, false)),
	('meta_old', fb_runtime_touch_debug('meta', 'prefilter-old.meta', 14400, false)),
	('meta_new', fb_runtime_touch_debug('meta', 'prefilter-new.meta', 30, false));

SELECT fb_runtime_cleanup_debug() <> '' AS cleanup_ran;

SELECT
	EXISTS (
		SELECT 1
		FROM fb_runtime_cleanup_marks m
		WHERE m.kind = 'runtime_dead_old'
		  AND EXISTS (
			SELECT 1
			FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS d(name)
			WHERE d.name = m.basename
		  )
	) AS runtime_dead_old_exists,
	EXISTS (
		SELECT 1
		FROM fb_runtime_cleanup_marks m
		WHERE m.kind = 'runtime_live_old'
		  AND EXISTS (
			SELECT 1
			FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/runtime') AS d(name)
			WHERE d.name = m.basename
		  )
	) AS runtime_live_old_exists,
	EXISTS (
		SELECT 1
		FROM fb_runtime_cleanup_marks m
		WHERE m.kind = 'recovered_old'
		  AND EXISTS (
			SELECT 1
			FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/recovered_wal') AS d(name)
			WHERE d.name = m.basename
		  )
	) AS recovered_old_exists,
	EXISTS (
		SELECT 1
		FROM fb_runtime_cleanup_marks m
		WHERE m.kind = 'recovered_new'
		  AND EXISTS (
			SELECT 1
			FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/recovered_wal') AS d(name)
			WHERE d.name = m.basename
		  )
	) AS recovered_new_exists,
	EXISTS (
		SELECT 1
		FROM fb_runtime_cleanup_marks m
		WHERE m.kind = 'meta_old'
		  AND EXISTS (
			SELECT 1
			FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/meta') AS d(name)
			WHERE d.name = m.basename
		  )
	) AS meta_old_exists,
	EXISTS (
		SELECT 1
		FROM fb_runtime_cleanup_marks m
		WHERE m.kind = 'meta_new'
		  AND EXISTS (
			SELECT 1
			FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/meta') AS d(name)
			WHERE d.name = m.basename
		  )
	) AS meta_new_exists;

DROP FUNCTION fb_runtime_touch_debug(text, text, integer, boolean);
DROP FUNCTION fb_runtime_cleanup_debug();
DROP FUNCTION fb_runtime_retention_debug();
