DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;

CREATE TABLE fb_runtime_table (id integer);

SELECT fb_export_undo('fb_runtime_table'::regclass, now());

SET pg_flashback.archive_dir = '/__definitely_missing_pg_flashback_archive__';
SELECT fb_export_undo('fb_runtime_table'::regclass, now());

SET pg_flashback.archive_dir = '/tmp';
SELECT fb_export_undo('fb_runtime_table'::regclass, now());

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;
SELECT fb_export_undo('fb_runtime_table'::regclass, now() + interval '1 hour');
SELECT fb_export_undo('pg_class'::regclass, now());
SELECT fb_export_undo('fb_runtime_table'::regclass, now());
