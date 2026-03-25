DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;

CREATE TABLE fb_runtime_table (id integer);

SELECT pg_flashback('fb_runtime_result', 'fb_runtime_table', now()::text);

SET pg_flashback.archive_dir = '/__definitely_missing_pg_flashback_archive__';
SELECT pg_flashback('fb_runtime_result', 'fb_runtime_table', now()::text);

SET pg_flashback.archive_dir = '/tmp';
SELECT pg_flashback('fb_runtime_result', 'fb_runtime_table', now()::text);

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;
SELECT pg_flashback('fb_runtime_result', 'fb_runtime_table', (now() + interval '1 hour')::text);
SELECT pg_flashback('fb_runtime_result', 'pg_class', now()::text);
SELECT pg_flashback('fb_runtime_result', 'fb_runtime_table', now()::text);
TABLE fb_runtime_result;
