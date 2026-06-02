DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;
SELECT fb_version();
CREATE TABLE fb_smoke_tbl (id integer PRIMARY KEY, v text);
SELECT fb_check_relation('fb_smoke_tbl'::regclass);
DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;
SELECT *
FROM pg_flashback(NULL::public.fb_smoke_tbl, now()::text);
