CREATE EXTENSION pg_flashback;
SELECT fb_version();
CREATE TABLE fb_smoke_tbl (id integer PRIMARY KEY, v text);
SELECT fb_check_relation('fb_smoke_tbl'::regclass);
DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;
SELECT * FROM pg_flashback('fb_smoke_tbl'::regclass, now()) AS t(id integer, v text);
