DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
CREATE OR REPLACE FUNCTION fb_wal_source_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_wal_source_debug'
LANGUAGE C
STRICT;

SELECT current_setting('pg_flashback.ckwal_restore_dir', true) IS NULL
   AND current_setting('pg_flashback.ckwal_command', true) IS NULL;

SET pg_flashback.archive_dir = '/__legacy_path_should_not_be_used__';
DO $$
BEGIN
	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf /tmp/fb_archive_fixture /tmp/fb_empty_pg_wal_fixture && mkdir -p /tmp/fb_archive_fixture /tmp/fb_empty_pg_wal_fixture');
END;
$$;

DO $$
DECLARE
	datadir text;
	seg text;
BEGIN
	datadir := current_setting('data_directory');

	FOR seg IN
		SELECT name
		  FROM (
			SELECT name
			  FROM pg_ls_waldir()
			 WHERE name ~ '^[0-9A-F]{24}(\\.partial)?$'
			 ORDER BY name DESC
			 LIMIT 4
		  ) latest
		 ORDER BY name
	LOOP
		EXECUTE format(
			'COPY (SELECT '''') TO PROGRAM %L',
			'cp ' || quote_literal(datadir || '/pg_wal/' || seg) ||
			' ' || quote_literal('/tmp/fb_archive_fixture/' || seg));
	END LOOP;

	EXECUTE format('SET pg_flashback.archive_dest = %L', '/tmp/fb_archive_fixture');
	EXECUTE format('SET pg_flashback.debug_pg_wal_dir = %L', '/tmp/fb_empty_pg_wal_fixture');
END;
$$;

DO $$
DECLARE
	recovered_dir text;
BEGIN
	recovered_dir := current_setting('data_directory') || '/pg_flashback/recovered_wal';

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || quote_literal(recovered_dir) || ' && mkdir -p ' || quote_literal(recovered_dir));
END;
$$;

SELECT fb_wal_source_debug() ~ 'mode=archive_dest .*archive=[1-9][0-9]*';

COPY (SELECT '') TO PROGRAM 'rm -rf /tmp/fb_pgwal_fixture /tmp/fb_archive_fixture && mkdir -p /tmp/fb_pgwal_fixture /tmp/fb_archive_fixture';

SELECT pg_switch_wal() IS NOT NULL;
SELECT pg_switch_wal() IS NOT NULL;

DO $$
DECLARE
	original_name text;
	poison_name text;
	original_path text;
	poison_path text;
BEGIN
	WITH segs AS (
		SELECT name, row_number() OVER (ORDER BY name DESC) AS rn
		  FROM pg_ls_waldir()
		 WHERE name ~ '^[0-9A-F]{24}$'
	)
	SELECT max(name) FILTER (WHERE rn = 2),
		   max(name) FILTER (WHERE rn = 1)
	  INTO original_name, poison_name
	  FROM segs;

	IF original_name IS NULL OR poison_name IS NULL OR original_name = poison_name THEN
		RAISE EXCEPTION 'could not build WAL mismatch fixture';
	END IF;

	original_path := current_setting('data_directory') || '/pg_wal/' || original_name;
	poison_path := current_setting('data_directory') || '/pg_wal/' || poison_name;

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'cp ' || quote_literal(poison_path) || ' ' || quote_literal('/tmp/fb_pgwal_fixture/' || original_name));
END;
$$;

SET pg_flashback.archive_dest = '/tmp/fb_archive_fixture';
SET pg_flashback.debug_pg_wal_dir = '/tmp/fb_pgwal_fixture';

DO $$
DECLARE
	recovered_dir text;
BEGIN
	recovered_dir := current_setting('data_directory') || '/pg_flashback/recovered_wal';

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || quote_literal(recovered_dir) || ' && mkdir -p ' || quote_literal(recovered_dir));
END;
$$;

SELECT fb_wal_source_debug() ~ 'mode=archive_dest .*pg_wal=0 .*archive=0 .*ckwal=[1-9][0-9]*';
