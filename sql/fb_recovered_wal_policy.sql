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
	IF to_regprocedure('fb_prepare_wal_scan_debug(timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_prepare_wal_scan_debug(timestamptz)';
	END IF;
END;
$$;

CREATE FUNCTION fb_prepare_wal_scan_debug(timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_prepare_wal_scan_debug'
LANGUAGE C
STRICT;

CREATE TEMP TABLE fb_ckwal_fixture (
	src_name text NOT NULL,
	wrong_name text NOT NULL
);

DO $$
DECLARE
	src_name text;
	tli text;
	logid text;
	seg text;
	next_seg text;
	data_dir text := current_setting('data_directory');
	archive_dir text := '/tmp/fb_ckwal_archive_fixture';
	pgwal_dir text := '/tmp/fb_ckwal_pgwal_fixture';
BEGIN
	SELECT name
	INTO src_name
	FROM pg_ls_dir(data_dir || '/pg_wal') AS d(name)
	WHERE d.name ~ '^[0-9A-F]{24}$'
	ORDER BY name
	LIMIT 1;

	IF src_name IS NULL THEN
		RAISE EXCEPTION 'no WAL segment found in pg_wal';
	END IF;

	tli := substr(src_name, 1, 8);
	logid := substr(src_name, 9, 8);
	seg := substr(src_name, 17, 8);
	next_seg := lpad(to_hex(('x' || seg)::bit(32)::bigint + 1), 8, '0');

	INSERT INTO fb_ckwal_fixture(src_name, wrong_name)
	VALUES (src_name, upper(tli || logid || next_seg));

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || archive_dir || ' ' || pgwal_dir ||
		' && mkdir -p ' || archive_dir || ' ' || pgwal_dir ||
		' && rm -f ' || data_dir || '/pg_flashback/recovered_wal/*');

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'cp ' || quote_literal(data_dir || '/pg_wal/' || src_name) || ' ' ||
		quote_literal(archive_dir || '/' || src_name));

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'cp ' || quote_literal(data_dir || '/pg_wal/' || src_name) || ' ' ||
		quote_literal(pgwal_dir || '/' || (SELECT wrong_name FROM fb_ckwal_fixture LIMIT 1)));

	EXECUTE format('SET pg_flashback.archive_dest = %L', archive_dir);
	EXECUTE format('SET pg_flashback.debug_pg_wal_dir = %L', pgwal_dir);
END;
$$;

DO $$
BEGIN
	BEGIN
		PERFORM fb_prepare_wal_scan_debug(clock_timestamp());
	EXCEPTION
		WHEN OTHERS THEN
			NULL;
	END;
END;
$$;

SELECT count(*) = 0 AS recovered_stays_empty
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/recovered_wal') AS d(name)
WHERE d.name ~ '^[0-9A-F]{24}$';

DO $$
DECLARE
	data_dir text := current_setting('data_directory');
	archive_dir text := '/tmp/fb_ckwal_archive_fixture';
	pgwal_dir text := '/tmp/fb_ckwal_pgwal_fixture';
BEGIN
	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || archive_dir || ' ' || pgwal_dir ||
		' && mkdir -p ' || archive_dir || ' ' || pgwal_dir ||
		' && rm -f ' || data_dir || '/pg_flashback/recovered_wal/*');

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'cp ' || quote_literal(data_dir || '/pg_wal/' ||
							   (SELECT src_name FROM fb_ckwal_fixture LIMIT 1)) || ' ' ||
		quote_literal(archive_dir || '/' ||
					  (SELECT wrong_name FROM fb_ckwal_fixture LIMIT 1)));

	EXECUTE format('SET pg_flashback.archive_dest = %L', archive_dir);
	EXECUTE format('SET pg_flashback.debug_pg_wal_dir = %L', pgwal_dir);
END;
$$;

DO $$
BEGIN
	BEGIN
		PERFORM fb_prepare_wal_scan_debug(clock_timestamp());
	EXCEPTION
		WHEN OTHERS THEN
			NULL;
	END;
END;
$$;

SELECT count(*) = 0 AS archive_wrong_name_does_not_materialize
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/recovered_wal') AS d(name)
WHERE d.name ~ '^[0-9A-F]{24}$';

DO $$
DECLARE
	data_dir text := current_setting('data_directory');
	archive_dir text := '/tmp/fb_ckwal_archive_fixture';
	pgwal_dir text := '/tmp/fb_ckwal_pgwal_fixture';
BEGIN
	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || archive_dir || ' ' || pgwal_dir ||
		' && mkdir -p ' || archive_dir || ' ' || pgwal_dir ||
		' && rm -f ' || data_dir || '/pg_flashback/recovered_wal/*');

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'cp ' || quote_literal(data_dir || '/pg_wal/' ||
							   (SELECT src_name FROM fb_ckwal_fixture LIMIT 1)) || ' ' ||
		quote_literal(pgwal_dir || '/' ||
					  (SELECT wrong_name FROM fb_ckwal_fixture LIMIT 1)));

	EXECUTE format('SET pg_flashback.archive_dest = %L', archive_dir);
	EXECUTE format('SET pg_flashback.debug_pg_wal_dir = %L', pgwal_dir);
END;
$$;

DO $$
BEGIN
	BEGIN
		PERFORM fb_prepare_wal_scan_debug(clock_timestamp());
	EXCEPTION
		WHEN OTHERS THEN
			NULL;
	END;
END;
$$;

SELECT count(*) = 1 AS pgwal_mismatch_materializes_once
FROM pg_ls_dir(current_setting('data_directory') || '/pg_flashback/recovered_wal') AS d(name)
WHERE d.name = (SELECT src_name FROM fb_ckwal_fixture LIMIT 1);

DROP FUNCTION fb_prepare_wal_scan_debug(timestamptz);
