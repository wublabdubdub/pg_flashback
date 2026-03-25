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

SET pg_flashback.archive_dir = '/__legacy_path_should_not_be_used__';

DO $$
BEGIN
	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf /tmp/fb_empty_pg_wal_fixture && mkdir -p /tmp/fb_empty_pg_wal_fixture');
	EXECUTE format('SET pg_flashback.archive_dest = %L', current_setting('data_directory') || '/pg_wal');
	EXECUTE format('SET pg_flashback.debug_pg_wal_dir = %L', '/tmp/fb_empty_pg_wal_fixture');
END;
$$;

CREATE TABLE fb_wal_sources_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE TABLE fb_wal_sources_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_wal_sources_target VALUES (0, repeat('seed', 64));
INSERT INTO fb_wal_sources_mark VALUES (clock_timestamp());

BEGIN;
INSERT INTO fb_wal_sources_target VALUES (1, repeat('commit', 64));
COMMIT;

SELECT fb_scan_wal_debug(
	'fb_wal_sources_target'::regclass,
	(SELECT target_ts FROM fb_wal_sources_mark)
) AS summary
\gset

SELECT :'summary' LIKE '%parallel=off%' AS parallel_off,
	   :'summary' LIKE '%prefilter=off%' AS prefilter_off,
	   :'summary' ~ 'visited_segments=[0-9]+/[0-9]+' AS visited_seen,
	   :'summary' LIKE '%complete=true%' AS complete_seen;
