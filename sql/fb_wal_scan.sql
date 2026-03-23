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
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_wal_scan_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE TABLE fb_wal_scan_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_wal_scan_target VALUES (0, repeat('seed', 128));
INSERT INTO fb_wal_scan_mark VALUES (clock_timestamp());

BEGIN;
INSERT INTO fb_wal_scan_target VALUES (1, repeat('commit', 128));
COMMIT;

BEGIN;
INSERT INTO fb_wal_scan_target VALUES (2, repeat('abort', 128));
ROLLBACK;

SELECT fb_scan_wal_debug('fb_wal_scan_target'::regclass,
						 (SELECT target_ts FROM fb_wal_scan_mark));

CREATE TABLE fb_wal_scan_risky (id integer);
INSERT INTO fb_wal_scan_mark VALUES (clock_timestamp());
TRUNCATE fb_wal_scan_risky;

SELECT fb_scan_wal_debug('fb_wal_scan_risky'::regclass,
						 (SELECT max(target_ts) FROM fb_wal_scan_mark));
