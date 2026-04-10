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
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

DO $$
BEGIN
	IF to_regprocedure('fb_target_snapshot_clog_status_debug(bigint,timestamptz,timestamptz,text)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_target_snapshot_clog_status_debug(bigint, timestamptz, timestamptz, text)';
	END IF;
END;
$$;

CREATE FUNCTION fb_target_snapshot_clog_status_debug(bigint, timestamptz, timestamptz, text)
RETURNS text
AS '$libdir/pg_flashback', 'fb_target_snapshot_clog_status_debug'
LANGUAGE C
STRICT;

CREATE TABLE fb_target_snapshot_target (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

INSERT INTO fb_target_snapshot_target VALUES (1, 'alpha');
CHECKPOINT;

\setenv PGDATABASE :DBNAME
\setenv PGPORT :PORT
\setenv PGUSER :USER
\setenv PGHOST :HOST

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT txid_current_snapshot()::text AS fb_snapshot \gset
\! psql -X -v ON_ERROR_STOP=1 -qAt -c "UPDATE fb_target_snapshot_target SET payload = 'beta' WHERE id = 1" > /tmp/fb_target_snapshot_update.log 2>&1
SELECT clock_timestamp()::text AS fb_target_ts \gset
COMMIT;

SELECT id, payload FROM fb_target_snapshot_target ORDER BY 1;

SELECT set_config('pg_flashback.target_snapshot', :'fb_snapshot', false) = :'fb_snapshot' AS snapshot_applied;

SELECT id, payload
FROM pg_flashback(
	NULL::public.fb_target_snapshot_target,
	:'fb_target_ts'
)
ORDER BY 1;

RESET pg_flashback.target_snapshot;

CREATE TABLE fb_target_snapshot_clog_contract_target (
	id integer PRIMARY KEY,
	payload text NOT NULL
);

BEGIN;
SELECT pg_current_xact_id()::text AS fb_clog_before_xid \gset
INSERT INTO fb_target_snapshot_clog_contract_target VALUES (1, 'before');
COMMIT;

CHECKPOINT;

BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT txid_current_snapshot()::text AS fb_clog_snapshot \gset
SELECT clock_timestamp()::text AS fb_clog_target_ts \gset
COMMIT;

SELECT pg_sleep(1.1);

BEGIN;
SELECT pg_current_xact_id()::text AS fb_clog_after_xid \gset
UPDATE fb_target_snapshot_clog_contract_target
SET payload = 'after'
WHERE id = 1;
COMMIT;

SELECT fb_target_snapshot_clog_status_debug(
	:'fb_clog_before_xid'::bigint,
	:'fb_clog_target_ts'::timestamptz,
	clock_timestamp(),
	:'fb_clog_snapshot'
) LIKE '%resolved=true status=committed snapshot_invisible=false committed_before_target=true committed_after_target=false%'
AS snapshot_clog_committed_before_target;

SELECT fb_target_snapshot_clog_status_debug(
	:'fb_clog_after_xid'::bigint,
	:'fb_clog_target_ts'::timestamptz,
	clock_timestamp(),
	:'fb_clog_snapshot'
) LIKE '%resolved=true status=committed snapshot_invisible=true committed_before_target=false committed_after_target=true%'
AS snapshot_clog_committed_after_target;
