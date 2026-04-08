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
	IF to_regprocedure('fb_recordref_debug(regclass,timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_debug(regclass, timestamptz)';
	END IF;
	IF to_regprocedure('fb_summary_xid_resolution_debug(regclass,timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_xid_resolution_debug(regclass, timestamptz)';
	END IF;
	IF to_regprocedure('fb_summary_build_available_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_build_available_debug()';
	END IF;
END;
$$;

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_summary_xid_resolution_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_summary_xid_resolution_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_summary_build_available_debug()
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_build_available_debug'
LANGUAGE C
STRICT;

DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;

CREATE TABLE fb_recordref_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_target VALUES (0, 10);
UPDATE fb_recordref_target SET payload = 11 WHERE id = 0;

CREATE TABLE fb_recordref_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

CHECKPOINT;

INSERT INTO fb_recordref_target VALUES (1, 20);
UPDATE fb_recordref_target SET payload = 21 WHERE id = 1;
DELETE FROM fb_recordref_target WHERE id = 0;

BEGIN;
INSERT INTO fb_recordref_target VALUES (2, 30);
ROLLBACK;

SET pg_flashback.parallel_workers = 0;

SELECT fb_recordref_debug(
	'fb_recordref_target'::regclass,
	(SELECT target_ts FROM fb_recordref_mark)
) AS serial_summary
\gset

SET pg_flashback.parallel_workers = 4;

SELECT fb_recordref_debug(
	'fb_recordref_target'::regclass,
	(SELECT target_ts FROM fb_recordref_mark)
) AS parallel_summary
\gset

SELECT :'serial_summary' LIKE '%parallel=off%' AS parallel_off,
	   :'serial_summary' LIKE '%prefilter=on%' AS prefilter_on,
	   :'serial_summary' LIKE '%commits=%' AS serial_commits_present,
	   :'serial_summary' LIKE '%aborts=%' AS serial_aborts_present;

SELECT :'parallel_summary' LIKE '%parallel=on%' AS parallel_on,
	   :'parallel_summary' LIKE '%prefilter=on%' AS parallel_prefilter_on,
	   :'parallel_summary' LIKE '%commits=%' AS parallel_commits_present,
	   :'parallel_summary' LIKE '%aborts=%' AS parallel_aborts_present;

SELECT :'serial_summary' LIKE '%anchor=true%' AS serial_anchor_true,
	   :'parallel_summary' LIKE '%anchor=true%' AS parallel_anchor_true,
	   :'serial_summary' LIKE '%target_dml=3%' AS serial_target_dml_expected,
	   :'parallel_summary' LIKE '%target_dml=3%' AS parallel_target_dml_expected;

SELECT :'serial_summary' LIKE '%payload_windows=%' AS serial_payload_windows_present,
	   :'serial_summary' LIKE '%payload_scan_mode=%' AS serial_payload_scan_mode_present,
	   :'serial_summary' LIKE '%payload_covered_segments=%' AS serial_payload_covered_segments_present,
	   :'serial_summary' LIKE '%payload_scanned_records=%' AS serial_payload_scanned_records_present,
	   :'serial_summary' LIKE '%payload_kept_records=%' AS serial_payload_kept_records_present,
	   :'serial_summary' LIKE '%xact_summary_spool_records=%'
	   AS serial_xact_summary_spool_records_present,
	   :'serial_summary' LIKE '%xact_summary_spool_hits=%'
	   AS serial_xact_summary_spool_hits_present,
	   :'serial_summary' LIKE '%summary_payload_locator_records=%'
	   AS serial_summary_payload_locator_records_present,
	   :'serial_summary' LIKE '%summary_payload_locator_segments_read=%'
	   AS serial_summary_payload_locator_segments_read_present,
	   :'serial_summary' LIKE '%summary_payload_locator_public_builds=%'
	   AS serial_summary_payload_locator_public_builds_present,
	   :'serial_summary' LIKE '%summary_payload_locator_fallback_segments=%'
	   AS serial_summary_payload_locator_fallback_present,
	   :'serial_summary' LIKE '%summary_span_segments_read=%'
	   AS serial_summary_span_segments_read_present,
	   :'serial_summary' LIKE '%summary_span_public_builds=%'
	   AS serial_summary_span_public_builds_present,
	   :'serial_summary' LIKE '%summary_xid_exact_hits=%'
	   AS serial_summary_xid_exact_hits_present,
	   :'serial_summary' LIKE '%summary_xid_exact_segments_read=%'
	   AS serial_summary_xid_exact_segments_read_present,
	   :'serial_summary' LIKE '%xact_fallback_windows=%'
	   AS serial_xact_fallback_windows_present,
	   :'serial_summary' LIKE '%xact_fallback_covered_segments=%'
	   AS serial_xact_fallback_segments_present;

SELECT (regexp_match(:'serial_summary', 'xact_summary_spool_hits=([0-9]+)'))[1]::integer > 0
	   AS serial_xact_summary_spool_hit_expected,
	   (regexp_match(:'serial_summary', 'summary_xid_segments_read=([0-9]+)'))[1]::integer = 0
	   AS serial_xact_summary_avoids_summary_scan,
	   (regexp_match(:'serial_summary', 'xact_fallback_windows=([0-9]+)'))[1]::integer = 0
	   AS serial_xact_summary_avoids_wal_fallback;

SELECT (regexp_match(:'serial_summary', 'payload_windows=([0-9]+)'))[1]::integer <
	   (regexp_match(:'serial_summary', 'payload_refs=([0-9]+)'))[1]::integer
	   AS serial_payload_locator_batched;

SELECT :'parallel_summary' LIKE '%payload_windows=%' AS parallel_payload_windows_present,
	   :'parallel_summary' LIKE '%payload_scan_mode=%' AS parallel_payload_scan_mode_present,
	   :'parallel_summary' LIKE '%payload_covered_segments=%' AS parallel_payload_covered_segments_present,
	   :'parallel_summary' LIKE '%payload_scanned_records=%' AS parallel_payload_scanned_records_present,
	   :'parallel_summary' LIKE '%payload_kept_records=%' AS parallel_payload_kept_records_present,
	   :'parallel_summary' LIKE '%xact_summary_spool_records=%'
	   AS parallel_xact_summary_spool_records_present,
	   :'parallel_summary' LIKE '%xact_summary_spool_hits=%'
	   AS parallel_xact_summary_spool_hits_present,
	   :'parallel_summary' LIKE '%summary_payload_locator_records=%'
	   AS parallel_summary_payload_locator_records_present,
	   :'parallel_summary' LIKE '%summary_payload_locator_segments_read=%'
	   AS parallel_summary_payload_locator_segments_read_present,
	   :'parallel_summary' LIKE '%summary_payload_locator_public_builds=%'
	   AS parallel_summary_payload_locator_public_builds_present,
	   :'parallel_summary' LIKE '%summary_payload_locator_fallback_segments=%'
	   AS parallel_summary_payload_locator_fallback_present,
	   :'parallel_summary' LIKE '%summary_span_segments_read=%'
	   AS parallel_summary_span_segments_read_present,
	   :'parallel_summary' LIKE '%summary_span_public_builds=%'
	   AS parallel_summary_span_public_builds_present,
	   :'parallel_summary' LIKE '%summary_xid_exact_hits=%'
	   AS parallel_summary_xid_exact_hits_present,
	   :'parallel_summary' LIKE '%summary_xid_exact_segments_read=%'
	   AS parallel_summary_xid_exact_segments_read_present,
	   :'parallel_summary' LIKE '%xact_fallback_windows=%'
	   AS parallel_xact_fallback_windows_present,
	   :'parallel_summary' LIKE '%xact_fallback_covered_segments=%'
	   AS parallel_xact_fallback_segments_present;

SELECT (regexp_match(:'parallel_summary', 'xact_summary_spool_hits=([0-9]+)'))[1]::integer > 0
	   AS parallel_xact_summary_spool_hit_expected,
	   (regexp_match(:'parallel_summary', 'summary_xid_segments_read=([0-9]+)'))[1]::integer = 0
	   AS parallel_xact_summary_avoids_summary_scan,
	   (regexp_match(:'parallel_summary', 'xact_fallback_windows=([0-9]+)'))[1]::integer = 0
	   AS parallel_xact_summary_avoids_wal_fallback;

SELECT (regexp_match(:'parallel_summary', 'payload_windows=([0-9]+)'))[1]::integer <
	   (regexp_match(:'parallel_summary', 'payload_refs=([0-9]+)'))[1]::integer
	   AS parallel_payload_locator_batched;

DROP TABLE IF EXISTS fb_recordref_subxact_target;
DROP TABLE IF EXISTS fb_recordref_subxact_mark;

CREATE TABLE fb_recordref_subxact_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_subxact_target VALUES (1, 10);

CREATE TABLE fb_recordref_subxact_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_subxact_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

BEGIN;
SAVEPOINT fb_recordref_subxact_s1;
UPDATE fb_recordref_subxact_target
SET payload = 11
WHERE id = 1;
RELEASE SAVEPOINT fb_recordref_subxact_s1;
SAVEPOINT fb_recordref_subxact_s2;
INSERT INTO fb_recordref_subxact_target VALUES (2, 20);
RELEASE SAVEPOINT fb_recordref_subxact_s2;
COMMIT;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
	PERFORM fb_summary_build_available_debug();
END;
$$;

SET pg_flashback.parallel_workers = 4;

SELECT fb_summary_xid_resolution_debug(
	'fb_recordref_subxact_target'::regclass,
	(SELECT target_ts FROM fb_recordref_subxact_mark)
) AS subxact_summary_resolution
\gset

SELECT (regexp_match(:'subxact_summary_resolution', 'summary_hits=([0-9]+)'))[1]::integer > 0
	   AS subxact_summary_uses_xid_outcomes,
	   (regexp_match(:'subxact_summary_resolution', 'unresolved_touched=([0-9]+)'))[1]::integer = 0
	   AS subxact_summary_resolves_all_touched_xids,
	   (regexp_match(:'subxact_summary_resolution', 'fallback_windows=([0-9]+)'))[1]::integer = 0
	   AS subxact_summary_avoids_fallback_windows;

DROP TABLE IF EXISTS fb_recordref_abort_subxact_target;
DROP TABLE IF EXISTS fb_recordref_abort_subxact_mark;

CREATE TABLE fb_recordref_abort_subxact_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_abort_subxact_target VALUES (1, 10);

CREATE TABLE fb_recordref_abort_subxact_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_abort_subxact_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

BEGIN;
SAVEPOINT fb_recordref_abort_subxact_s1;
UPDATE fb_recordref_abort_subxact_target
SET payload = 11
WHERE id = 1;
ROLLBACK TO SAVEPOINT fb_recordref_abort_subxact_s1;
SAVEPOINT fb_recordref_abort_subxact_s2;
INSERT INTO fb_recordref_abort_subxact_target VALUES (2, 20);
RELEASE SAVEPOINT fb_recordref_abort_subxact_s2;
COMMIT;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
	PERFORM fb_summary_build_available_debug();
END;
$$;

SET pg_flashback.parallel_workers = 4;

SELECT fb_summary_xid_resolution_debug(
	'fb_recordref_abort_subxact_target'::regclass,
	(SELECT target_ts FROM fb_recordref_abort_subxact_mark)
) AS abort_subxact_summary_resolution
\gset

SELECT (regexp_match(:'abort_subxact_summary_resolution', 'summary_hits=([0-9]+)'))[1]::integer > 0
	   AS abort_subxact_summary_uses_xid_outcomes,
	   (regexp_match(:'abort_subxact_summary_resolution', 'unresolved_touched=([0-9]+)'))[1]::integer = 0
	   AS abort_subxact_summary_resolves_all_touched_xids,
	   (regexp_match(:'abort_subxact_summary_resolution', 'unresolved_unsafe=([0-9]+)'))[1]::integer = 0
	   AS abort_subxact_summary_resolves_all_unsafe_xids,
	   (regexp_match(:'abort_subxact_summary_resolution', 'fallback_windows=([0-9]+)'))[1]::integer = 0
	   AS abort_subxact_summary_avoids_fallback_windows;

DROP TABLE IF EXISTS fb_recordref_overflow_subxact_target;
DROP TABLE IF EXISTS fb_recordref_overflow_subxact_mark;

CREATE TABLE fb_recordref_overflow_subxact_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_overflow_subxact_target VALUES (1, 10);

CREATE TABLE fb_recordref_overflow_subxact_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_overflow_subxact_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

BEGIN;
\o /dev/null
SELECT format(
		   'SAVEPOINT fb_recordref_overflow_subxact_s%1$s; ' ||
		   'INSERT INTO fb_recordref_overflow_subxact_target VALUES (%2$s, %2$s); ' ||
		   'RELEASE SAVEPOINT fb_recordref_overflow_subxact_s%1$s;',
		   gs,
		   gs + 1)
FROM generate_series(1, 128) AS gs
\gexec
\o
COMMIT;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
	PERFORM fb_summary_build_available_debug();
END;
$$;

SET pg_flashback.parallel_workers = 4;

SELECT fb_summary_xid_resolution_debug(
	'fb_recordref_overflow_subxact_target'::regclass,
	(SELECT target_ts FROM fb_recordref_overflow_subxact_mark)
) AS overflow_subxact_summary_resolution
\gset

SELECT (regexp_match(:'overflow_subxact_summary_resolution', 'summary_hits=([0-9]+)'))[1]::integer > 0
	   AS overflow_subxact_summary_uses_xid_outcomes,
	   (regexp_match(:'overflow_subxact_summary_resolution', 'unresolved_touched=([0-9]+)'))[1]::integer = 0
	   AS overflow_subxact_summary_resolves_all_touched_xids,
	   (regexp_match(:'overflow_subxact_summary_resolution', 'fallback_windows=([0-9]+)'))[1]::integer = 0
	   AS overflow_subxact_summary_avoids_fallback_windows;

CREATE TABLE fb_recordref_unsafe (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_recordref_unsafe VALUES (1, 10);

CREATE TABLE fb_recordref_unsafe_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_recordref_unsafe_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

TRUNCATE fb_recordref_unsafe;

SET pg_flashback.parallel_workers = 0;

SELECT fb_recordref_debug(
	'fb_recordref_unsafe'::regclass,
	(SELECT target_ts FROM fb_recordref_unsafe_mark)
) AS serial_unsafe_summary
\gset

SET pg_flashback.parallel_workers = 4;

SELECT fb_recordref_debug(
	'fb_recordref_unsafe'::regclass,
	(SELECT target_ts FROM fb_recordref_unsafe_mark)
) AS parallel_unsafe_summary
\gset

SELECT :'serial_unsafe_summary' LIKE '%unsafe=true%' AS serial_unsafe_true,
	   :'serial_unsafe_summary' LIKE '%reason=%' AS serial_reason_present;

SELECT :'parallel_unsafe_summary' LIKE '%unsafe=true%' AS parallel_unsafe_true,
	   :'parallel_unsafe_summary' LIKE '%reason=%' AS parallel_reason_present;

SELECT regexp_replace(
		   :'serial_unsafe_summary',
		   ' (parallel|prefilter|visited_segments|payload_windows|payload_scan_mode|payload_parallel_workers|payload_covered_segments|payload_scanned_records|payload_kept_records|summary_payload_locator_records|summary_payload_locator_segments_read|summary_payload_locator_public_builds|summary_payload_locator_fallback_segments|summary_span_windows|summary_span_segments_read|summary_span_public_builds|summary_xid_hits|summary_xid_fallback|summary_xid_segments_read|summary_xid_exact_hits|summary_xid_exact_segments_read|summary_unsafe_hits|metadata_fallback_windows|xact_fallback_windows|xact_fallback_covered_segments|xact_summary_spool_records|xact_summary_spool_hits)=[^ ]+',
		   '',
		   'g'
	   ) =
	   regexp_replace(
		   :'parallel_unsafe_summary',
		   ' (parallel|prefilter|visited_segments|payload_windows|payload_scan_mode|payload_parallel_workers|payload_covered_segments|payload_scanned_records|payload_kept_records|summary_payload_locator_records|summary_payload_locator_segments_read|summary_payload_locator_public_builds|summary_payload_locator_fallback_segments|summary_span_windows|summary_span_segments_read|summary_span_public_builds|summary_xid_hits|summary_xid_fallback|summary_xid_segments_read|summary_xid_exact_hits|summary_xid_exact_segments_read|summary_unsafe_hits|metadata_fallback_windows|xact_fallback_windows|xact_fallback_covered_segments|xact_summary_spool_records|xact_summary_spool_hits)=[^ ]+',
		   '',
		   'g'
	   ) AS unsafe_serial_parallel_stable_contract_equal;
