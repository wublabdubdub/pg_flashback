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

SET pg_flashback.parallel_workers = 4;

DROP TABLE IF EXISTS fb_unresolved_xid_debug_resolved_target;
DROP TABLE IF EXISTS fb_unresolved_xid_debug_resolved_mark;

CREATE TABLE fb_unresolved_xid_debug_resolved_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_unresolved_xid_debug_resolved_target VALUES (1, 10);

CREATE TABLE fb_unresolved_xid_debug_resolved_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_unresolved_xid_debug_resolved_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

BEGIN;
SAVEPOINT fb_unresolved_xid_debug_resolved_s1;
UPDATE fb_unresolved_xid_debug_resolved_target
SET payload = 11
WHERE id = 1;
RELEASE SAVEPOINT fb_unresolved_xid_debug_resolved_s1;
SAVEPOINT fb_unresolved_xid_debug_resolved_s2;
INSERT INTO fb_unresolved_xid_debug_resolved_target VALUES (2, 20);
RELEASE SAVEPOINT fb_unresolved_xid_debug_resolved_s2;
COMMIT;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT count(*) >= 0 AS resolved_case_debug_query_runs
FROM pg_flashback_debug_unresolv_xid(
	'fb_unresolved_xid_debug_resolved_target'::regclass,
	(SELECT target_ts FROM fb_unresolved_xid_debug_resolved_mark)
);

DROP TABLE IF EXISTS fb_unresolved_xid_debug_fallback_target;
DROP TABLE IF EXISTS fb_unresolved_xid_debug_fallback_mark;

CREATE TABLE fb_unresolved_xid_debug_fallback_target (
	id integer PRIMARY KEY,
	payload integer
);

CHECKPOINT;

INSERT INTO fb_unresolved_xid_debug_fallback_target VALUES (1, 10);

CREATE TABLE fb_unresolved_xid_debug_fallback_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_unresolved_xid_debug_fallback_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

BEGIN;
SAVEPOINT fb_unresolved_xid_debug_fallback_s1;
UPDATE fb_unresolved_xid_debug_fallback_target
SET payload = 11
WHERE id = 1;
RELEASE SAVEPOINT fb_unresolved_xid_debug_fallback_s1;
SELECT pg_switch_wal() IS NOT NULL AS switched_segment;
COMMIT;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

WITH rows AS (
	SELECT *
	FROM pg_flashback_debug_unresolv_xid(
		'fb_unresolved_xid_debug_fallback_target'::regclass,
		(SELECT target_ts FROM fb_unresolved_xid_debug_fallback_mark)
	)
)
SELECT count(*) >= 0 AS fallback_case_debug_query_runs,
	   COALESCE(bool_and(xid > 0), true) AS fallback_case_xid_positive_when_present,
	   COALESCE(bool_and(xid_role IN ('touched', 'unsafe')), true)
	   AS fallback_case_role_domain_ok_when_present,
	   COALESCE(bool_and(resolved_by IN ('summary', 'exact_window', 'exact_all', 'unresolved')), true)
	   AS fallback_case_resolved_by_domain_ok_when_present,
	   COALESCE(bool_and(diag IS NOT NULL AND diag <> ''), true)
	   AS fallback_case_diag_present_when_present
FROM rows;

WITH rows AS (
	SELECT *
	FROM pg_flashback_debug_unresolv_xid(
		'fb_unresolved_xid_debug_fallback_target'::regclass,
		(SELECT target_ts FROM fb_unresolved_xid_debug_fallback_mark)
	)
)
SELECT COALESCE(
		   bool_and(NOT (
			   resolved_by = 'exact_all' AND
			   fallback_reason = 'summary_missing_assignment' AND
			   diag LIKE '%all_outcome_found=true%'
		   )),
		   true
	   ) AS fallback_case_exact_all_direct_outcome_not_misclassified
FROM rows;
