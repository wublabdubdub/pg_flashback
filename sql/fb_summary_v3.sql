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
SET pg_flashback.parallel_workers = 0;
SET pg_flashback.show_progress = off;

DO $$
BEGIN
	IF to_regprocedure('fb_summary_build_available_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_build_available_debug()';
	END IF;
	IF to_regprocedure('fb_summary_v6_rejected_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_v6_rejected_debug()';
	END IF;
	IF to_regprocedure('fb_recordref_debug(regclass, timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_debug(regclass, timestamptz)';
	END IF;
END;
$$;

CREATE FUNCTION fb_summary_build_available_debug()
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_build_available_debug'
LANGUAGE C;

CREATE FUNCTION fb_summary_v6_rejected_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_summary_v6_rejected_debug'
LANGUAGE C;

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_debug'
LANGUAGE C;

DROP TABLE IF EXISTS fb_summary_v3_target;
DROP TABLE IF EXISTS fb_summary_v3_mark;

CREATE TABLE fb_summary_v3_target (
	id integer PRIMARY KEY,
	note text
);

CHECKPOINT;

INSERT INTO fb_summary_v3_target VALUES
	(1, 'before'),
	(2, 'before'),
	(3, 'before');

CREATE TABLE fb_summary_v3_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_summary_v3_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

UPDATE fb_summary_v3_target
SET note = 'after'
WHERE id IN (1, 2);

DELETE FROM fb_summary_v3_target
WHERE id = 3;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS built_summary;

SELECT fb_summary_v6_rejected_debug() AS summary_v6_rejected_contract
\gset

SELECT :'summary_v6_rejected_contract' LIKE '%summary_v6_rejected=true%' AS rejects_v6_summary;

SELECT substring(
		   fb_recordref_debug(
			   'fb_summary_v3_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_mark)
		   )
		   FROM 'summary_span_windows=([0-9]+)')::int > 0
	   AS uses_summary_spans,
	   substring(
		   fb_recordref_debug(
			   'fb_summary_v3_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_mark)
		   )
		   FROM 'summary_xid_hits=([0-9]+)')::int > 0
	   AS uses_summary_xid_outcomes,
	   substring(
		   fb_recordref_debug(
			   'fb_summary_v3_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_mark)
		   )
		   FROM 'summary_xid_segments_read=([0-9]+)')::int > 0
	   AS tracks_summary_xid_segments,
	   substring(
		   fb_recordref_debug(
			   'fb_summary_v3_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_mark)
		   )
		   FROM 'summary_xid_fallback=([0-9]+)')::int = 0
	   AS avoids_xid_refill,
	   substring(
		   fb_recordref_debug(
			   'fb_summary_v3_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_mark)
		   )
		   FROM 'metadata_fallback_windows=([0-9]+)') IS NOT NULL
	   AS tracks_metadata_fallback_counter;

SELECT *
FROM pg_flashback(
	NULL::public.fb_summary_v3_target,
	(SELECT target_ts::text FROM fb_summary_v3_mark)
)
ORDER BY id;

DROP TABLE IF EXISTS fb_summary_v3_unsafe_target;
DROP TABLE IF EXISTS fb_summary_v3_unsafe_mark;

CREATE TABLE fb_summary_v3_unsafe_target (
	id integer PRIMARY KEY,
	note text
);

CHECKPOINT;

INSERT INTO fb_summary_v3_unsafe_target VALUES (1, 'before');

CREATE TABLE fb_summary_v3_unsafe_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_summary_v3_unsafe_mark VALUES (clock_timestamp());

DO $$
BEGIN
	PERFORM pg_sleep(1.1);
END;
$$;

TRUNCATE fb_summary_v3_unsafe_target;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS rebuilt_summary_after_unsafe;

SELECT substring(
		   fb_recordref_debug(
			   'fb_summary_v3_unsafe_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_unsafe_mark)
		   )
		   FROM 'summary_unsafe_hits=([0-9]+)')::int > 0
	   AS uses_summary_unsafe_facts,
	   substring(
		   fb_recordref_debug(
			   'fb_summary_v3_unsafe_target'::regclass,
			   (SELECT target_ts FROM fb_summary_v3_unsafe_mark)
		   )
		   FROM 'metadata_fallback_windows=([0-9]+)') IS NOT NULL
	   AS tracks_unsafe_metadata_fallback_counter;
