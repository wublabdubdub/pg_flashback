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
	IF to_regprocedure('fb_recordref_debug(regclass, timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_debug(regclass, timestamptz)';
	END IF;
END;
$$;

CREATE FUNCTION fb_summary_build_available_debug()
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_build_available_debug'
LANGUAGE C;

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_debug'
LANGUAGE C
STRICT;

DROP TABLE IF EXISTS fb_payload_scan_mode_target;
DROP TABLE IF EXISTS fb_payload_scan_mode_noise;
DROP TABLE IF EXISTS fb_payload_scan_mode_mark;

CREATE TABLE fb_payload_scan_mode_target (
	id integer PRIMARY KEY,
	payload text
);

CREATE TABLE fb_payload_scan_mode_noise (
	id bigserial PRIMARY KEY,
	payload text
);

ALTER TABLE fb_payload_scan_mode_noise
	ALTER COLUMN payload SET STORAGE EXTERNAL;

CHECKPOINT;

INSERT INTO fb_payload_scan_mode_target VALUES (1, 'before');

CREATE TABLE fb_payload_scan_mode_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_payload_scan_mode_mark VALUES (clock_timestamp());

DO $$
DECLARE
	i integer;
BEGIN
	PERFORM pg_sleep(1.1);

	FOR i IN 1..96 LOOP
		UPDATE fb_payload_scan_mode_target
		SET payload = format('after-%s', i)
		WHERE id = 1;

		INSERT INTO fb_payload_scan_mode_noise(payload)
		SELECT array_to_string(ARRAY(
			SELECT md5((i * 100000 + s)::text)
			FROM generate_series(1, 768) AS s
		), '');
	END LOOP;
END;
$$;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS built_summary;

SELECT fb_recordref_debug(
	'fb_payload_scan_mode_target'::regclass,
	(SELECT target_ts FROM fb_payload_scan_mode_mark)
) AS fragmented_summary
\gset

SELECT substring(:'fragmented_summary' FROM 'summary_span_windows=([0-9]+)')::int > 0
	   AS fragmented_summary_spans,
	   substring(:'fragmented_summary'
				 FROM 'summary_payload_locator_records=([0-9]+)')::bigint > 0
	   AS fragmented_uses_payload_locators,
	   :'fragmented_summary' LIKE '%summary_payload_locator_segments_read=%'
	   AS fragmented_tracks_locator_segments_read,
	   :'fragmented_summary' LIKE '%summary_payload_locator_public_builds=%'
	   AS fragmented_tracks_locator_public_builds,
	   :'fragmented_summary' LIKE '%payload_scan_mode=locator%'
	   AS fragmented_prefers_locator;
