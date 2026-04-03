DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
		EXECUTE 'DROP EXTENSION pg_flashback CASCADE';
	END IF;
END;
$$;
CREATE EXTENSION pg_flashback;
SET pg_flashback.parallel_workers = 0;
SET pg_flashback.show_progress = on;

DO $$
BEGIN
	IF to_regprocedure('fb_progress_debug_set_clock(bigint[])') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_progress_debug_set_clock(bigint[])';
	END IF;
	IF to_regprocedure('fb_progress_debug_reset_clock()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_progress_debug_reset_clock()';
	END IF;
	IF to_regprocedure('fb_recordref_missing_spool_debug(regclass, timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_missing_spool_debug(regclass, timestamptz)';
	END IF;
	IF to_regprocedure('fb_wal_error_surface_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_wal_error_surface_debug()';
	END IF;
	IF to_regprocedure('fb_wal_payload_window_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_wal_payload_window_contract_debug()';
	END IF;
END;
$$;

CREATE FUNCTION fb_progress_debug_set_clock(bigint[])
RETURNS void
AS '$libdir/pg_flashback', 'fb_progress_debug_set_clock'
LANGUAGE C STRICT;

CREATE FUNCTION fb_progress_debug_reset_clock()
RETURNS void
AS '$libdir/pg_flashback', 'fb_progress_debug_reset_clock'
LANGUAGE C;

CREATE FUNCTION fb_recordref_missing_spool_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_missing_spool_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_wal_error_surface_debug()
RETURNS TABLE (
	message_ok boolean,
	detail_mentions_first_retained boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
	got_message text;
	got_detail text;
	expected_seg text;
BEGIN
	SELECT seg_name
	INTO expected_seg
	FROM fb_wal_error_surface_fixture
	ORDER BY segno
	LIMIT 1;

	BEGIN
		PERFORM count(*)
		FROM pg_flashback(
			NULL::public.fb_wal_error_surface_target,
			'2000-01-01 00:00:00+00'
		);
	EXCEPTION
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS
				got_message = MESSAGE_TEXT,
				got_detail = PG_EXCEPTION_DETAIL;

			message_ok :=
				got_message = 'WAL not complete: target timestamp predates retained continuous WAL suffix';
			detail_mentions_first_retained :=
				position(expected_seg IN coalesce(got_detail, '')) > 0;
			RETURN NEXT;
			RETURN;
	END;

	RAISE EXCEPTION 'expected WAL incomplete error';
END;
$$;

CREATE FUNCTION fb_wal_payload_window_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_wal_payload_window_contract_debug'
LANGUAGE C STRICT;

CREATE TABLE fb_wal_error_surface_target (
	id integer PRIMARY KEY,
	payload integer
);

CREATE TEMP TABLE fb_wal_error_surface_fixture (
	seg_name text NOT NULL,
	segno bigint NOT NULL
);

DO $$
DECLARE
	data_dir text := current_setting('data_directory');
	archive_dir text := '/tmp/fb_wal_error_surface_archive';
	pgwal_dir text := '/tmp/fb_wal_error_surface_pgwal';
	rec record;
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();

	EXECUTE format(
		'COPY (SELECT '''') TO PROGRAM %L',
		'rm -rf ' || archive_dir || ' ' || pgwal_dir ||
		' && mkdir -p ' || archive_dir || ' ' || pgwal_dir ||
		' && rm -f ' || data_dir || '/pg_flashback/recovered_wal/*');

	INSERT INTO fb_wal_error_surface_fixture(seg_name, segno)
	WITH wal AS (
		SELECT name,
			   (('x' || substr(name, 9, 8))::bit(32)::bigint *
				(4294967296::bigint / pg_size_bytes(current_setting('wal_segment_size'))) +
				('x' || substr(name, 17, 8))::bit(32)::bigint) AS segno,
			   lead(
				   (('x' || substr(name, 9, 8))::bit(32)::bigint *
					(4294967296::bigint / pg_size_bytes(current_setting('wal_segment_size'))) +
					('x' || substr(name, 17, 8))::bit(32)::bigint),
				   1
			   ) OVER (ORDER BY name) AS next_segno_1,
			   lead(
				   (('x' || substr(name, 9, 8))::bit(32)::bigint *
					(4294967296::bigint / pg_size_bytes(current_setting('wal_segment_size'))) +
					('x' || substr(name, 17, 8))::bit(32)::bigint),
				   2
			   ) OVER (ORDER BY name) AS next_segno_2
		FROM pg_ls_dir(data_dir || '/pg_wal') AS d(name)
		WHERE d.name ~ '^[0-9A-F]{24}$'
	),
	pick AS (
		SELECT segno AS start_segno
		FROM wal
		WHERE next_segno_1 = segno + 1
		  AND next_segno_2 = segno + 2
		ORDER BY segno DESC
		LIMIT 1
	)
	SELECT wal.name, wal.segno
	FROM wal, pick
	WHERE wal.segno IN (pick.start_segno, pick.start_segno + 1, pick.start_segno + 2)
	ORDER BY wal.segno;

	IF (SELECT count(*) FROM fb_wal_error_surface_fixture) <> 3 THEN
		RAISE EXCEPTION 'failed to prepare wal error-surface fixture';
	END IF;

	FOR rec IN
		SELECT seg_name
		FROM fb_wal_error_surface_fixture
		ORDER BY segno
	LOOP
		EXECUTE format(
			'COPY (SELECT '''') TO PROGRAM %L',
			'cp ' || quote_literal(data_dir || '/pg_wal/' || rec.seg_name) || ' ' ||
			quote_literal(archive_dir || '/' || rec.seg_name));
	END LOOP;
END;
$$;

SET pg_flashback.archive_dest = '/tmp/fb_wal_error_surface_archive';
SET pg_flashback.debug_pg_wal_dir = '/tmp/fb_wal_error_surface_pgwal';

SELECT fb_progress_debug_set_clock(ARRAY[
	0,
	1000,
	3000
]::bigint[]);

SELECT *
FROM fb_wal_error_surface_debug();

SELECT fb_wal_payload_window_contract_debug();

SELECT fb_progress_debug_reset_clock();

RESET pg_flashback.archive_dest;
RESET pg_flashback.debug_pg_wal_dir;
DO $$
BEGIN
	EXECUTE format('SET pg_flashback.archive_dir = %L', current_setting('data_directory') || '/pg_wal');
END;
$$;
SET pg_flashback.show_progress = off;

CHECKPOINT;
INSERT INTO fb_wal_error_surface_target VALUES (1, 10);

CREATE TABLE fb_wal_error_surface_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_wal_error_surface_mark VALUES (clock_timestamp());
UPDATE fb_wal_error_surface_target
SET payload = 11
WHERE id = 1;

SELECT fb_recordref_missing_spool_debug(
	'fb_wal_error_surface_target'::regclass,
	(SELECT target_ts FROM fb_wal_error_surface_mark)
);
