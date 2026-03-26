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

CREATE TABLE fb_storage_boundary_target (
	id integer PRIMARY KEY,
	v_int integer NOT NULL,
	payload text NOT NULL
);

CREATE TABLE fb_storage_boundary_noise (
	id bigserial PRIMARY KEY,
	payload text NOT NULL
);

INSERT INTO fb_storage_boundary_target
SELECT g,
	   g,
	   repeat('seed', 16)
  FROM generate_series(1, 1000) AS g;

CHECKPOINT;

CREATE TABLE fb_storage_boundary_mark (
	target_ts timestamptz NOT NULL,
	base_sum bigint NOT NULL
);

INSERT INTO fb_storage_boundary_mark
SELECT clock_timestamp(), sum(v_int)::bigint
FROM fb_storage_boundary_target;

DO $$
DECLARE
	i integer;
BEGIN
	FOR i IN 1..6 LOOP
		INSERT INTO fb_storage_boundary_noise (payload)
		SELECT repeat(md5((i * 100000 + g)::text), 128)
		FROM generate_series(1, 256) AS g;
		PERFORM pg_switch_wal();
	END LOOP;
END;
$$;

SELECT count(*) AS result_count,
	   sum(v_int)::bigint AS result_sum
FROM pg_flashback(
	NULL::public.fb_storage_boundary_target,
	(SELECT target_ts::text FROM fb_storage_boundary_mark)
);

SELECT base_sum
FROM fb_storage_boundary_mark;
