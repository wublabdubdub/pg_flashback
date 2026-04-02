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
	IF to_regprocedure('fb_summary_block_anchor_debug(regclass)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_block_anchor_debug(regclass)';
	END IF;
	IF to_regprocedure('fb_replay_debug(regclass,timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_debug(regclass, timestamptz)';
	END IF;
	IF to_regprocedure('fb_replay_apply_image_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_apply_image_contract_debug()';
	END IF;
END;
$$;

CREATE FUNCTION fb_summary_build_available_debug()
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_build_available_debug'
LANGUAGE C;

CREATE FUNCTION fb_summary_block_anchor_debug(regclass)
RETURNS integer
AS '$libdir/pg_flashback', 'fb_summary_block_anchor_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_apply_image_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_apply_image_contract_debug'
LANGUAGE C;

SELECT fb_replay_apply_image_contract_debug() AS apply_image_contract
\gset

SELECT :'apply_image_contract' LIKE '%preserve_existing=true%' AS preserve_existing,
	   :'apply_image_contract' LIKE '%materialize_requires_apply=true%' AS materialize_requires_apply;

DROP TABLE IF EXISTS fb_replay_target;
DROP TABLE IF EXISTS fb_replay_mark;

CREATE TABLE fb_replay_target (
	id integer PRIMARY KEY,
	payload text
);

CHECKPOINT;

INSERT INTO fb_replay_target VALUES (0, 'seed');

CREATE TABLE fb_replay_mark (
	target_ts timestamptz NOT NULL
);

INSERT INTO fb_replay_mark VALUES (clock_timestamp());

INSERT INTO fb_replay_target VALUES (1, 'alpha');
UPDATE fb_replay_target SET payload = 'alpha-updated' WHERE id = 1;
DELETE FROM fb_replay_target WHERE id = 0;

DO $$
BEGIN
	PERFORM pg_switch_wal();
	PERFORM pg_switch_wal();
END;
$$;

SELECT fb_summary_build_available_debug() > 0 AS built_summary;

SELECT fb_summary_block_anchor_debug('fb_replay_target'::regclass) > 0
	   AS block_anchor_summary_present;

SELECT substring(
		   fb_replay_debug(
			   'fb_replay_target'::regclass,
			   (SELECT target_ts FROM fb_replay_mark)
		   )
		   FROM 'precomputed_missing_blocks=([0-9]+)')::int = 0
	   AS avoids_precomputed_missing_blocks,
	   substring(
		   fb_replay_debug(
			   'fb_replay_target'::regclass,
			   (SELECT target_ts FROM fb_replay_mark)
		   )
		   FROM 'discover_rounds=([0-9]+)')::int = 0
	   AS skips_discover_rounds;

SELECT *
FROM pg_flashback(
	NULL::public.fb_replay_target,
	(SELECT target_ts::text FROM fb_replay_mark)
)
ORDER BY id;
