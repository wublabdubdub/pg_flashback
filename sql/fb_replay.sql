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
	IF to_regprocedure('fb_recordref_debug(regclass,timestamptz)') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_recordref_debug(regclass, timestamptz)';
	END IF;
	IF to_regprocedure('fb_replay_apply_image_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_apply_image_contract_debug()';
	END IF;
	IF to_regprocedure('fb_replay_nonapply_image_missing_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_nonapply_image_missing_contract_debug()';
	END IF;
	IF to_regprocedure('fb_replay_heap_update_same_block_init_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_heap_update_same_block_init_contract_debug()';
	END IF;
	IF to_regprocedure('fb_replay_heap_update_block_id_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_heap_update_block_id_contract_debug()';
	END IF;
	IF to_regprocedure('fb_wal_nonapply_image_spool_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_wal_nonapply_image_spool_contract_debug()';
	END IF;
	IF to_regprocedure('fb_wal_hint_fpi_payload_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_wal_hint_fpi_payload_contract_debug()';
	END IF;
	IF to_regprocedure('fb_wal_heap2_visible_payload_contract_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_wal_heap2_visible_payload_contract_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_image_short_circuit_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_image_short_circuit_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_image_preserve_next_insert_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_image_preserve_next_insert_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_image_preserve_dead_old_tuple_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_image_preserve_dead_old_tuple_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_image_reject_used_insert_slot_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_image_reject_used_insert_slot_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_compose_future_constraints_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_compose_future_constraints_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_lookahead_snapshot_isolation_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_lookahead_snapshot_isolation_debug()';
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

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS '$libdir/pg_flashback', 'fb_recordref_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_replay_apply_image_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_apply_image_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_nonapply_image_missing_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_nonapply_image_missing_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_heap_update_same_block_init_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_heap_update_same_block_init_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_heap_update_block_id_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_heap_update_block_id_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_wal_nonapply_image_spool_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_wal_nonapply_image_spool_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_wal_hint_fpi_payload_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_wal_hint_fpi_payload_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_wal_heap2_visible_payload_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_wal_heap2_visible_payload_contract_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_image_short_circuit_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_image_short_circuit_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_image_preserve_next_insert_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_image_preserve_next_insert_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_image_preserve_dead_old_tuple_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_image_preserve_dead_old_tuple_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_image_reject_used_insert_slot_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_image_reject_used_insert_slot_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_compose_future_constraints_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_compose_future_constraints_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_lookahead_snapshot_isolation_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_lookahead_snapshot_isolation_debug'
LANGUAGE C;

SELECT fb_replay_apply_image_contract_debug() AS apply_image_contract
\gset

SELECT :'apply_image_contract' LIKE '%preserve_existing=false%' AS image_replaces_existing,
	   :'apply_image_contract' LIKE '%materialize_requires_apply=false%' AS materialize_ignores_apply_flag;

SELECT fb_replay_nonapply_image_missing_contract_debug() AS nonapply_image_missing_contract
\gset

SELECT :'nonapply_image_missing_contract' LIKE '%ready=false%' AS nonapply_image_not_ready,
	   :'nonapply_image_missing_contract' LIKE '%notes_missing=true%' AS nonapply_image_notes_missing,
	   :'nonapply_image_missing_contract' LIKE '%initialized=false%' AS nonapply_image_not_initialized,
	   :'nonapply_image_missing_contract' LIKE '%anchor_requires_apply=true%' AS anchor_requires_apply;

SELECT fb_replay_heap_update_same_block_init_contract_debug() AS same_block_update_init_contract
\gset

SELECT :'same_block_update_init_contract' LIKE '%same_block_update_allow_init=true%' AS same_block_update_allows_init,
	   :'same_block_update_init_contract' LIKE '%same_block_update_ready=true%' AS same_block_update_init_ready;

SELECT fb_replay_heap_update_block_id_contract_debug() AS heap_update_block_id_contract
\gset

SELECT :'heap_update_block_id_contract' LIKE '%heap_update_block_id_contract=true%' AS heap_update_uses_wal_block_id;

SELECT fb_wal_nonapply_image_spool_contract_debug() AS nonapply_image_spool_contract
\gset

SELECT :'nonapply_image_spool_contract' LIKE '%stored_has_image=false%' AS nonapply_image_not_stored,
	   :'nonapply_image_spool_contract' LIKE '%materializes=false%' AS nonapply_image_does_not_materialize;

SELECT fb_wal_hint_fpi_payload_contract_debug() AS hint_fpi_payload_contract
\gset

SELECT :'hint_fpi_payload_contract' LIKE '%hint_fpi_payload_enabled=false%' AS hint_fpi_not_in_payload;

SELECT fb_wal_heap2_visible_payload_contract_debug() AS heap2_visible_payload_contract
\gset

SELECT :'heap2_visible_payload_contract' LIKE '%heap2_visible_payload_enabled=false%' AS heap2_visible_not_in_payload;

SELECT fb_replay_prune_image_short_circuit_debug() AS prune_image_contract
\gset

SELECT :'prune_image_contract' LIKE '%prune_image_short_circuit=true%' AS prune_image_short_circuit;

SELECT fb_replay_prune_image_preserve_next_insert_debug() AS prune_image_preserve_contract
\gset

SELECT :'prune_image_preserve_contract' LIKE '%prune_image_preserve_next_insert=true%' AS prune_image_preserve_next_insert;

SELECT fb_replay_prune_image_preserve_dead_old_tuple_debug() AS prune_image_dead_old_tuple_contract
\gset

SELECT :'prune_image_dead_old_tuple_contract' LIKE '%prune_image_preserve_dead_old_tuple=true%' AS prune_image_preserve_dead_old_tuple;

SELECT fb_replay_prune_image_reject_used_insert_slot_debug() AS prune_image_used_insert_slot_contract
\gset

SELECT :'prune_image_used_insert_slot_contract' LIKE '%prune_image_reject_used_insert_slot=true%' AS prune_image_reject_used_insert_slot;

SELECT fb_replay_prune_compose_future_constraints_debug() AS prune_compose_future_constraints_contract
\gset

SELECT :'prune_compose_future_constraints_contract' LIKE '%prune_compose_future_constraints=true%' AS prune_compose_future_constraints;

SELECT fb_replay_prune_lookahead_snapshot_isolation_debug() AS prune_lookahead_snapshot_isolation_contract
\gset

SELECT :'prune_lookahead_snapshot_isolation_contract' LIKE '%prune_lookahead_snapshot_isolated=true%' AS prune_lookahead_snapshot_isolated;

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

SELECT fb_summary_block_anchor_debug('fb_replay_target'::regclass) >= 0
	   AS block_anchor_debug_callable;

DO $$
DECLARE
	summary text;
	tries integer;
BEGIN
	FOR tries IN 1..50 LOOP
		SELECT fb_recordref_debug(
				   'fb_replay_target'::regclass,
				   (SELECT target_ts FROM fb_replay_mark)
			   )
		  INTO summary;
		EXIT WHEN coalesce(
			substring(summary FROM 'summary_payload_locator_records=([0-9]+)')::int,
			0) > 0;
		PERFORM pg_sleep(0.1);
	END LOOP;
END;
$$;

SET pg_flashback.parallel_workers = 2;

SELECT fb_recordref_debug(
		   'fb_replay_target'::regclass,
		   (SELECT target_ts FROM fb_replay_mark)
	   ) AS replay_recordref_summary
\gset

SELECT substring(:'replay_recordref_summary'
				 FROM 'summary_payload_locator_records=([0-9]+)')::bigint > 0
	   AS replay_uses_payload_locators,
	   :'replay_recordref_summary' LIKE '%payload_scan_mode=locator%'
	   AS replay_prefers_locator_mode;

SELECT fb_replay_debug(
		   'fb_replay_target'::regclass,
		   (SELECT target_ts FROM fb_replay_mark)
	   ) AS replay_debug_summary
\gset

SELECT substring(
		   :'replay_debug_summary'
		   FROM 'precomputed_missing_blocks=([0-9]+)')::int = 0
	   AS avoids_precomputed_missing_blocks,
	   substring(
		   :'replay_debug_summary'
		   FROM 'discover_rounds=([0-9]+)')::int = 0
	   AS skips_discover_rounds,
	   substring(
		   :'replay_debug_summary'
		   FROM 'record_materializer_resets=([0-9]+)')::bigint >= 1
	   AS tracks_record_materializer_resets,
	   substring(
		   :'replay_debug_summary'
		   FROM 'record_materializer_reuses=([0-9]+)')::bigint > 0
	   AS reuses_record_materializer,
	   substring(
		   :'replay_debug_summary'
		   FROM 'locator_stub_materializations=([0-9]+)')::bigint > 0
	   AS materializes_locator_stubs;

SET pg_flashback.parallel_workers = 0;

SELECT *
FROM pg_flashback(
	NULL::public.fb_replay_target,
	(SELECT target_ts::text FROM fb_replay_mark)
)
ORDER BY id;

\echo fb_replay_end
