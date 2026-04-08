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
	IF to_regprocedure('fb_replay_prune_image_preserve_next_multi_insert_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_image_preserve_next_multi_insert_debug()';
	END IF;
	IF to_regprocedure('fb_replay_prune_image_reject_future_warm_state_debug()') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_replay_prune_image_reject_future_warm_state_debug()';
	END IF;
END;
$$;

CREATE FUNCTION fb_replay_prune_image_preserve_next_multi_insert_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_image_preserve_next_multi_insert_debug'
LANGUAGE C;

CREATE FUNCTION fb_replay_prune_image_reject_future_warm_state_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_prune_image_reject_future_warm_state_debug'
LANGUAGE C;

SELECT fb_replay_prune_image_preserve_next_multi_insert_debug() AS prune_image_future_multi_insert_contract
\gset

SELECT :'prune_image_future_multi_insert_contract' LIKE '%prune_image_preserve_next_multi_insert=true%' AS prune_image_preserve_next_multi_insert;

SELECT fb_replay_prune_image_reject_future_warm_state_debug() AS prune_image_future_warm_state_contract
\gset

SELECT :'prune_image_future_warm_state_contract' LIKE '%prune_image_reject_future_warm_state=true%' AS prune_image_reject_future_warm_state;

SELECT 'fb_replay_prune_future_state_done' AS end_marker;

SET client_min_messages = warning;
