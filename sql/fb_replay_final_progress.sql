CREATE EXTENSION pg_flashback;
SET pg_flashback.show_progress = off;

CREATE FUNCTION fb_replay_final_progress_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_replay_final_progress_contract_debug'
LANGUAGE C STRICT;

SELECT fb_replay_final_progress_contract_debug();

DROP FUNCTION fb_replay_final_progress_contract_debug();
