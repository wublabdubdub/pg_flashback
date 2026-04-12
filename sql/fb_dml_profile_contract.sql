SET client_min_messages = warning;
CREATE EXTENSION IF NOT EXISTS pg_flashback;
RESET client_min_messages;
SET pg_flashback.show_progress = off;

CREATE FUNCTION fb_dml_profile_counter_reset_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_dml_profile_counter_reset_contract_debug'
LANGUAGE C STRICT;

SELECT fb_dml_profile_counter_reset_contract_debug();

DROP FUNCTION fb_dml_profile_counter_reset_contract_debug();
