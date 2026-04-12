CREATE FUNCTION fb_summary_payload_locator_merge_contract_debug()
RETURNS text
AS '$libdir/pg_flashback', 'fb_summary_payload_locator_merge_contract_debug'
LANGUAGE C STRICT;

SELECT fb_summary_payload_locator_merge_contract_debug();

DROP FUNCTION fb_summary_payload_locator_merge_contract_debug();
