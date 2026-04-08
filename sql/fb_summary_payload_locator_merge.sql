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
	IF to_regprocedure('fb_summary_payload_locator_merge_debug(integer[])') IS NOT NULL THEN
		EXECUTE 'DROP FUNCTION fb_summary_payload_locator_merge_debug(integer[])';
	END IF;
END;
$$;

CREATE FUNCTION fb_summary_payload_locator_merge_debug(integer[])
RETURNS text
AS '$libdir/pg_flashback', 'fb_summary_payload_locator_merge_debug'
LANGUAGE C
STRICT;

SELECT fb_summary_payload_locator_merge_debug(ARRAY[2, 1, 0]);
SELECT fb_summary_payload_locator_merge_debug(ARRAY[1, 0, 2, 0]);
