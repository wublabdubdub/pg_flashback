CREATE OR REPLACE FUNCTION fb_version()
RETURNS text
AS 'MODULE_PATHNAME', 'fb_version'
LANGUAGE C
STRICT;

COMMENT ON FUNCTION fb_version() IS
'Return current pg_flashback extension version.';
