CREATE FUNCTION fb_version()
RETURNS text
AS 'MODULE_PATHNAME', 'fb_version'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_check_relation(regclass)
RETURNS text
AS 'MODULE_PATHNAME', 'fb_check_relation'
LANGUAGE C
STRICT;

CREATE FUNCTION pg_flashback(text, text, text)
RETURNS text
AS 'MODULE_PATHNAME', 'pg_flashback'
LANGUAGE C
STRICT;

COMMENT ON FUNCTION fb_version() IS
'Return current pg_flashback extension version.';

COMMENT ON FUNCTION fb_check_relation(regclass) IS
'Inspect current scaffold mode selection for a relation.';

COMMENT ON FUNCTION pg_flashback(text, text, text) IS
'Create a temp flashback result table from plain text table name and timestamp text, without requiring casts or AS t(...).';
