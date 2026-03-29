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

CREATE FUNCTION fb_pg_flashback_support(internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'fb_pg_flashback_support'
LANGUAGE C;

CREATE FUNCTION pg_flashback_to(regclass, text)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_flashback_rewind'
LANGUAGE C
STRICT;

CREATE FUNCTION pg_flashback(anyelement, text)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'pg_flashback'
LANGUAGE C
SUPPORT fb_pg_flashback_support;

COMMENT ON FUNCTION fb_version() IS
'Return current pg_flashback development version.';

COMMENT ON FUNCTION fb_check_relation(regclass) IS
'Inspect current scaffold mode selection for a relation.';

COMMENT ON FUNCTION pg_flashback_to(regclass, text) IS
'Rewind the keyed source table itself back to the target timestamp by applying batched UPDATE/INSERT/DELETE operations in place.';

COMMENT ON FUNCTION pg_flashback(anyelement, text) IS
'Return flashback rows directly for NULL::schema.table and target timestamp text, without result-table materialization or AS t(...).';
