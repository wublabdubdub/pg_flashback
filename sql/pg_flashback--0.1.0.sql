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

CREATE FUNCTION fb_runtime_dir_debug()
RETURNS text
AS 'MODULE_PATHNAME', 'fb_runtime_dir_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_scan_wal_debug(regclass, timestamptz)
RETURNS text
AS 'MODULE_PATHNAME', 'fb_scan_wal_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_recordref_debug(regclass, timestamptz)
RETURNS text
AS 'MODULE_PATHNAME', 'fb_recordref_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_replay_debug(regclass, timestamptz)
RETURNS text
AS 'MODULE_PATHNAME', 'fb_replay_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_wal_window_debug(timestamptz)
RETURNS text
AS 'MODULE_PATHNAME', 'fb_wal_window_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_decode_insert_debug(regclass, timestamptz)
RETURNS SETOF text
AS 'MODULE_PATHNAME', 'fb_decode_insert_debug'
LANGUAGE C
STRICT;

CREATE FUNCTION pg_flashback(regclass, timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_flashback'
LANGUAGE C
STRICT;

CREATE FUNCTION fb_flashback_materialize(regclass, timestamptz, text DEFAULT 'fb_flashback_result')
RETURNS text
LANGUAGE plpgsql
STRICT
AS $$
DECLARE
    coldef text;
BEGIN
    SELECT string_agg(
               format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)),
               ', ' ORDER BY a.attnum)
      INTO coldef
      FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = $1
       AND a.attnum > 0
       AND NOT a.attisdropped;

    IF coldef IS NULL THEN
        RAISE EXCEPTION 'could not derive row type for relation %', $1;
    END IF;

    IF to_regclass(format('pg_temp.%I', $3)) IS NOT NULL THEN
        EXECUTE format('DROP TABLE pg_temp.%I', $3);
    END IF;
    EXECUTE format(
        'CREATE TEMP TABLE pg_temp.%I AS SELECT * FROM pg_flashback($1, $2) AS t(%s)',
        $3,
        coldef)
    USING $1, $2;

    RETURN $3;
END;
$$;

CREATE FUNCTION fb_create_flashback_table(text, text, text)
RETURNS text
LANGUAGE plpgsql
STRICT
AS $$
DECLARE
    result_name alias for $1;
    source_name alias for $2;
    target_ts_text alias for $3;
    source_relid regclass;
    target_ts timestamptz;
    coldef text;
    window_info text;
BEGIN
    IF to_regclass(result_name) IS NOT NULL THEN
        RAISE EXCEPTION 'flashback target relation "%" already exists', result_name;
    END IF;

    source_relid := to_regclass(source_name);
    IF source_relid IS NULL THEN
        RAISE EXCEPTION 'source relation "%" does not exist', source_name;
    END IF;

    target_ts := target_ts_text::timestamptz;

    SELECT string_agg(
               format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)),
               ', ' ORDER BY a.attnum)
      INTO coldef
      FROM pg_catalog.pg_attribute a
     WHERE a.attrelid = source_relid
       AND a.attnum > 0
       AND NOT a.attisdropped;

    IF coldef IS NULL THEN
        RAISE EXCEPTION 'could not derive row type for relation %', source_relid;
    END IF;

    window_info := fb_wal_window_debug(target_ts);
    EXECUTE format(
        'CREATE TEMP TABLE %I AS SELECT * FROM pg_flashback($1, $2) AS t(%s)',
        result_name,
        coldef)
    USING source_relid, target_ts;

    RAISE NOTICE 'flashback info: %', window_info;

    RETURN result_name;
END;
$$;

CREATE FUNCTION fb_export_undo(regclass, timestamptz)
RETURNS SETOF text
AS 'MODULE_PATHNAME', 'fb_export_undo'
LANGUAGE C
STRICT;

COMMENT ON FUNCTION fb_version() IS
'Return current pg_flashback development version.';

COMMENT ON FUNCTION fb_check_relation(regclass) IS
'Inspect current scaffold mode selection for a relation.';

COMMENT ON FUNCTION fb_runtime_dir_debug() IS
'Ensure the pg_flashback private runtime directories exist and return a compact status summary.';

COMMENT ON FUNCTION fb_scan_wal_debug(regclass, timestamptz) IS
'Development-only WAL scan summary for the current PG18 scaffold.';

COMMENT ON FUNCTION fb_recordref_debug(regclass, timestamptz) IS
'Development-only RecordRef index summary for the current PG18 scaffold.';

COMMENT ON FUNCTION fb_replay_debug(regclass, timestamptz) IS
'Development-only page replay summary for the current PG18 scaffold.';

COMMENT ON FUNCTION fb_wal_window_debug(timestamptz) IS
'Development-only WAL window summary for the current PG18 scaffold.';

COMMENT ON FUNCTION fb_decode_insert_debug(regclass, timestamptz) IS
'Development-only placeholder for future replay-backed INSERT row-image output.';

COMMENT ON FUNCTION pg_flashback(regclass, timestamptz) IS
'Return historical result set for a relation at target timestamp.';

COMMENT ON FUNCTION fb_flashback_materialize(regclass, timestamptz, text) IS
'Create a temp table with dictionary-derived columns from pg_flashback(), avoiding manual AS t(...) column lists.';

COMMENT ON FUNCTION fb_create_flashback_table(text, text, text) IS
'Create a temp flashback result table from plain text table name and timestamp text, without requiring casts or AS t(...).';

COMMENT ON FUNCTION fb_export_undo(regclass, timestamptz) IS
'Export undo SQL or reverse-op log for a relation and target timestamp.';
