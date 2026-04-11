CREATE FUNCTION pg_flashback_debug_unresolv_xid(regclass, timestamptz)
RETURNS TABLE (
	xid bigint,
	xid_role text,
	resolved_by text,
	fallback_reason text,
	top_xid bigint,
	commit_ts timestamptz,
	summary_missing_segments integer,
	fallback_windows integer,
	diag text
)
AS 'MODULE_PATHNAME', 'pg_flashback_debug_unresolv_xid'
LANGUAGE C
STRICT;

COMMENT ON FUNCTION pg_flashback_debug_unresolv_xid(regclass, timestamptz) IS
'Show xid-level fallback and unresolved diagnostics for a flashback target, so copied logs and WAL files can be analyzed offline.';
