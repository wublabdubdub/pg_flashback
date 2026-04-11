CREATE FUNCTION pg_flashback_dml_profile(anyelement, text)
RETURNS TABLE (
	target_ts timestamptz,
	query_now_ts timestamptz,
	window_supported boolean,
	unsafe_reason text,
	total_ops bigint,
	insert_ops bigint,
	insert_pct double precision,
	update_ops bigint,
	update_pct double precision,
	delete_ops bigint,
	delete_pct double precision,
	vacuum_ops bigint,
	vacuum_pct double precision,
	other_ops bigint,
	other_pct double precision
)
AS 'MODULE_PATHNAME', 'pg_flashback_dml_profile'
LANGUAGE C;

CREATE FUNCTION pg_flashback_dml_profile_detail(anyelement, text)
RETURNS TABLE (
	target_ts timestamptz,
	query_now_ts timestamptz,
	op_kind text,
	op_group text,
	op_count bigint,
	op_pct double precision
)
AS 'MODULE_PATHNAME', 'pg_flashback_dml_profile_detail'
LANGUAGE C;

COMMENT ON FUNCTION pg_flashback_dml_profile(anyelement, text) IS
'Return exact per-table post-target replay-op summary counts and percentages for flashback planning, plus whether the flashback window is currently supported.';

COMMENT ON FUNCTION pg_flashback_dml_profile_detail(anyelement, text) IS
'Return exact per-kind post-target replay-op counts and percentages for flashback planning.';
