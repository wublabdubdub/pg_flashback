#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config

DRY_RUN=0

if [[ "${1:-}" == "--dry-run" ]]; then
	DRY_RUN=1
	shift
fi

fb_release_gate_require_cmd "$FB_RELEASE_GATE_PSQL"
fb_release_gate_require_cmd "$FB_RELEASE_GATE_CREATEDB"
fb_release_gate_require_cmd "$FB_RELEASE_GATE_DROPDB"
fb_release_gate_prepare_output_tree

major="$(fb_release_gate_detect_pg_major)"
archive_dir="$(fb_release_gate_archive_dir_from_major "$major")"
env_sql="$FB_RELEASE_GATE_SQL_DIR/check_environment.sql"
dbs_sql="$FB_RELEASE_GATE_SQL_DIR/list_large_databases.sql"

[[ -f "$env_sql" ]] || fb_release_gate_fail "missing SQL file: $env_sql"
[[ -f "$dbs_sql" ]] || fb_release_gate_fail "missing SQL file: $dbs_sql"

mapfile -t env_rows < <(
	fb_release_gate_psql_file "$FB_RELEASE_GATE_MAINT_DB" "$env_sql" \
		-v expected_archive_dir="$archive_dir"
)

archive_mode=""
archive_command=""
for row in "${env_rows[@]}"; do
	case "$row" in
		archive_mode=*) archive_mode="${row#archive_mode=}" ;;
		archive_command=*) archive_command="${row#archive_command=}" ;;
	esac
done

[[ "$archive_mode" == "on" || "$archive_mode" == "always" ]] || \
	fb_release_gate_fail "archive_mode must be on/always, got: ${archive_mode:-<empty>}"
fb_release_gate_archive_command_matches_dir "$archive_command" "$archive_dir" || \
	fb_release_gate_fail "archive_command does not resolve to expected dir $archive_dir"

fb_release_gate_log "using archive_dir=$archive_dir"

if [[ "$DRY_RUN" -eq 0 ]]; then
	mkdir -p "$archive_dir"
	find "$archive_dir" -mindepth 1 -delete
fi

mapfile -t large_dbs < <(
	fb_release_gate_psql_file "$FB_RELEASE_GATE_MAINT_DB" "$dbs_sql" \
		-v threshold_mb="$FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB"
)

for dbname in "${large_dbs[@]}"; do
	[[ -n "$dbname" ]] || continue
	if [[ "$DRY_RUN" -eq 1 ]]; then
		fb_release_gate_log "would drop oversized database: $dbname"
	else
		fb_release_gate_log "dropping oversized database: $dbname"
		fb_release_gate_psql_sql "$FB_RELEASE_GATE_MAINT_DB" \
			"select pg_terminate_backend(pid) from pg_stat_activity where datname = '$dbname' and pid <> pg_backend_pid();"
		fb_release_gate_run_as_os_user \
			"$(printf '%q -p %q -U %q --if-exists %q' \
				"$FB_RELEASE_GATE_DROPDB" \
				"$FB_RELEASE_GATE_PGPORT" \
				"$FB_RELEASE_GATE_PGUSER" \
				"$dbname")"
	fi
done

if [[ "$DRY_RUN" -eq 1 ]]; then
	fb_release_gate_log "would recreate database: $FB_RELEASE_GATE_DBNAME"
else
	fb_release_gate_run_as_os_user \
		"$(printf '%q -p %q -U %q --if-exists %q || true' \
			"$FB_RELEASE_GATE_DROPDB" \
			"$FB_RELEASE_GATE_PGPORT" \
			"$FB_RELEASE_GATE_PGUSER" \
			"$FB_RELEASE_GATE_DBNAME")"
	fb_release_gate_run_as_os_user \
		"$(printf '%q -p %q -U %q %q' \
			"$FB_RELEASE_GATE_CREATEDB" \
			"$FB_RELEASE_GATE_PGPORT" \
			"$FB_RELEASE_GATE_PGUSER" \
			"$FB_RELEASE_GATE_DBNAME")"
	fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" \
		"CREATE EXTENSION IF NOT EXISTS pg_flashback;"
fi

env_json="$(fb_release_gate_json_path environment)"
cat > "$env_json" <<EOF
{
  "pg_major": "$major",
  "archive_dir": "$archive_dir",
  "archive_mode": "$archive_mode",
  "archive_command": $(printf '%s' "$archive_command" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),
  "dry_run": $DRY_RUN
}
EOF

fb_release_gate_log "environment summary written to $env_json"
