#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

DRY_RUN=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dry-run)
			DRY_RUN=1
			shift
			;;
		--pilot)
			FB_DEEP_MODE=pilot
			shift
			;;
		--full)
			FB_DEEP_MODE=full
			shift
			;;
		*)
			echo "unknown option: $1" >&2
			exit 1
			;;
	esac
done

fb_deep_apply_mode

if [[ "$DRY_RUN" -eq 1 ]]; then
	fb_deep_log "dry-run"
	fb_deep_log "database=${FB_DEEP_DBNAME}"
	fb_deep_log "mode=${FB_DEEP_MODE}"
	fb_deep_log "archive_dir=${FB_DEEP_ARCHIVE_DIR}"
	fb_deep_log "ckwal_dir=${FB_DEEP_CKWAL_DIR}"
	fb_deep_log "fake_pg_wal_dir=${FB_DEEP_FAKE_PGWAL_DIR}"
	fb_deep_log "row_count=${FB_DEEP_ROW_COUNT}"
	fb_deep_log "insert_count=${FB_DEEP_INSERT_COUNT}"
	exit 0
fi

mkdir -p "$FB_DEEP_ARCHIVE_DIR" "$FB_DEEP_CKWAL_DIR" "$FB_DEEP_FAKE_PGWAL_DIR" \
	"$(fb_deep_output_dir bootstrap)"

if fb_deep_db_exists; then
	fb_deep_log "database already exists: ${FB_DEEP_DBNAME}"
else
	local_cmd=""
	printf -v local_cmd 'PGPORT=%q %q %q' \
		"$FB_DEEP_PGPORT" "$FB_DEEP_CREATEDB" "$FB_DEEP_DBNAME"
	fb_deep_as_18pg "$local_cmd"
	fb_deep_log "created database: ${FB_DEEP_DBNAME}"
fi

fb_deep_psql_sql "CREATE EXTENSION IF NOT EXISTS pg_flashback;"
fb_deep_psql_sql "ALTER DATABASE ${FB_DEEP_DBNAME} SET pg_flashback.memory_limit_kb = '${FB_DEEP_MEMORY_LIMIT_KB}';"
fb_deep_write_env_file
fb_deep_log "environment ready"
