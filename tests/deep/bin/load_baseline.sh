#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

if [[ "${1:-}" == "--full" ]]; then
	FB_DEEP_MODE=full
elif [[ "${1:-}" == "--pilot" || $# -eq 0 ]]; then
	FB_DEEP_MODE=pilot
fi

fb_deep_apply_mode

mkdir -p "$(fb_deep_output_dir baseline)"

fb_deep_psql_file "$FB_DEEP_SQL_DIR/01_schema_keyed.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR"
fb_deep_psql_file "$FB_DEEP_SQL_DIR/02_schema_bag.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR"
fb_deep_psql_file "$FB_DEEP_SQL_DIR/03_schema_truth.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR"

fb_deep_psql_file "$FB_DEEP_SQL_DIR/10_load_keyed_baseline.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"row_count=$FB_DEEP_ROW_COUNT"
fb_deep_psql_file "$FB_DEEP_SQL_DIR/11_load_bag_baseline.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"row_count=$FB_DEEP_ROW_COUNT" \
	"distinct_count=$FB_DEEP_BAG_DISTINCT"

fb_deep_psql_sql "CHECKPOINT;"

fb_deep_psql_sql "select 'keyed=' || count(*) from fb_deep_keyed_01; select 'bag=' || count(*) from fb_deep_bag_01;"
