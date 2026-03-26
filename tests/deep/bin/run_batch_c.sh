#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

FB_DEEP_MODE="${1#--}"
fb_deep_apply_mode

mkdir -p "$(fb_deep_output_dir batch_c)"
bash "$(dirname "$0")/capture_truth.sh" fb_deep_bag_01 fb_batch_c_t0 fb_truth_batch_c_bag
fb_deep_psql_file "$FB_DEEP_SQL_DIR/50_batch_c_bag_workload.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"row_count=$FB_DEEP_ROW_COUNT" \
	"distinct_count=$FB_DEEP_BAG_DISTINCT" \
	"op_distinct_count=$FB_DEEP_OPERATION_BUCKET_COUNT" \
	"insert_count=$FB_DEEP_INSERT_COUNT"
fb_deep_refresh_archive_fixture
fb_deep_psql_file "$FB_DEEP_SQL_DIR/51_batch_c_bag_validate.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"source_table=fb_deep_bag_01" \
	"truth_table=fb_truth_batch_c_bag" \
	"marker_label=fb_batch_c_t0"
