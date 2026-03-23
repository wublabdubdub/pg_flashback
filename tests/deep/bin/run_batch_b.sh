#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

FB_DEEP_MODE="${1#--}"
fb_deep_apply_mode

mkdir -p "$(fb_deep_output_dir batch_b)"
bash "$(dirname "$0")/capture_truth.sh" fb_deep_keyed_01 fb_batch_b_t0 fb_truth_batch_b_keyed
fb_deep_psql_file "$FB_DEEP_SQL_DIR/40_batch_b_txn_workload.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"row_count=$FB_DEEP_ROW_COUNT"
fb_deep_refresh_archive_fixture
fb_deep_psql_file "$FB_DEEP_SQL_DIR/41_batch_b_txn_validate.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"source_table=fb_deep_keyed_01" \
	"truth_table=fb_truth_batch_b_keyed" \
	"result_table=fb_result_batch_b_keyed" \
	"marker_label=fb_batch_b_t0"
