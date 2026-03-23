#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

FB_DEEP_MODE="${1#--}"
fb_deep_apply_mode

mkdir -p "$(fb_deep_output_dir batch_a)"
bash "$(dirname "$0")/capture_truth.sh" fb_deep_keyed_01 fb_batch_a_t0 fb_truth_batch_a_keyed
fb_deep_psql_file "$FB_DEEP_SQL_DIR/30_batch_a_keyed_workload.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"row_count=$FB_DEEP_ROW_COUNT" \
	"insert_count=$FB_DEEP_INSERT_COUNT"
fb_deep_refresh_archive_fixture
fb_deep_psql_file "$FB_DEEP_SQL_DIR/31_batch_a_keyed_validate.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"source_table=fb_deep_keyed_01" \
	"truth_table=fb_truth_batch_a_keyed" \
	"result_table=fb_result_batch_a_keyed" \
	"marker_label=fb_batch_a_t0"
