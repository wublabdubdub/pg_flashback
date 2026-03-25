#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

FB_DEEP_MODE="${1#--}"
fb_deep_apply_mode

mkdir -p "$(fb_deep_output_dir batch_e)"
bash "$(dirname "$0")/capture_truth.sh" fb_deep_keyed_01 fb_batch_e_t0 fb_truth_batch_e_keyed
fb_deep_psql_file "$FB_DEEP_SQL_DIR/70_batch_e_stress_workload.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"row_count=$FB_DEEP_ROW_COUNT" \
	"op_row_count=$FB_DEEP_OPERATION_ROW_COUNT" \
	"insert_count=$FB_DEEP_STRESS_INSERT_COUNT" \
	"stress_rounds=$FB_DEEP_STRESS_ROUNDS"
fb_deep_refresh_archive_fixture
fb_deep_psql_file "$FB_DEEP_SQL_DIR/71_batch_e_stress_validate.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"source_table=fb_deep_keyed_01" \
	"truth_table=fb_truth_batch_e_keyed" \
	"result_table=fb_result_batch_e_keyed" \
	"marker_label=fb_batch_e_t0"
