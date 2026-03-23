#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

if [[ $# -ne 3 ]]; then
	echo "usage: $0 <source_table> <marker_label> <truth_table>" >&2
	exit 1
fi

SOURCE_TABLE="$1"
MARKER_LABEL="$2"
TRUTH_TABLE="$3"

fb_deep_psql_sql "CHECKPOINT;"
fb_deep_psql_file "$FB_DEEP_SQL_DIR/20_capture_truth.sql" \
	"archive_dest=$FB_DEEP_ARCHIVE_DIR" \
	"ckwal_dir=$FB_DEEP_CKWAL_DIR" \
	"source_table=$SOURCE_TABLE" \
	"marker_label=$MARKER_LABEL" \
	"truth_table=$TRUTH_TABLE"
