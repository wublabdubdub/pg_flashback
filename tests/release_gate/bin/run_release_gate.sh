#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config

DRY_RUN=0

if [[ "${1:-}" == "--dry-run" ]]; then
	DRY_RUN=1
	shift
fi

cleanup() {
	local major archive_dir

	if ! major="$(fb_release_gate_detect_pg_major 2>/dev/null)"; then
		return 0
	fi
	archive_dir="$(fb_release_gate_archive_dir_from_major "$major")"
	if [[ -d "$archive_dir" ]]; then
		find "$archive_dir" -mindepth 1 -delete 2>/dev/null || true
	fi
}

trap cleanup EXIT

fb_release_gate_require_cmd "$FB_RELEASE_GATE_PSQL"
fb_release_gate_prepare_output_tree
fb_release_gate_log "starting release gate output_dir=$FB_RELEASE_GATE_OUTPUT_DIR dry_run=$DRY_RUN"

if [[ "$DRY_RUN" -eq 1 ]]; then
	"$FB_RELEASE_GATE_BIN_DIR/prepare_empty_instance.sh" --dry-run
else
	"$FB_RELEASE_GATE_BIN_DIR/prepare_empty_instance.sh"
fi

fb_release_gate_log "release gate skeleton finished after environment preparation"
