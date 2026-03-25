#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

tmp_root="$(mktemp -d /tmp/fb_deep_snapshot_test.XXXXXX)"
trap 'rm -rf "$tmp_root"' EXIT

FB_DEEP_ROOT_DIR="$tmp_root/root"
FB_DEEP_OUTPUT_ROOT="$tmp_root/output"
FB_DEEP_SNAPSHOT_ROOT="$tmp_root/snapshots"
mkdir -p "$FB_DEEP_ROOT_DIR" "$FB_DEEP_OUTPUT_ROOT" "$FB_DEEP_SNAPSHOT_ROOT"

fake_datadir="$tmp_root/datadir"
mkdir -p "$fake_datadir/base"
printf 'baseline\n' > "$fake_datadir/base/state.txt"

fb_deep_pgdata_dir() {
	printf '%s\n' "$fake_datadir"
}

fb_deep_stop_cluster() {
	:
}

fb_deep_start_cluster() {
	:
}

fb_deep_disk_watermark_hit() {
	return 1
}

fb_deep_require_snapshot_capacity() {
	:
}

fb_deep_create_baseline_snapshot

printf 'mutated\n' > "$fake_datadir/base/state.txt"
fb_deep_restore_baseline_snapshot

restored_value="$(cat "$fake_datadir/base/state.txt")"
if [[ "$restored_value" != "baseline" ]]; then
	echo "restore failed: expected baseline, got $restored_value" >&2
	exit 1
fi

fb_deep_init_full_state
fb_deep_mark_full_batch_completed "batch_a"
fb_deep_mark_full_batch_completed "batch_c"

completed_batches="$(fb_deep_full_state_value completed_batches)"
if [[ "$completed_batches" != "batch_a,batch_c" ]]; then
	echo "unexpected completed_batches: $completed_batches" >&2
	exit 1
fi

echo "PASS: full snapshot resume helpers"
