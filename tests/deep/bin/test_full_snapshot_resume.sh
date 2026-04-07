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

live_archive_dir="$tmp_root/live_archive"
fixture_archive_dir="$tmp_root/fixture_archive"
fake_pgwal_dir="$tmp_root/fake_pg_wal"

FB_DEEP_ROOT_DIR="$tmp_root/cleanup_root"
FB_DEEP_ARCHIVE_CLEAN_DIR="$live_archive_dir"
FB_DEEP_ARCHIVE_DIR="$live_archive_dir"
FB_DEEP_FAKE_PGWAL_DIR="$fake_pgwal_dir"
mkdir -p "$FB_DEEP_ROOT_DIR" "$live_archive_dir" "$fake_pgwal_dir"
touch "$FB_DEEP_ROOT_DIR/root.tmp" "$live_archive_dir/keep.seg" "$fake_pgwal_dir/fake.tmp"

fb_deep_cleanup_round_artifacts

[[ ! -e "$FB_DEEP_ROOT_DIR/root.tmp" ]] || {
	echo "cleanup should remove round root artifacts" >&2
	exit 1
}
[[ ! -e "$FB_DEEP_FAKE_PGWAL_DIR/fake.tmp" ]] || {
	echo "cleanup should remove fake pg_wal artifacts" >&2
	exit 1
}
[[ -f "$live_archive_dir/keep.seg" ]] || {
	echo "cleanup should preserve live archive files in full mode" >&2
	exit 1
}

FB_DEEP_ARCHIVE_DIR="$fixture_archive_dir"
mkdir -p "$fixture_archive_dir"
touch "$fixture_archive_dir/drop.seg"

fb_deep_cleanup_round_artifacts

[[ ! -e "$fixture_archive_dir/drop.seg" ]] || {
	echo "cleanup should remove non-live archive fixture files" >&2
	exit 1
}

echo "PASS: full snapshot resume helpers"
