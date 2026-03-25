#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

MODE="pilot"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--pilot)
			MODE="pilot"
			shift
			;;
		--full)
			MODE="full"
			shift
			;;
		--dry-run)
			DRY_RUN=1
			shift
			;;
		*)
			echo "unknown option: $1" >&2
			exit 1
			;;
	esac
done

FB_DEEP_MODE="$MODE"
fb_deep_apply_mode

run_main_full_prepare_snapshot() {
	fb_deep_init_full_state || return $?
	bash "$(dirname "$0")/bootstrap_env.sh" --full || return $?
	bash "$(dirname "$0")/load_baseline.sh" --full || return $?
	fb_deep_psql_sql "CHECKPOINT;" || return $?
	fb_deep_create_baseline_snapshot || return $?
	find "$FB_DEEP_ARCHIVE_CLEAN_DIR" -mindepth 1 -delete 2>/dev/null || true
	fb_deep_log "cleared archive after baseline snapshot prepare: archive_dir=$FB_DEEP_ARCHIVE_CLEAN_DIR"
}

run_main_full_batch_from_snapshot() {
	local batch_script="$1"
	local round_label="$2"
	local start_lsn

	fb_deep_restore_baseline_snapshot || return $?
	find "$FB_DEEP_ARCHIVE_CLEAN_DIR" -mindepth 1 -delete 2>/dev/null || true
	start_lsn="$(fb_deep_current_lsn)" || return $?
	bash "$(dirname "$0")/$batch_script" --full || return $?
	fb_deep_log_wal_budget_since "$start_lsn" "$round_label"
}

run_main_full_suite_with_snapshot() {
	local batch_label
	local batch_script
	local -a batches=(
		"batch_a:run_batch_a.sh"
		"batch_b:run_batch_b.sh"
		"batch_c:run_batch_c.sh"
		"batch_d:run_batch_d.sh"
		"batch_e:run_batch_e.sh"
	)

	if [[ ! -f "$(fb_deep_full_state_file)" ]]; then
		fb_deep_init_full_state
	fi

	if [[ "$(fb_deep_full_state_value baseline_snapshot_ready)" != "1" ]] || \
		[[ ! -d "$(fb_deep_full_state_value baseline_snapshot_dir)" ]]; then
		fb_deep_log "baseline snapshot missing; preparing a fresh baseline snapshot"
		fb_deep_run_round_with_retry "prepare_baseline_snapshot" run_main_full_prepare_snapshot
	fi

	for entry in "${batches[@]}"; do
		batch_label="${entry%%:*}"
		batch_script="${entry#*:}"
		if fb_deep_full_batch_completed "$batch_label"; then
			fb_deep_log "skipping completed batch from state file: $batch_label"
			continue
		fi
		fb_deep_run_round_with_retry "$batch_label" run_main_full_batch_from_snapshot "$batch_script" "$batch_label"
		fb_deep_mark_full_batch_completed "$batch_label"
	done
}

if [[ "$DRY_RUN" -eq 1 ]]; then
	fb_deep_log "dry-run"
	fb_deep_log "mode=$FB_DEEP_MODE"
	if [[ "$FB_DEEP_MODE" == "full" ]]; then
		fb_deep_log "steps=prepare_baseline_snapshot(once) -> restore_snapshot(batch_a) -> restore_snapshot(batch_b) -> restore_snapshot(batch_c) -> restore_snapshot(batch_d) -> restore_snapshot(batch_e)"
	else
		fb_deep_log "steps=bootstrap -> load_baseline -> batch_a -> batch_b -> batch_c -> batch_d -> batch_e"
	fi
	exit 0
fi

if [[ "$FB_DEEP_MODE" == "full" ]]; then
	set +e
	run_main_full_suite_with_snapshot
	status=$?
	set -e
	if [[ "$status" -eq 0 ]]; then
		fb_deep_remove_baseline_snapshot
		fb_deep_reset_full_state
	else
		fb_deep_log "full run failed; baseline snapshot and state file retained for resume"
	fi
	exit "$status"
else
	bash "$(dirname "$0")/bootstrap_env.sh" "--$FB_DEEP_MODE"
	bash "$(dirname "$0")/load_baseline.sh" "--$FB_DEEP_MODE"
	bash "$(dirname "$0")/run_batch_a.sh" "--$FB_DEEP_MODE"
	bash "$(dirname "$0")/run_batch_b.sh" "--$FB_DEEP_MODE"
	bash "$(dirname "$0")/run_batch_c.sh" "--$FB_DEEP_MODE"
	bash "$(dirname "$0")/run_batch_d.sh" "--$FB_DEEP_MODE"
	bash "$(dirname "$0")/run_batch_e.sh" "--$FB_DEEP_MODE"
fi

fb_deep_log "deep test run completed: mode=$FB_DEEP_MODE"
