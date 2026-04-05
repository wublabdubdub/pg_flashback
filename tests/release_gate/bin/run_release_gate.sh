#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config

DRY_RUN=0
LIST_STAGES=0
FROM_STAGE=""
TO_STAGE=""
ONLY_STAGE=""
STAGE_NAMES=(
	prepare_instance
	start_alldbsimulator
	load_seed_data
	grow_target_table
	start_dml_pressure
	capture_random_truth_snapshots
	wait_dml_pressure_finish
	capture_dml_truth_snapshots
	run_flashback_checks
	evaluate_gate
	render_gate_report
)

stage_description() {
	case "$1" in
		prepare_instance)
			printf '%s\n' 'check archive settings, clear archive dir, clean oversized databases, recreate alldb'
			;;
		start_alldbsimulator)
			printf '%s\n' 'start /root/alldbsimulator and wait for health'
			;;
		load_seed_data)
			printf '%s\n' 'load initial seed data and build the 50 x 100MB dataset'
			;;
		grow_target_table)
			printf '%s\n' 'grow the configured flashback target table to 5GB before DML pressure starts'
			;;
		start_dml_pressure)
			printf '%s\n' 'start the 1-hour schema-level DML pressure job after seed load and target growth'
			;;
		capture_random_truth_snapshots)
			printf '%s\n' 'capture five random truth snapshots while the DML job is still running'
			;;
		wait_dml_pressure_finish)
			printf '%s\n' 'wait for the running DML pressure job to finish'
			;;
		capture_dml_truth_snapshots)
			printf '%s\n' 'run deterministic single-row, bulk 10k, and mixed DML snapshot capture'
			;;
		run_flashback_checks)
			printf '%s\n' 'run pg_flashback query, COPY TO, and CTAS verification scenarios'
			;;
		evaluate_gate)
			printf '%s\n' 'compare flashback results with truth snapshots and golden baselines'
			;;
		render_gate_report)
			printf '%s\n' 'render the final Markdown report from collected JSON artifacts'
			;;
		*)
			fb_release_gate_fail "unknown stage description lookup: $1"
			;;
	esac
}

print_stage_list() {
	local stage

	for stage in "${STAGE_NAMES[@]}"; do
		printf '%s\t%s\n' "$stage" "$(stage_description "$stage")"
	done
}

stage_index() {
	local target="$1"
	local idx

	for idx in "${!STAGE_NAMES[@]}"; do
		if [[ "${STAGE_NAMES[$idx]}" == "$target" ]]; then
			printf '%s\n' "$idx"
			return 0
		fi
	done
	return 1
}

stage_cmd() {
	local stage="$1"

	case "$stage" in
		prepare_instance)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/prepare_empty_instance.sh" "--dry-run"
			else
				printf '%q\n' "$FB_RELEASE_GATE_BIN_DIR/prepare_empty_instance.sh"
			fi
			;;
		start_alldbsimulator)
			printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/start_alldbsim.sh" "start"
			;;
		load_seed_data)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/load_alldb_seed.sh" "--dry-run"
			else
				printf '%q\n' "$FB_RELEASE_GATE_BIN_DIR/load_alldb_seed.sh"
			fi
			;;
		start_dml_pressure)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q %q\n' "$FB_RELEASE_GATE_BIN_DIR/run_alldb_dml_pressure.sh" "--dry-run" "--start-only"
			else
				printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/run_alldb_dml_pressure.sh" "--start-only"
			fi
			;;
		capture_random_truth_snapshots)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q %q %q\n' "$FB_RELEASE_GATE_BIN_DIR/capture_truth_snapshots.sh" "--dry-run" "--mode" "random"
			else
				printf '%q %q %q\n' "$FB_RELEASE_GATE_BIN_DIR/capture_truth_snapshots.sh" "--mode" "random"
			fi
			;;
		wait_dml_pressure_finish)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q %q\n' "$FB_RELEASE_GATE_BIN_DIR/run_alldb_dml_pressure.sh" "--dry-run" "--wait"
			else
				printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/run_alldb_dml_pressure.sh" "--wait"
			fi
			;;
		grow_target_table)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/grow_flashback_target.sh" "--dry-run"
			else
				printf '%q\n' "$FB_RELEASE_GATE_BIN_DIR/grow_flashback_target.sh"
			fi
			;;
		capture_dml_truth_snapshots)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q %q %q\n' "$FB_RELEASE_GATE_BIN_DIR/capture_truth_snapshots.sh" "--dry-run" "--mode" "dml"
			else
				printf '%q %q %q\n' "$FB_RELEASE_GATE_BIN_DIR/capture_truth_snapshots.sh" "--mode" "dml"
			fi
			;;
		run_flashback_checks)
			if [[ "$DRY_RUN" -eq 1 ]]; then
				printf '%q %q\n' "$FB_RELEASE_GATE_BIN_DIR/run_flashback_matrix.sh" "--dry-run"
			else
				printf '%q\n' "$FB_RELEASE_GATE_BIN_DIR/run_flashback_matrix.sh"
			fi
			;;
		evaluate_gate)
			printf '%q\n' "$FB_RELEASE_GATE_BIN_DIR/evaluate_gate.sh"
			;;
		render_gate_report)
			printf '%q\n' "$FB_RELEASE_GATE_BIN_DIR/render_report.sh"
			;;
		*)
			fb_release_gate_fail "unsupported stage: $stage"
			;;
	esac
}

run_stage() {
	local stage="$1"
	local cmd

	cmd="$(stage_cmd "$stage")"
	fb_release_gate_log "stage start: ${stage} - $(stage_description "$stage")"
	eval "$cmd"
	if [[ "$stage" == "prepare_instance" ]]; then
		RAN_PREPARE_INSTANCE=1
	fi
	fb_release_gate_log "stage done: ${stage}"
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dry-run)
			DRY_RUN=1
			;;
		--list-stages)
			LIST_STAGES=1
			;;
		--from)
			shift
			[[ $# -gt 0 ]] || fb_release_gate_fail "--from requires a stage name"
			FROM_STAGE="$1"
			;;
		--to)
			shift
			[[ $# -gt 0 ]] || fb_release_gate_fail "--to requires a stage name"
			TO_STAGE="$1"
			;;
		--only)
			shift
			[[ $# -gt 0 ]] || fb_release_gate_fail "--only requires a stage name"
			ONLY_STAGE="$1"
			;;
		*)
			fb_release_gate_fail "unknown argument: $1"
			;;
	esac
	shift
done

if [[ "$LIST_STAGES" -eq 1 ]]; then
	print_stage_list
	exit 0
fi

if [[ -n "$ONLY_STAGE" && ( -n "$FROM_STAGE" || -n "$TO_STAGE" ) ]]; then
	fb_release_gate_fail "--only cannot be combined with --from or --to"
fi

if [[ -n "$ONLY_STAGE" ]]; then
	FROM_STAGE="$ONLY_STAGE"
	TO_STAGE="$ONLY_STAGE"
fi

cleanup() {
	local major archive_dir

	"$FB_RELEASE_GATE_BIN_DIR/start_alldbsim.sh" stop >/dev/null 2>&1 || true
	if [[ "$DRY_RUN" -eq 1 || "${RAN_PREPARE_INSTANCE:-0}" -ne 1 ]]; then
		return 0
	fi
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
from_idx=0
to_idx=$((${#STAGE_NAMES[@]} - 1))

if [[ -n "$FROM_STAGE" ]]; then
	from_idx="$(stage_index "$FROM_STAGE")" || fb_release_gate_fail "unknown stage for --from: $FROM_STAGE"
fi
if [[ -n "$TO_STAGE" ]]; then
	to_idx="$(stage_index "$TO_STAGE")" || fb_release_gate_fail "unknown stage for --to: $TO_STAGE"
fi
if (( from_idx > to_idx )); then
	fb_release_gate_fail "--from stage must not come after --to stage"
fi

fb_release_gate_log "starting release gate output_dir=$FB_RELEASE_GATE_OUTPUT_DIR dry_run=$DRY_RUN from=${STAGE_NAMES[$from_idx]} to=${STAGE_NAMES[$to_idx]}"

for idx in "${!STAGE_NAMES[@]}"; do
	if (( idx < from_idx || idx > to_idx )); then
		continue
	fi
	run_stage "${STAGE_NAMES[$idx]}"
done

fb_release_gate_log "release gate finished"
