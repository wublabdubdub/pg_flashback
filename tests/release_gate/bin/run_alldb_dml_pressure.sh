#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

DRY_RUN=0
START_ONLY=0
WAIT_ONLY=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dry-run)
			DRY_RUN=1
			;;
		--start-only)
			START_ONLY=1
			;;
		--wait)
			WAIT_ONLY=1
			;;
		*)
			fb_release_gate_fail "unknown argument: $1"
			;;
	esac
	shift
done

fb_release_gate_require_cmd jq

if [[ "$START_ONLY" -eq 1 && "$WAIT_ONLY" -eq 1 ]]; then
	fb_release_gate_fail "--start-only and --wait are mutually exclusive"
fi

schema_name="$(fb_release_gate_scenario_schema)"
runtime_file="$(fb_release_gate_json_path dml_pressure_runtime)"
request_file="$(fb_release_gate_json_path dml_pressure_request)"
response_file="$(fb_release_gate_json_path dml_pressure_response)"
final_file="$(fb_release_gate_json_path dml_pressure_final)"

payload="$(jq -cn \
	--arg target_type "schema" \
	--arg schema "$schema_name" \
	--argjson duration_sec "$FB_RELEASE_GATE_SIM_DML_DURATION_SEC" \
	--argjson workers "$FB_RELEASE_GATE_SIM_DML_WORKERS" \
	--argjson rate_limit_ops "$FB_RELEASE_GATE_SIM_DML_RATE_LIMIT_OPS" \
	'{target_type:$target_type, schema:$schema, duration_sec:$duration_sec, workers:$workers, rate_limit_ops:$rate_limit_ops, operations:{insert:{enabled:true, weight:1}, update:{enabled:true, weight:1}, delete:{enabled:true, weight:1}}}')"
printf '%s\n' "$payload" > "$request_file"

start_job() {
	local job
	local job_id
	local pressure_start_ts
	local pressure_end_ts
	local runtime_json

	if [[ "$DRY_RUN" -eq 1 ]]; then
		pressure_start_ts="$(date -u '+%Y-%m-%d %H:%M:%S+00')"
		pressure_end_ts="$(fb_release_gate_timestamp_add_sec "$pressure_start_ts" "$FB_RELEASE_GATE_SIM_DML_DURATION_SEC")"
		runtime_json="$(jq -cn \
			--arg job_id "dry-run-pressure-job" \
			--arg pressure_start_ts "$pressure_start_ts" \
			--arg pressure_end_ts "$pressure_end_ts" \
			--arg schema_name "$schema_name" \
			--argjson duration_sec "$FB_RELEASE_GATE_SIM_DML_DURATION_SEC" \
			--argjson random_seed "$FB_RELEASE_GATE_RANDOM_SEED" \
			'{job_id:$job_id, pressure_start_ts:$pressure_start_ts, planned_end_ts:$pressure_end_ts, duration_sec:$duration_sec, schema_name:$schema_name, random_seed:$random_seed, dry_run:true}')"
		fb_release_gate_write_json "$runtime_file" "$runtime_json"
		fb_release_gate_log "would submit dml pressure job using payload $request_file"
		return 0
	fi

	fb_release_gate_sim_connect > "$(fb_release_gate_json_path simulator_connect)"
	job="$(fb_release_gate_sim_request POST /api/jobs "$payload")"
	printf '%s\n' "$job" > "$response_file"
	job_id="$(printf '%s' "$job" | jq -r '.id')"
	[[ -n "$job_id" && "$job_id" != "null" ]] || fb_release_gate_fail "dml pressure response missing job id"
	pressure_start_ts="$(date -u '+%Y-%m-%d %H:%M:%S+00')"
	pressure_end_ts="$(fb_release_gate_timestamp_add_sec "$pressure_start_ts" "$FB_RELEASE_GATE_SIM_DML_DURATION_SEC")"
	runtime_json="$(jq -cn \
		--arg job_id "$job_id" \
		--arg pressure_start_ts "$pressure_start_ts" \
		--arg pressure_end_ts "$pressure_end_ts" \
		--arg schema_name "$schema_name" \
		--argjson duration_sec "$FB_RELEASE_GATE_SIM_DML_DURATION_SEC" \
		--argjson random_seed "$FB_RELEASE_GATE_RANDOM_SEED" \
		'{job_id:$job_id, pressure_start_ts:$pressure_start_ts, planned_end_ts:$pressure_end_ts, duration_sec:$duration_sec, schema_name:$schema_name, random_seed:$random_seed, dry_run:false}')"
	fb_release_gate_write_json "$runtime_file" "$runtime_json"
	fb_release_gate_log "dml pressure started job_id=$job_id"
}

wait_job() {
	local job_id

	[[ -f "$runtime_file" ]] || fb_release_gate_fail "missing runtime file: $runtime_file"
	job_id="$(jq -r '.job_id' "$runtime_file")"
	[[ -n "$job_id" && "$job_id" != "null" ]] || fb_release_gate_fail "runtime file missing job_id"

	if [[ "$DRY_RUN" -eq 1 ]]; then
		fb_release_gate_write_json "$final_file" "$(jq -cn \
			--arg id "$job_id" \
			--arg status "done" \
			'{id:$id, status:$status, dry_run:true}')"
		fb_release_gate_log "would wait for dml pressure job_id=$job_id"
		return 0
	fi

	fb_release_gate_sim_wait_job "$job_id" "$final_file" $((FB_RELEASE_GATE_SIM_DML_DURATION_SEC + 1800))
	fb_release_gate_log "dml pressure completed job_id=$job_id"
}

if [[ "$WAIT_ONLY" -eq 1 ]]; then
	wait_job
elif [[ "$START_ONLY" -eq 1 ]]; then
	start_job
else
	start_job
	wait_job
fi
