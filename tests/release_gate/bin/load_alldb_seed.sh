#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
	DRY_RUN=1
	shift
fi

fb_release_gate_require_cmd jq

payload="$(jq -cn \
	--arg mode "scenario" \
	--arg scenario "$FB_RELEASE_GATE_SIM_SCENARIO" \
	--argjson table_count "$FB_RELEASE_GATE_SIM_TABLE_COUNT" \
	--argjson rows_per_table "$FB_RELEASE_GATE_SIM_ROWS_PER_TABLE" \
	--argjson batch_size "$FB_RELEASE_GATE_SIM_BATCH_SIZE" \
	--argjson import_workers "$FB_RELEASE_GATE_SIM_IMPORT_WORKERS" \
	'{mode:$mode, scenario:$scenario, table_count:$table_count, rows_per_table:$rows_per_table, batch_size:$batch_size, import_workers:$import_workers, with_primary_key:true, with_indexes:true, drop_existing_schema:true}')"

request_file="$(fb_release_gate_json_path load_seed_request)"
response_file="$(fb_release_gate_json_path load_seed_response)"
printf '%s\n' "$payload" > "$request_file"

if [[ "$DRY_RUN" -eq 1 ]]; then
	fb_release_gate_log "would submit seed generation job using payload $request_file"
	exit 0
fi

fb_release_gate_sim_connect > "$(fb_release_gate_json_path simulator_connect)"
job="$(fb_release_gate_sim_request POST /api/generate/jobs "$payload")"
printf '%s\n' "$job" > "$response_file"
job_id="$(printf '%s' "$job" | jq -r '.id')"
[[ -n "$job_id" && "$job_id" != "null" ]] || fb_release_gate_fail "seed generation response missing job id"
fb_release_gate_sim_wait_job "$job_id" "$(fb_release_gate_json_path load_seed_final)"
fb_release_gate_log "seed generation completed job_id=$job_id"
