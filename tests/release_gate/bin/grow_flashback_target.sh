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

schema_name="$(fb_release_gate_scenario_schema)"
target_qualified="${schema_name}.${FB_RELEASE_GATE_TARGET_TABLE_NAME}"

if [[ "$DRY_RUN" -eq 1 ]]; then
	fb_release_gate_log "would grow target table ${target_qualified} toward ${FB_RELEASE_GATE_TARGET_SIZE_BYTES} bytes"
	exit 0
fi

fb_release_gate_sim_connect > "$(fb_release_gate_json_path simulator_connect)"

while true; do
	size_bytes="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select pg_total_relation_size('${target_qualified}'::regclass);")"
	[[ -n "$size_bytes" ]] || fb_release_gate_fail "could not read target table size for ${target_qualified}"
	if (( size_bytes >= FB_RELEASE_GATE_TARGET_SIZE_BYTES )); then
		break
	fi

	payload="$(jq -cn \
		--arg mode "existing_table" \
		--arg schema "$schema_name" \
		--arg table "$FB_RELEASE_GATE_TARGET_TABLE_NAME" \
		--argjson rows_to_import "$FB_RELEASE_GATE_TARGET_GROW_ROWS_PER_BATCH" \
		--argjson batch_size "$FB_RELEASE_GATE_SIM_BATCH_SIZE" \
		--argjson import_workers "$FB_RELEASE_GATE_SIM_IMPORT_WORKERS" \
		'{mode:$mode, schema:$schema, table:$table, rows_to_import:$rows_to_import, batch_size:$batch_size, import_workers:$import_workers, truncate_before_import:false}')"
	job="$(fb_release_gate_sim_request POST /api/generate/jobs "$payload")"
	job_id="$(printf '%s' "$job" | jq -r '.id')"
	[[ -n "$job_id" && "$job_id" != "null" ]] || fb_release_gate_fail "grow target response missing job id"
	fb_release_gate_sim_wait_job "$job_id" "$(fb_release_gate_json_path "grow_target_${job_id}")"
	fb_release_gate_log "target growth batch completed job_id=$job_id current_size=${size_bytes}"
done

cat > "$(fb_release_gate_json_path grow_target_final)" <<EOF
{
  "schema": "$schema_name",
  "table": "$FB_RELEASE_GATE_TARGET_TABLE_NAME",
  "qualified_name": "$target_qualified",
  "size_bytes": $size_bytes,
  "target_size_bytes": $FB_RELEASE_GATE_TARGET_SIZE_BYTES
}
EOF

fb_release_gate_log "target table reached ${size_bytes} bytes table=${target_qualified}"
