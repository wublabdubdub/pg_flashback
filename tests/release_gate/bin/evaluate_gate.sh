#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

fb_release_gate_require_cmd jq

results_manifest="$(fb_release_gate_json_path flashback_results)"
truth_manifest="$(fb_release_gate_json_path truth_manifest)"
evaluation_file="$(fb_release_gate_json_path gate_evaluation)"
pg_major="$(fb_release_gate_detect_pg_major)"
items_file="$(mktemp)"

[[ -f "$results_manifest" ]] || fb_release_gate_fail "missing results manifest: $results_manifest"
[[ -f "$truth_manifest" ]] || fb_release_gate_fail "missing truth manifest: $truth_manifest"

fb_release_gate_manifest_init "$items_file"

while IFS= read -r result_entry; do
	scenario_id="$(printf '%s' "$result_entry" | jq -r '.scenario_id')"
	truth_scenario_id="$(printf '%s' "$result_entry" | jq -r '.truth_scenario_id')"
	schema_name="$(printf '%s' "$result_entry" | jq -r '.schema_name // empty')"
	table_name="$(printf '%s' "$result_entry" | jq -r '.table_name')"
	target_ts="$(printf '%s' "$result_entry" | jq -r '.target_ts // empty')"
	path_kind="$(printf '%s' "$result_entry" | jq -r '.path_kind // "query"')"
	csv_path="$(printf '%s' "$result_entry" | jq -r '.csv_path // empty')"
	table_class="$(printf '%s' "$result_entry" | jq -r '.table_class // empty')"
	result_sha256="$(printf '%s' "$result_entry" | jq -r '.sha256 // empty')"
	result_row_count="$(printf '%s' "$result_entry" | jq -r '.row_count // 0')"
	gate_elapsed_ms="$(printf '%s' "$result_entry" | jq -r '.gate_elapsed_ms // 0')"
	measured_elapsed_ms="$(printf '%s' "$result_entry" | jq -c '.measured_elapsed_ms // []')"
	correctness_eval_json="$(fb_release_gate_eval_correctness_json \
		"$scenario_id" \
		"$truth_manifest" \
		"$truth_scenario_id" \
		"$table_name" \
		"$result_sha256" \
		"$result_row_count" \
		"$csv_path")"

	correctness_status="$(printf '%s' "$correctness_eval_json" | jq -r '.correctness_status')"
	correctness_reason="$(printf '%s' "$correctness_eval_json" | jq -r '.reason')"
	reason="$correctness_reason"
	truth_sha256="$(printf '%s' "$correctness_eval_json" | jq -r '.truth_sha256')"
	truth_row_count="$(printf '%s' "$correctness_eval_json" | jq -r '.truth_row_count')"
	diff_path="$(printf '%s' "$correctness_eval_json" | jq -r '.diff_path')"

	item_json="$(jq -cn \
		--arg scenario_id "$scenario_id" \
		--arg truth_scenario_id "$truth_scenario_id" \
		--arg schema_name "$schema_name" \
		--arg table_name "$table_name" \
		--arg target_ts "$target_ts" \
		--arg path_kind "$path_kind" \
		--arg csv_path "$csv_path" \
		--arg table_class "$table_class" \
		--arg correctness_status "$correctness_status" \
		--arg correctness_reason "$correctness_reason" \
		--arg reason "$reason" \
		--arg diff_path "$diff_path" \
		--arg result_sha256 "$result_sha256" \
		--arg truth_sha256 "$truth_sha256" \
		--argjson result_row_count "$result_row_count" \
		--argjson truth_row_count "$truth_row_count" \
		--argjson gate_elapsed_ms "$gate_elapsed_ms" \
		--argjson measured_elapsed_ms "$measured_elapsed_ms" \
		'{scenario_id:$scenario_id, truth_scenario_id:$truth_scenario_id, schema_name:$schema_name, table_name:$table_name, target_ts:$target_ts, path_kind:$path_kind, csv_path:$csv_path, table_class:$table_class, correctness_status:$correctness_status, correctness_reason:$correctness_reason, reason:$reason, diff_path:$diff_path, result_sha256:$result_sha256, truth_sha256:$truth_sha256, result_row_count:$result_row_count, truth_row_count:$truth_row_count, gate_elapsed_ms:$gate_elapsed_ms, measured_elapsed_ms:$measured_elapsed_ms}')"
	fb_release_gate_manifest_append "$items_file" "$item_json"
done < <(jq -c '.[]' "$results_manifest")

truth_entries="$(jq 'length' "$truth_manifest")"
truth_target_snapshot_entries="$(jq '[.[] | select((.target_snapshot // "") != "")] | length' "$truth_manifest")"

evaluation_json="$(jq -n \
	--arg pg_major "$pg_major" \
	--arg output_dir "$FB_RELEASE_GATE_OUTPUT_DIR" \
	--argjson truth_entries "$truth_entries" \
	--argjson truth_target_snapshot_entries "$truth_target_snapshot_entries" \
	--slurpfile items "$items_file" '
		{
			pg_major: $pg_major,
			output_dir: $output_dir,
			results: $items[0],
			truth_summary: {
				entries: $truth_entries,
				target_snapshot_entries: $truth_target_snapshot_entries
			},
			summary: {
				total: ($items[0] | length),
				correctness_passed: ($items[0] | map(select(.correctness_status == "pass")) | length),
				correctness_failures: ($items[0] | map(select(.correctness_status != "pass")) | length),
				correctness_infra_failures: ($items[0] | map(select(.correctness_status == "infra_fail")) | length)
			},
			verdict: (
				if (($items[0] | length) == 0) or
				   (($items[0] | map(select(.correctness_status != "pass")) | length) > 0)
				then "FAIL"
				else "PASS"
				end
			)
		}')"

fb_release_gate_write_json "$evaluation_file" "$evaluation_json"
rm -f "$items_file"
fb_release_gate_log "gate evaluation written to $evaluation_file"
