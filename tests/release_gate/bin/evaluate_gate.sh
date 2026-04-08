#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

fb_release_gate_require_cmd jq

results_manifest="$(fb_release_gate_json_path flashback_results)"
truth_manifest="$(fb_release_gate_json_path truth_manifest)"
evaluation_file="$(fb_release_gate_json_path gate_evaluation)"
thresholds_file="$FB_RELEASE_GATE_CONFIG_DIR/thresholds.json"
pg_major="$(fb_release_gate_detect_pg_major)"
golden_file="$FB_RELEASE_GATE_ROOT/golden/pg${pg_major}.json"
items_file="$(mktemp)"

[[ -f "$results_manifest" ]] || fb_release_gate_fail "missing results manifest: $results_manifest"
[[ -f "$truth_manifest" ]] || fb_release_gate_fail "missing truth manifest: $truth_manifest"
[[ -f "$golden_file" ]] || fb_release_gate_fail "missing golden baseline: $golden_file"

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
	baseline_key="$(printf '%s' "$result_entry" | jq -r '.baseline_key // empty')"
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
	performance_status="pass"
	performance_reason=""
	reason=""
	truth_sha256="$(printf '%s' "$correctness_eval_json" | jq -r '.truth_sha256')"
	truth_row_count="$(printf '%s' "$correctness_eval_json" | jq -r '.truth_row_count')"
	diff_path="$(printf '%s' "$correctness_eval_json" | jq -r '.diff_path')"
	baseline_ms=""
	ratio_threshold="$(jq -r --arg scenario_id "$scenario_id" '.scenarios[$scenario_id].ratio_threshold // .default_ratio_threshold' "$thresholds_file")"
	absolute_threshold_ms="$(jq -r --arg scenario_id "$scenario_id" '.scenarios[$scenario_id].absolute_threshold_ms // .default_absolute_threshold_ms' "$thresholds_file")"

	if [[ "$correctness_status" != "pass" ]]; then
		if [[ "$correctness_status" == "infra_fail" ]]; then
			performance_status="infra_fail"
			performance_reason="correctness infra failure"
		else
			performance_status="skipped"
			performance_reason="correctness failed"
		fi
	fi

	if [[ "$performance_status" == "pass" ]]; then
		baseline_ms="$(jq -r --arg baseline_key "$baseline_key" '.scenarios[$baseline_key].baseline_elapsed_ms // empty' "$golden_file")"
		if [[ -z "$baseline_ms" ]]; then
			performance_status="missing_baseline"
			performance_reason="missing golden baseline"
		else
			if awk -v current="$gate_elapsed_ms" -v baseline="$baseline_ms" -v ratio="$ratio_threshold" -v absolute="$absolute_threshold_ms" 'BEGIN { exit !((current > baseline * (1 + ratio)) && ((current - baseline) > absolute)) }'; then
				performance_status="fail"
				performance_reason="performance regression"
			fi
		fi
	fi

	if [[ -n "$correctness_reason" ]]; then
		reason="$correctness_reason"
	fi
	if [[ -n "$performance_reason" ]]; then
		if [[ -n "$reason" ]]; then
			reason="${reason}; ${performance_reason}"
		else
			reason="$performance_reason"
		fi
	fi

	item_json="$(jq -cn \
		--arg scenario_id "$scenario_id" \
		--arg truth_scenario_id "$truth_scenario_id" \
		--arg schema_name "$schema_name" \
		--arg table_name "$table_name" \
		--arg target_ts "$target_ts" \
		--arg path_kind "$path_kind" \
		--arg csv_path "$csv_path" \
		--arg table_class "$table_class" \
		--arg baseline_key "$baseline_key" \
		--arg correctness_status "$correctness_status" \
		--arg correctness_reason "$correctness_reason" \
		--arg performance_status "$performance_status" \
		--arg performance_reason "$performance_reason" \
		--arg reason "$reason" \
		--arg diff_path "$diff_path" \
		--arg result_sha256 "$result_sha256" \
		--arg truth_sha256 "$truth_sha256" \
		--argjson result_row_count "$result_row_count" \
		--argjson truth_row_count "$truth_row_count" \
		--argjson gate_elapsed_ms "$gate_elapsed_ms" \
		--argjson measured_elapsed_ms "$measured_elapsed_ms" \
		--argjson baseline_elapsed_ms "${baseline_ms:-0}" \
		--argjson ratio_threshold "$ratio_threshold" \
		--argjson absolute_threshold_ms "$absolute_threshold_ms" \
		'{scenario_id:$scenario_id, truth_scenario_id:$truth_scenario_id, schema_name:$schema_name, table_name:$table_name, target_ts:$target_ts, path_kind:$path_kind, csv_path:$csv_path, table_class:$table_class, baseline_key:$baseline_key, correctness_status:$correctness_status, correctness_reason:$correctness_reason, performance_status:$performance_status, performance_reason:$performance_reason, reason:$reason, diff_path:$diff_path, result_sha256:$result_sha256, truth_sha256:$truth_sha256, result_row_count:$result_row_count, truth_row_count:$truth_row_count, gate_elapsed_ms:$gate_elapsed_ms, measured_elapsed_ms:$measured_elapsed_ms, baseline_elapsed_ms:$baseline_elapsed_ms, ratio_threshold:$ratio_threshold, absolute_threshold_ms:$absolute_threshold_ms}')"
	fb_release_gate_manifest_append "$items_file" "$item_json"
done < <(jq -c '.[]' "$results_manifest")

truth_entries="$(jq 'length' "$truth_manifest")"
truth_target_snapshot_entries="$(jq '[.[] | select((.target_snapshot // "") != "")] | length' "$truth_manifest")"
golden_entries="$(jq '.scenarios | length' "$golden_file")"

evaluation_json="$(jq -n \
	--arg pg_major "$pg_major" \
	--arg output_dir "$FB_RELEASE_GATE_OUTPUT_DIR" \
	--arg golden_file "$golden_file" \
	--argjson truth_entries "$truth_entries" \
	--argjson truth_target_snapshot_entries "$truth_target_snapshot_entries" \
	--argjson golden_entries "$golden_entries" \
	--slurpfile items "$items_file" '
		{
			pg_major: $pg_major,
			output_dir: $output_dir,
			golden_file: $golden_file,
			results: $items[0],
			truth_summary: {
				entries: $truth_entries,
				target_snapshot_entries: $truth_target_snapshot_entries
			},
			golden_summary: {
				entries: $golden_entries
			},
			summary: {
				total: ($items[0] | length),
				correctness_passed: ($items[0] | map(select(.correctness_status == "pass")) | length),
				correctness_failures: ($items[0] | map(select(.correctness_status != "pass")) | length),
				correctness_infra_failures: ($items[0] | map(select(.correctness_status == "infra_fail")) | length),
				performance_failures: ($items[0] | map(select(.performance_status == "fail" or .performance_status == "infra_fail")) | length),
				performance_regressions: ($items[0] | map(select(.performance_status == "fail")) | length),
				performance_infra_failures: ($items[0] | map(select(.performance_status == "infra_fail")) | length),
				performance_skipped: ($items[0] | map(select(.performance_status == "skipped")) | length),
				performance_missing_baseline: ($items[0] | map(select(.performance_status == "missing_baseline")) | length)
			},
			verdict: (
				if (($items[0] | length) == 0) or
				   (($items[0] | map(select(.correctness_status != "pass")) | length) > 0) or
				   (($items[0] | map(select(.performance_status == "fail" or .performance_status == "infra_fail")) | length) > 0)
				then "FAIL"
				elif (($items[0] | map(select(.performance_status == "missing_baseline")) | length) > 0)
				then "INCOMPLETE"
				else "PASS"
				end
			)
		}')"

fb_release_gate_write_json "$evaluation_file" "$evaluation_json"
rm -f "$items_file"
fb_release_gate_log "gate evaluation written to $evaluation_file"
