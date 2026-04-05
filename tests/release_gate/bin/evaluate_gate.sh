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
	table_name="$(printf '%s' "$result_entry" | jq -r '.table_name')"
	baseline_key="$(printf '%s' "$result_entry" | jq -r '.baseline_key // empty')"
	result_sha256="$(printf '%s' "$result_entry" | jq -r '.sha256 // empty')"
	result_row_count="$(printf '%s' "$result_entry" | jq -r '.row_count // 0')"
	gate_elapsed_ms="$(printf '%s' "$result_entry" | jq -r '.gate_elapsed_ms // 0')"
	truth_entry="$(jq -c \
		--arg scenario_id "$truth_scenario_id" \
		--arg table_name "$table_name" \
		'.[] | select(.scenario_id == $scenario_id and .table_name == $table_name)' \
		"$truth_manifest" | head -n 1)"

	correctness_status="pass"
	performance_status="pass"
	reason=""
	truth_sha256=""
	truth_row_count="0"
	truth_csv_path=""
	result_csv_path="$(printf '%s' "$result_entry" | jq -r '.csv_path // empty')"
	diff_path=""
	baseline_ms=""
	ratio_threshold="$(jq -r --arg scenario_id "$scenario_id" '.scenarios[$scenario_id].ratio_threshold // .default_ratio_threshold' "$thresholds_file")"
	absolute_threshold_ms="$(jq -r --arg scenario_id "$scenario_id" '.scenarios[$scenario_id].absolute_threshold_ms // .default_absolute_threshold_ms' "$thresholds_file")"

	if [[ -z "$truth_entry" ]]; then
		correctness_status="infra_fail"
		performance_status="infra_fail"
		reason="missing truth snapshot"
	else
		truth_sha256="$(printf '%s' "$truth_entry" | jq -r '.sha256')"
		truth_row_count="$(printf '%s' "$truth_entry" | jq -r '.row_count')"
		truth_csv_path="$(printf '%s' "$truth_entry" | jq -r '.csv_path // empty')"
		if [[ "$truth_sha256" != "$result_sha256" || "$truth_row_count" != "$result_row_count" ]]; then
			correctness_status="fail"
			performance_status="skipped"
			reason="row_count or sha256 mismatch"
			if [[ -n "$truth_csv_path" && -n "$result_csv_path" && -f "$truth_csv_path" && -f "$result_csv_path" ]]; then
				diff_path="$(fb_release_gate_output_path "logs/diff_${scenario_id}__${table_name}.diff")"
				diff -u "$truth_csv_path" "$result_csv_path" > "$diff_path" 2>&1 || true
				chmod 0666 "$diff_path" 2>/dev/null || true
			fi
		fi
	fi

	if [[ "$performance_status" == "pass" ]]; then
		baseline_ms="$(jq -r --arg baseline_key "$baseline_key" '.scenarios[$baseline_key].baseline_elapsed_ms // empty' "$golden_file")"
		if [[ -z "$baseline_ms" ]]; then
			performance_status="infra_fail"
			if [[ -n "$reason" ]]; then
				reason="${reason}; missing golden baseline"
			else
				reason="missing golden baseline"
			fi
		else
			if awk -v current="$gate_elapsed_ms" -v baseline="$baseline_ms" -v ratio="$ratio_threshold" -v absolute="$absolute_threshold_ms" 'BEGIN { exit !((current > baseline * (1 + ratio)) && ((current - baseline) > absolute)) }'; then
				performance_status="fail"
				if [[ -n "$reason" ]]; then
					reason="${reason}; performance regression"
				else
					reason="performance regression"
				fi
			fi
		fi
	fi

	item_json="$(jq -cn \
		--arg scenario_id "$scenario_id" \
		--arg truth_scenario_id "$truth_scenario_id" \
		--arg table_name "$table_name" \
		--arg baseline_key "$baseline_key" \
		--arg correctness_status "$correctness_status" \
		--arg performance_status "$performance_status" \
		--arg reason "$reason" \
		--arg diff_path "$diff_path" \
		--arg result_sha256 "$result_sha256" \
		--arg truth_sha256 "$truth_sha256" \
		--argjson result_row_count "$result_row_count" \
		--argjson truth_row_count "$truth_row_count" \
		--argjson gate_elapsed_ms "$gate_elapsed_ms" \
		--argjson baseline_elapsed_ms "${baseline_ms:-0}" \
		--argjson ratio_threshold "$ratio_threshold" \
		--argjson absolute_threshold_ms "$absolute_threshold_ms" \
		'{scenario_id:$scenario_id, truth_scenario_id:$truth_scenario_id, table_name:$table_name, baseline_key:$baseline_key, correctness_status:$correctness_status, performance_status:$performance_status, reason:$reason, diff_path:$diff_path, result_sha256:$result_sha256, truth_sha256:$truth_sha256, result_row_count:$result_row_count, truth_row_count:$truth_row_count, gate_elapsed_ms:$gate_elapsed_ms, baseline_elapsed_ms:$baseline_elapsed_ms, ratio_threshold:$ratio_threshold, absolute_threshold_ms:$absolute_threshold_ms}')"
	fb_release_gate_manifest_append "$items_file" "$item_json"
done < <(jq -c '.[]' "$results_manifest")

evaluation_json="$(jq -n \
	--arg pg_major "$pg_major" \
	--arg output_dir "$FB_RELEASE_GATE_OUTPUT_DIR" \
	--arg golden_file "$golden_file" \
	--slurpfile items "$items_file" '
		{
			pg_major: $pg_major,
			output_dir: $output_dir,
			golden_file: $golden_file,
			results: $items[0],
			summary: {
				total: ($items[0] | length),
				correctness_failures: ($items[0] | map(select(.correctness_status != "pass")) | length),
				performance_failures: ($items[0] | map(select(.performance_status == "fail" or .performance_status == "infra_fail")) | length)
			},
			verdict: (
				if (($items[0] | length) == 0) or
				   (($items[0] | map(select(.correctness_status != "pass")) | length) > 0) or
				   (($items[0] | map(select(.performance_status == "fail" or .performance_status == "infra_fail")) | length) > 0)
				then "FAIL"
				else "PASS"
				end
			)
		}')"

fb_release_gate_write_json "$evaluation_file" "$evaluation_json"
rm -f "$items_file"
fb_release_gate_log "gate evaluation written to $evaluation_file"
