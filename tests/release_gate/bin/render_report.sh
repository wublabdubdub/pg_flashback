#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

fb_release_gate_require_cmd jq

evaluation_file="$(fb_release_gate_json_path gate_evaluation)"
environment_file="$(fb_release_gate_json_path environment)"
template_file="$FB_RELEASE_GATE_TEMPLATE_DIR/report.md.tpl"
report_file="$(fb_release_gate_output_path reports/release_gate_report.md)"

[[ -f "$evaluation_file" ]] || fb_release_gate_fail "missing evaluation file: $evaluation_file"
[[ -f "$environment_file" ]] || fb_release_gate_fail "missing environment file: $environment_file"
[[ -f "$template_file" ]] || fb_release_gate_fail "missing report template: $template_file"

pg_major="$(jq -r '.pg_major' "$evaluation_file")"
verdict="$(jq -r '.verdict' "$evaluation_file")"
golden_file="$(jq -r '.golden_file' "$evaluation_file")"
archive_dir="$(jq -r '.archive_dir // empty' "$environment_file")"
archive_mode="$(jq -r '.archive_mode // empty' "$environment_file")"
archive_command="$(jq -r '.archive_command // empty' "$environment_file")"

summary_block="$(jq -r '
[
  "- Total Scenarios: \(.summary.total)",
  "- Correctness Failures: \(.summary.correctness_failures)",
  "- Performance Failures: \(.summary.performance_failures)"
] | join("\n")' "$evaluation_file")"

correctness_block="$(jq -r '
	["| Scenario | Table | Correctness | Reason |",
	 "| --- | --- | --- | --- |"] +
	(.results | map("| \(.scenario_id) | \(.table_name) | \(.correctness_status) | \((.reason // "") | gsub("\n"; " ")) |"))
	| join("\n")' "$evaluation_file")"

performance_block="$(jq -r '
	["| Scenario | Table | Current ms | Baseline ms | Performance |",
	 "| --- | --- | ---: | ---: | --- |"] +
	(.results | map("| \(.scenario_id) | \(.table_name) | \(.gate_elapsed_ms) | \(.baseline_elapsed_ms) | \(.performance_status) |"))
	| join("\n")' "$evaluation_file")"

failures_block="$(jq -r '
	(.results | map(select(.correctness_status != "pass" or .performance_status == "fail" or .performance_status == "infra_fail"))) as $fails
	| if .summary.total == 0 then
		"- No scenarios executed"
	  elif ($fails | length) == 0 then
		"- None"
	  else
		$fails | map("- \(.scenario_id) / \(.table_name): \(.reason)") | join("\n")
	  end' "$evaluation_file")"

report_content="$(cat "$template_file")"
report_content="${report_content//'{{pg_major}}'/$pg_major}"
report_content="${report_content//'{{verdict}}'/$verdict}"
report_content="${report_content//'{{output_dir}}'/$FB_RELEASE_GATE_OUTPUT_DIR}"
report_content="${report_content//'{{golden_file}}'/$golden_file}"
report_content="${report_content//'{{archive_dir}}'/$archive_dir}"
report_content="${report_content//'{{archive_mode}}'/$archive_mode}"
report_content="${report_content//'{{archive_command}}'/$archive_command}"
report_content="${report_content//'{{summary}}'/$summary_block}"
report_content="${report_content//'{{correctness}}'/$correctness_block}"
report_content="${report_content//'{{performance}}'/$performance_block}"
report_content="${report_content//'{{failures}}'/$failures_block}"

printf '%s\n' "$report_content" > "$report_file"
chmod 0666 "$report_file" 2>/dev/null || true
fb_release_gate_log "report written to $report_file"
