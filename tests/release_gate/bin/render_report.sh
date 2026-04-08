#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

fb_release_gate_require_cmd jq

evaluation_file="$(fb_release_gate_json_path gate_evaluation)"
environment_file="$(fb_release_gate_json_path environment)"
truth_manifest_file="$(fb_release_gate_json_path truth_manifest)"
schedule_file="$(fb_release_gate_json_path random_snapshot_schedule)"
report_file="$(fb_release_gate_output_path reports/release_gate_report.md)"

[[ -f "$evaluation_file" ]] || fb_release_gate_fail "missing evaluation file: $evaluation_file"
[[ -f "$environment_file" ]] || fb_release_gate_fail "missing environment file: $environment_file"

tmp_truth_file=""
tmp_schedule_file=""

if [[ -f "$truth_manifest_file" ]]; then
	truth_source="$truth_manifest_file"
else
	tmp_truth_file="$(mktemp)"
	printf '[]\n' > "$tmp_truth_file"
	truth_source="$tmp_truth_file"
fi

if [[ -f "$schedule_file" ]]; then
	schedule_source="$schedule_file"
else
	tmp_schedule_file="$(mktemp)"
	printf '[]\n' > "$tmp_schedule_file"
	schedule_source="$tmp_schedule_file"
fi

cleanup() {
	rm -f "${tmp_truth_file:-}" "${tmp_schedule_file:-}"
}
trap cleanup EXIT

report_content="$(jq -nr \
	--slurpfile eval "$evaluation_file" \
	--slurpfile env "$environment_file" \
	--slurpfile truth "$truth_source" \
	--slurpfile schedule "$schedule_source" '
		def path_label($value):
			if $value == "query" then "查询"
			elif $value == "copy" then "COPY TO"
			elif $value == "ctas" then "CTAS"
			else ($value // "-")
			end;
		def correctness_label($value):
			if $value == "pass" then "通过"
			elif $value == "fail" then "失败"
			elif $value == "infra_fail" then "基础设施失败"
			else ($value // "-")
			end;
		def performance_label($value):
			if $value == "pass" then "通过"
			elif $value == "fail" then "回归"
			elif $value == "missing_baseline" then "未评估"
			elif $value == "skipped" then "跳过"
			elif $value == "infra_fail" then "基础设施失败"
			else ($value // "-")
			end;
		def nonempty($value):
			if ($value // "") == "" then "-" else $value end;

		($eval[0]) as $evaluation |
		($env[0]) as $environment |
		($truth[0] // []) as $truth_manifest |
		($schedule[0] // []) as $schedule |
		($evaluation.results // []) as $results |
		($truth_manifest | length) as $truth_entries |
		($truth_manifest | map(select((.target_snapshot // "") != "")) | length) as $truth_snapshot_entries |
		[
			"# Release Gate 最终报告",
			"",
			"## 一页结论",
			"",
			"- 判定：\($evaluation.verdict)",
			"- PostgreSQL 主版本：\($evaluation.pg_major)",
			"- 输出目录：\($evaluation.output_dir)",
			"- Golden 基线文件：\($evaluation.golden_file)",
			"- Archive Dir：\($environment.archive_dir // "-")",
			"- Archive Mode：\($environment.archive_mode // "-")",
			"- Archive Command：\($environment.archive_command // "-")",
			"- 正确性：\($evaluation.summary.correctness_passed // 0) 通过 / \($evaluation.summary.correctness_failures // 0) 失败 / \($evaluation.summary.correctness_infra_failures // 0) 基础设施失败",
			"- 性能：\($evaluation.summary.performance_regressions // 0) 回归 / \($evaluation.summary.performance_missing_baseline // 0) 未评估 / \($evaluation.summary.performance_skipped // 0) 跳过 / \($evaluation.summary.performance_infra_failures // 0) 基础设施失败",
			"- truth manifest：\($truth_entries) 条，target_snapshot 覆盖率：\($truth_snapshot_entries)/\($truth_entries)",
			(if ($truth_entries > 0 and $truth_snapshot_entries < $truth_entries)
			 then "- 风险提示：本轮 truth manifest 未完整携带 `target_snapshot`，这通常意味着并未按最新 MVCC snapshot 口径重新采集 truth。"
			 else empty
			 end),
			"",
			"## 测试过程",
			"",
			"### 阶段概览",
			"",
			"| 阶段 | 当前产物观察 | 备注 |",
			"| --- | --- | --- |",
			"| 环境检查 | archive_mode=\($environment.archive_mode // "-") | archive_command=\(($environment.archive_command // "-") | gsub("\n"; " ")) |",
			"| 随机快照计划 | \($schedule | length) 个时间点 | 来自 `random_snapshot_schedule.json` |",
			"| Truth 采集 | \($truth_entries) 条 manifest | 包含 random + dml snapshot |",
			"| Flashback 执行矩阵 | \($evaluation.summary.total // ($results | length)) 条 case | 来自 `flashback_results.json` |",
			"| Gate 评估 | verdict=\($evaluation.verdict) | 来自 `gate_evaluation.json` |",
			"",
			"### 随机时间点快照",
			"",
			(if (($schedule | length) == 0)
			 then "没有随机快照计划产物。"
			 else (( 
				[
					"| 场景 | 计划时间(UTC) | 实际 target_ts(UTC) | 是否携带 target_snapshot |",
					"| --- | --- | --- | --- |"
				] + (
					$schedule | map(
						. as $item |
						($truth_manifest | map(select(.scenario_id == $item.scenario_id)) | .[0]) as $truth_item |
						"| \($item.scenario_id) | \($item.scheduled_ts // "-") | \($truth_item.target_ts // "-") | " +
						(if (($truth_item.target_snapshot // "") != "") then "是" else "否" end) + " |"
						)
					)
			 ) | join("\n"))
			 end),
			"",
			"### 场景执行矩阵",
			"",
			(if (($results | length) == 0)
			 then "没有可渲染的执行结果。"
			 else ((
				[
					"| 场景 | 路径 | 表 | target_ts(UTC) | 正确性 | 性能 | 耗时(ms) |",
					"| --- | --- | --- | --- | --- | --- | ---: |"
				] + (
					$results | map(
						"| \(.scenario_id) | \(path_label(.path_kind)) | \(.table_name) | \(.target_ts // "-") | \(correctness_label(.correctness_status)) | \(performance_label(.performance_status)) | \(.gate_elapsed_ms // 0) |"
						)
					)
			 ) | join("\n"))
			 end),
			"",
			"## 最终结果",
			"",
			"- 总 case 数：\($evaluation.summary.total // ($results | length))",
			"- 正确性失败：\($evaluation.summary.correctness_failures // 0)",
			"- 正确性基础设施失败：\($evaluation.summary.correctness_infra_failures // 0)",
			"- 性能回归：\($evaluation.summary.performance_regressions // 0)",
			"- 性能未评估（缺少 golden baseline）：\($evaluation.summary.performance_missing_baseline // 0)",
			"- 性能跳过：\($evaluation.summary.performance_skipped // 0)",
			"- 性能基础设施失败：\($evaluation.summary.performance_infra_failures // 0)",
			"",
			"## 正确性失败明细",
			"",
			(if (($results | map(select(.correctness_status == "fail" or .correctness_status == "infra_fail")) | length) == 0)
			 then "本轮没有正确性失败。"
			 else ((
				[
					"| 场景 | 路径 | 表 | target_ts(UTC) | 结果行数 | Truth 行数 | 原因 | diff |",
					"| --- | --- | --- | --- | ---: | ---: | --- | --- |"
				] + (
					$results
					| map(select(.correctness_status == "fail" or .correctness_status == "infra_fail"))
					| map(
						"| \(.scenario_id) | \(path_label(.path_kind)) | \(.table_name) | \(.target_ts // "-") | \(.result_row_count // 0) | \(.truth_row_count // 0) | \(nonempty(.correctness_reason)) | \(nonempty(.diff_path)) |"
						)
					)
			 ) | join("\n"))
			 end),
			"",
			"## 性能结论",
			"",
			(if (($results | map(select(.performance_status == "fail")) | length) == 0)
			 then "本轮没有检测到性能回归。"
			 else ((
				[
					"| 场景 | 路径 | 表 | 当前耗时(ms) | 基线(ms) | 阈值 | 结论 |",
					"| --- | --- | --- | ---: | ---: | --- | --- |"
				] + (
					$results
					| map(select(.performance_status == "fail"))
					| map(
						"| \(.scenario_id) | \(path_label(.path_kind)) | \(.table_name) | \(.gate_elapsed_ms // 0) | \(.baseline_elapsed_ms // 0) | ratio>\(.ratio_threshold // 0) and +\(.absolute_threshold_ms // 0)ms | 回归 |"
						)
					)
			 ) | join("\n"))
			 end),
			"",
			"## 未评估与阻塞项",
			"",
			(if (($results | map(select(.performance_status == "missing_baseline")) | length) == 0)
			 then "本轮没有“缺少 golden baseline”导致的未评估项。"
			 else ((
				[
					"| 场景 | 路径 | 表 | 未评估原因 |",
					"| --- | --- | --- | --- |"
				] + (
					$results
					| map(select(.performance_status == "missing_baseline"))
					| map(
						"| \(.scenario_id) | \(path_label(.path_kind)) | \(.table_name) | 缺少 golden baseline |"
						)
					)
			 ) | join("\n"))
			 end),
			"",
			"## 附件路径",
			"",
			"- `gate_evaluation.json`: \($evaluation.output_dir)/json/gate_evaluation.json",
			"- `truth_manifest.json`: \($evaluation.output_dir)/json/truth_manifest.json",
			"- `flashback_results.json`: \($evaluation.output_dir)/json/flashback_results.json",
			"- 当前报告：\($evaluation.output_dir)/reports/release_gate_report.md"
		] | join("\n")
	')"

printf '%s\n' "$report_content" > "$report_file"
chmod 0666 "$report_file" 2>/dev/null || true
fb_release_gate_log "report written to $report_file"
