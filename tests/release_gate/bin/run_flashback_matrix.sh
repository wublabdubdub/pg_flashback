#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

DRY_RUN=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dry-run)
			DRY_RUN=1
			;;
		*)
			fb_release_gate_fail "unknown argument: $1"
			;;
	esac
	shift
done

fb_release_gate_require_cmd jq
fb_release_gate_require_cmd sha256sum

truth_manifest="$(fb_release_gate_json_path truth_manifest)"
results_manifest="$(fb_release_gate_json_path flashback_results)"
scenario_matrix_file="$FB_RELEASE_GATE_CONFIG_DIR/scenario_matrix.json"
schema_name="$(fb_release_gate_scenario_schema)"
memory_limit_retry_pattern='estimated flashback working set exceeds pg_flashback.memory_limit'
flashback_memory_limit_value="${FB_RELEASE_GATE_FLASHBACK_MEMORY_LIMIT:-6GB}"

build_run_sql_to_csv_cmd() {
	local sql="$1"
	local csv_path="$2"
	local mode="${3:-query}"
	local cmd

	case "$mode" in
		query)
			printf -v cmd '%q --csv -X -q -v ON_ERROR_STOP=1 -p %q -U %q -d %q -c %q > %q' \
				"$FB_RELEASE_GATE_PSQL" \
				"$FB_RELEASE_GATE_PGPORT" \
				"$FB_RELEASE_GATE_PGUSER" \
				"$FB_RELEASE_GATE_DBNAME" \
				"$sql" \
				"$csv_path"
			;;
		copy)
			printf -v cmd '%q -X -q -v ON_ERROR_STOP=1 -p %q -U %q -d %q -c %q > %q' \
				"$FB_RELEASE_GATE_PSQL" \
				"$FB_RELEASE_GATE_PGPORT" \
				"$FB_RELEASE_GATE_PGUSER" \
				"$FB_RELEASE_GATE_DBNAME" \
				"$sql" \
				"$csv_path"
			;;
		*)
			fb_release_gate_fail "unsupported export mode: $mode"
			;;
	esac

	printf '%s\n' "$cmd"
}

build_psql_file_cmd() {
	local dbname="$1"
	local file="$2"
	shift 2
	local cmd
	local arg

	printf -v cmd '%q -X -q -A -t -v ON_ERROR_STOP=1 -p %q -U %q -d %q -f %q' \
		"$FB_RELEASE_GATE_PSQL" \
		"$FB_RELEASE_GATE_PGPORT" \
		"$FB_RELEASE_GATE_PGUSER" \
		"$dbname" \
		"$file"
	for arg in "$@"; do
		printf -v cmd '%s %q' "$cmd" "$arg"
	done

	printf '%s\n' "$cmd"
}

run_shell_cmd_with_stderr_log() {
	local cmd="$1"
	local stderr_log="$2"
	local rc

	set +e
	fb_release_gate_run_as_os_user "$cmd" 2> >(tee "$stderr_log" >&2)
	rc=$?
	set -e

	return "$rc"
}

run_flashback_cmd_with_memory_retry() {
	local cmd="$1"
	local label="$2"
	local extra_pgoptions="${3:-}"
	local initial_cmd
	local stderr_log
	local retry_cmd
	local retry_pgoptions

	retry_pgoptions="-c pg_flashback.memory_limit=${flashback_memory_limit_value}"
	if [[ -n "$extra_pgoptions" ]]; then
		retry_pgoptions="${retry_pgoptions} ${extra_pgoptions}"
	fi
	printf -v initial_cmd 'env PGOPTIONS=%q %s' "$retry_pgoptions" "$cmd"

	stderr_log="$(fb_release_gate_shared_tmp_file "flashback_stderr")"
	if run_shell_cmd_with_stderr_log "$initial_cmd" "$stderr_log"; then
		rm -f "$stderr_log"
		return 0
	fi

	if ! grep -Fq "$memory_limit_retry_pattern" "$stderr_log"; then
		rm -f "$stderr_log"
		return 1
	fi

	rm -f "$stderr_log"
	fb_release_gate_log "flashback case ${label} hit memory_limit preflight; retrying with pg_flashback.memory_limit=${flashback_memory_limit_value}"

	printf -v retry_cmd 'env PGOPTIONS=%q %s' "$retry_pgoptions" "$cmd"

	stderr_log="$(fb_release_gate_shared_tmp_file "flashback_stderr_retry")"
	if run_shell_cmd_with_stderr_log "$retry_cmd" "$stderr_log"; then
		rm -f "$stderr_log"
		return 0
	fi

	rm -f "$stderr_log"
	return 1
}

run_sql_to_csv() {
	local sql="$1"
	local csv_path="$2"
	local mode="${3:-query}"
	local label="$4"
	local extra_pgoptions="${5:-}"
	local cmd

	cmd="$(build_run_sql_to_csv_cmd "$sql" "$csv_path" "$mode")"
	run_flashback_cmd_with_memory_retry "$cmd" "$label" "$extra_pgoptions"
}

run_query_case() {
	local scenario_id="$1"
	local truth_scenario_id="$2"
	local table_name="$3"
	local target_ts="$4"
	local target_snapshot="$5"
	local path_kind="$6"
	local table_class="$7"
	local typed_null
	local qualified_name
	local order_by
	local sql
	local final_csv
	local run_idx
	local elapsed_ms
	local start_ms
	local end_ms
	local current_csv
	local -a measured=()
	local gate_elapsed_ms
	local sha256
	local row_count
	local baseline_key
	local result_json
	local correctness_eval_json
	local correctness_status
	local correctness_reason
	local truth_sha256
	local truth_row_count
	local diff_path
	local ctas_table
	local ctas_sql
	local ctas_create_cmd
	local ctas_drop_cmd
	local flashback_pgoptions=""

	typed_null="$(fb_release_gate_typed_null_expr "$schema_name" "$table_name")"
	qualified_name="$(fb_release_gate_sql_qualified_name "$schema_name" "$table_name")"
	order_by="$(fb_release_gate_table_pk_order "$FB_RELEASE_GATE_DBNAME" "$schema_name" "$table_name")"
	[[ -n "$order_by" ]] || fb_release_gate_fail "missing primary key order for ${schema_name}.${table_name}"
	if [[ -n "$target_snapshot" && "$target_snapshot" != "null" ]]; then
		flashback_pgoptions="-c pg_flashback.target_snapshot=${target_snapshot}"
	fi

	case "$path_kind" in
		query)
			sql="select * from pg_flashback(${typed_null}, $(fb_release_gate_sql_literal "$target_ts")) order by ${order_by};"
			final_csv="$(fb_release_gate_output_path "csv/flashback/${scenario_id}__${table_name}.csv")"
			;;
		copy)
			sql="copy (select * from pg_flashback(${typed_null}, $(fb_release_gate_sql_literal "$target_ts")) order by ${order_by}) to stdout with (format csv, header true);"
			final_csv="$(fb_release_gate_output_path "csv/materialized/${scenario_id}__${table_name}.csv")"
			;;
		ctas)
			ctas_table="$(fb_release_gate_sql_qualified_name "release_gate_results" "${scenario_id}_${table_name}")"
			ctas_sql="drop table if exists ${ctas_table}; create unlogged table ${ctas_table} as select * from pg_flashback(${typed_null}, $(fb_release_gate_sql_literal "$target_ts"));"
			final_csv="$(fb_release_gate_output_path "csv/materialized/${scenario_id}__${table_name}.csv")"
			;;
		*)
			fb_release_gate_fail "unsupported path_kind: $path_kind"
			;;
	esac

	if [[ "$DRY_RUN" -eq 1 ]]; then
		result_json="$(jq -cn \
			--arg scenario_id "$scenario_id" \
			--arg truth_scenario_id "$truth_scenario_id" \
			--arg table_name "$table_name" \
			--arg target_ts "$target_ts" \
			--arg path_kind "$path_kind" \
			--arg csv_path "$final_csv" \
			--arg table_class "$table_class" \
			'{scenario_id:$scenario_id, truth_scenario_id:$truth_scenario_id, table_name:$table_name, target_ts:$target_ts, path_kind:$path_kind, csv_path:$csv_path, table_class:$table_class, dry_run:true}')"
		fb_release_gate_manifest_append "$results_manifest" "$result_json"
		return 0
	fi

	if [[ "$path_kind" == "ctas" ]]; then
		fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" 'create schema if not exists release_gate_results;'
	fi

	for ((run_idx = 1; run_idx <= FB_RELEASE_GATE_WARMUP_RUNS + FB_RELEASE_GATE_MEASURED_RUNS; run_idx++)); do
		if (( run_idx == FB_RELEASE_GATE_WARMUP_RUNS + 1 )); then
			current_csv="$final_csv"
		else
			current_csv="$(fb_release_gate_shared_tmp_file "flashback_${scenario_id}_${table_name}")"
		fi

		start_ms="$(date +%s%3N)"
		case "$path_kind" in
			query)
				fb_release_gate_log "flashback sql [${scenario_id}:${table_name}:query:run${run_idx}] ${sql}"
				run_sql_to_csv "$sql" "$current_csv" query "${scenario_id}:${table_name}:query:run${run_idx}" "$flashback_pgoptions"
				;;
			copy)
				fb_release_gate_log "flashback sql [${scenario_id}:${table_name}:copy:run${run_idx}] ${sql}"
				run_sql_to_csv "$sql" "$current_csv" copy "${scenario_id}:${table_name}:copy:run${run_idx}" "$flashback_pgoptions"
				;;
				ctas)
					fb_release_gate_log "flashback sql [${scenario_id}:${table_name}:ctas-create:run${run_idx}] ${ctas_sql}"
					ctas_create_cmd="$(build_psql_file_cmd "$FB_RELEASE_GATE_DBNAME" "$FB_RELEASE_GATE_SQL_DIR/create_flashback_ctas.sql" \
						-v ctas_table="$ctas_table" \
						-v typed_null_expr="$typed_null" \
						-v target_ts="$target_ts")"
					printf -v ctas_create_cmd '%s >/dev/null' "$ctas_create_cmd"
					run_flashback_cmd_with_memory_retry "$ctas_create_cmd" "${scenario_id}:${table_name}:ctas-create:run${run_idx}" "$flashback_pgoptions"
					run_sql_to_csv "select * from ${ctas_table} order by ${order_by};" "$current_csv" query "${scenario_id}:${table_name}:ctas-read:run${run_idx}"
					ctas_drop_cmd="$(build_psql_file_cmd "$FB_RELEASE_GATE_DBNAME" "$FB_RELEASE_GATE_SQL_DIR/drop_flashback_ctas.sql" \
						-v ctas_table="$ctas_table")"
					printf -v ctas_drop_cmd '%s >/dev/null' "$ctas_drop_cmd"
					fb_release_gate_run_as_os_user "$ctas_drop_cmd"
				;;
		esac
		end_ms="$(date +%s%3N)"
		elapsed_ms=$((end_ms - start_ms))

		if (( run_idx > FB_RELEASE_GATE_WARMUP_RUNS )); then
			measured+=("$elapsed_ms")
		fi
		if [[ "$current_csv" != "$final_csv" ]]; then
			rm -f "$current_csv"
		fi
	done

	gate_elapsed_ms="${measured[0]}"
	for elapsed_ms in "${measured[@]}"; do
		if (( elapsed_ms < gate_elapsed_ms )); then
			gate_elapsed_ms="$elapsed_ms"
		fi
	done

	sha256="$(fb_release_gate_sha256_file "$final_csv")"
	row_count="$(fb_release_gate_csv_row_count "$final_csv")"
	correctness_eval_json="$(fb_release_gate_eval_correctness_json \
		"$scenario_id" \
		"$truth_manifest" \
		"$truth_scenario_id" \
		"$table_name" \
		"$sha256" \
		"$row_count" \
		"$final_csv")"
	correctness_status="$(printf '%s' "$correctness_eval_json" | jq -r '.correctness_status')"
	correctness_reason="$(printf '%s' "$correctness_eval_json" | jq -r '.reason')"
	truth_sha256="$(printf '%s' "$correctness_eval_json" | jq -r '.truth_sha256')"
	truth_row_count="$(printf '%s' "$correctness_eval_json" | jq -r '.truth_row_count')"
	diff_path="$(printf '%s' "$correctness_eval_json" | jq -r '.diff_path')"
	if [[ -n "$correctness_reason" ]]; then
		fb_release_gate_log "accuracy [${scenario_id}:${table_name}:${path_kind}] ${correctness_status} reason=${correctness_reason} result_row_count=${row_count} truth_row_count=${truth_row_count} result_sha256=${sha256} truth_sha256=${truth_sha256} diff_path=${diff_path}"
	else
		fb_release_gate_log "accuracy [${scenario_id}:${table_name}:${path_kind}] ${correctness_status} result_row_count=${row_count} truth_row_count=${truth_row_count} result_sha256=${sha256} truth_sha256=${truth_sha256}"
	fi
	baseline_key="${path_kind}:${scenario_id}:${schema_name}.${table_name}"
	result_json="$(jq -cn \
		--arg scenario_id "$scenario_id" \
		--arg truth_scenario_id "$truth_scenario_id" \
		--arg schema_name "$schema_name" \
		--arg table_name "$table_name" \
		--arg target_ts "$target_ts" \
		--arg path_kind "$path_kind" \
		--arg csv_path "$final_csv" \
		--arg sha256 "$sha256" \
		--arg correctness_status "$correctness_status" \
		--arg correctness_reason "$correctness_reason" \
		--arg diff_path "$diff_path" \
		--arg truth_sha256 "$truth_sha256" \
		--arg table_class "$table_class" \
		--arg baseline_key "$baseline_key" \
		--argjson row_count "$row_count" \
		--argjson truth_row_count "$truth_row_count" \
		--argjson gate_elapsed_ms "$gate_elapsed_ms" \
		--argjson measured_elapsed_ms "$(printf '%s\n' "${measured[@]}" | jq -R . | jq -s .)" \
		'{scenario_id:$scenario_id, truth_scenario_id:$truth_scenario_id, schema_name:$schema_name, table_name:$table_name, target_ts:$target_ts, path_kind:$path_kind, csv_path:$csv_path, sha256:$sha256, row_count:$row_count, truth_sha256:$truth_sha256, truth_row_count:$truth_row_count, correctness_status:$correctness_status, correctness_reason:$correctness_reason, diff_path:$diff_path, gate_elapsed_ms:$gate_elapsed_ms, measured_elapsed_ms:$measured_elapsed_ms, table_class:$table_class, baseline_key:$baseline_key, dry_run:false}')"
	fb_release_gate_manifest_append "$results_manifest" "$result_json"
}

[[ -f "$truth_manifest" ]] || fb_release_gate_fail "missing truth manifest: $truth_manifest"
fb_release_gate_manifest_init "$results_manifest"
rm -f "$(fb_release_gate_output_path logs)"/diff_*.diff 2>/dev/null || true

while IFS= read -r truth_entry; do
	scenario_id="$(printf '%s' "$truth_entry" | jq -r '.scenario_id')"
	table_name="$(printf '%s' "$truth_entry" | jq -r '.table_name')"
	target_ts="$(printf '%s' "$truth_entry" | jq -r '.target_ts')"
	target_snapshot="$(printf '%s' "$truth_entry" | jq -r '.target_snapshot // empty')"
	table_class="$(printf '%s' "$truth_entry" | jq -r '.table_class')"
	run_query_case "$scenario_id" "$scenario_id" "$table_name" "$target_ts" "$target_snapshot" "query" "$table_class"
done < <(jq -c '.[]' "$truth_manifest")

materialization_truth="$(jq -c --arg table_name "$FB_RELEASE_GATE_TARGET_TABLE_NAME" '.[] | select(.scenario_id == "random_flashback_1" and .table_name == $table_name) | . ' "$truth_manifest" | head -n 1)"
if [[ -n "$materialization_truth" ]]; then
	table_name="$(printf '%s' "$materialization_truth" | jq -r '.table_name')"
	target_ts="$(printf '%s' "$materialization_truth" | jq -r '.target_ts')"
	target_snapshot="$(printf '%s' "$materialization_truth" | jq -r '.target_snapshot // empty')"
	table_class="$(printf '%s' "$materialization_truth" | jq -r '.table_class')"
	run_query_case "copy_to_flashback" "random_flashback_1" "$table_name" "$target_ts" "$target_snapshot" "copy" "$table_class"
	run_query_case "ctas_flashback" "random_flashback_1" "$table_name" "$target_ts" "$target_snapshot" "ctas" "$table_class"
fi

fb_release_gate_log "flashback matrix finished"
