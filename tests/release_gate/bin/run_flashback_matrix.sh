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

run_sql_to_csv() {
	local sql="$1"
	local csv_path="$2"
	local mode="${3:-query}"
	local cmd

	case "$mode" in
		query)
			printf -v cmd '%q --csv -X -v ON_ERROR_STOP=1 -p %q -U %q -d %q -c %q > %q' \
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
	fb_release_gate_run_as_os_user "$cmd"
}

run_query_case() {
	local scenario_id="$1"
	local truth_scenario_id="$2"
	local table_name="$3"
	local target_ts="$4"
	local path_kind="$5"
	local table_class="$6"
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
	local ctas_table

	typed_null="$(fb_release_gate_typed_null_expr "$schema_name" "$table_name")"
	qualified_name="$(fb_release_gate_sql_qualified_name "$schema_name" "$table_name")"
	order_by="$(fb_release_gate_table_pk_order "$FB_RELEASE_GATE_DBNAME" "$schema_name" "$table_name")"
	[[ -n "$order_by" ]] || fb_release_gate_fail "missing primary key order for ${schema_name}.${table_name}"

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
				run_sql_to_csv "$sql" "$current_csv" query
				;;
			copy)
				run_sql_to_csv "$sql" "$current_csv" copy
				;;
			ctas)
				fb_release_gate_psql_file "$FB_RELEASE_GATE_DBNAME" "$FB_RELEASE_GATE_SQL_DIR/create_flashback_ctas.sql" \
					-v ctas_table="$ctas_table" \
					-v typed_null_expr="$typed_null" \
					-v target_ts="$target_ts" >/dev/null
				run_sql_to_csv "select * from ${ctas_table} order by ${order_by};" "$current_csv" query
				fb_release_gate_psql_file "$FB_RELEASE_GATE_DBNAME" "$FB_RELEASE_GATE_SQL_DIR/drop_flashback_ctas.sql" \
					-v ctas_table="$ctas_table" >/dev/null
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
		--arg table_class "$table_class" \
		--arg baseline_key "$baseline_key" \
		--argjson row_count "$row_count" \
		--argjson gate_elapsed_ms "$gate_elapsed_ms" \
		--argjson measured_elapsed_ms "$(printf '%s\n' "${measured[@]}" | jq -R . | jq -s .)" \
		'{scenario_id:$scenario_id, truth_scenario_id:$truth_scenario_id, schema_name:$schema_name, table_name:$table_name, target_ts:$target_ts, path_kind:$path_kind, csv_path:$csv_path, sha256:$sha256, row_count:$row_count, gate_elapsed_ms:$gate_elapsed_ms, measured_elapsed_ms:$measured_elapsed_ms, table_class:$table_class, baseline_key:$baseline_key, dry_run:false}')"
	fb_release_gate_manifest_append "$results_manifest" "$result_json"
}

[[ -f "$truth_manifest" ]] || fb_release_gate_fail "missing truth manifest: $truth_manifest"
fb_release_gate_manifest_init "$results_manifest"

while IFS= read -r truth_entry; do
	scenario_id="$(printf '%s' "$truth_entry" | jq -r '.scenario_id')"
	table_name="$(printf '%s' "$truth_entry" | jq -r '.table_name')"
	target_ts="$(printf '%s' "$truth_entry" | jq -r '.target_ts')"
	table_class="$(printf '%s' "$truth_entry" | jq -r '.table_class')"
	run_query_case "$scenario_id" "$scenario_id" "$table_name" "$target_ts" "query" "$table_class"
done < <(jq -c '.[]' "$truth_manifest")

materialization_truth="$(jq -c --arg table_name "$FB_RELEASE_GATE_TARGET_TABLE_NAME" '.[] | select(.scenario_id == "random_flashback_1" and .table_name == $table_name) | . ' "$truth_manifest" | head -n 1)"
if [[ -n "$materialization_truth" ]]; then
	table_name="$(printf '%s' "$materialization_truth" | jq -r '.table_name')"
	target_ts="$(printf '%s' "$materialization_truth" | jq -r '.target_ts')"
	table_class="$(printf '%s' "$materialization_truth" | jq -r '.table_class')"
	run_query_case "copy_to_flashback" "random_flashback_1" "$table_name" "$target_ts" "copy" "$table_class"
	run_query_case "ctas_flashback" "random_flashback_1" "$table_name" "$target_ts" "ctas" "$table_class"
fi

fb_release_gate_log "flashback matrix finished"
