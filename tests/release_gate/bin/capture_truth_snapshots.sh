#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

DRY_RUN=0
MODE="all"

while [[ $# -gt 0 ]]; do
	case "$1" in
		--dry-run)
			DRY_RUN=1
			;;
		--mode)
			shift
			[[ $# -gt 0 ]] || fb_release_gate_fail "--mode requires a value"
			MODE="$1"
			;;
		*)
			fb_release_gate_fail "unknown argument: $1"
			;;
	esac
	shift
done

fb_release_gate_require_cmd jq
fb_release_gate_require_cmd awk
fb_release_gate_require_cmd sha256sum

schema_name="$(fb_release_gate_scenario_schema)"
target_table_name="$FB_RELEASE_GATE_TARGET_TABLE_NAME"
truth_random_manifest="$(fb_release_gate_json_path truth_random_manifest)"
truth_dml_manifest="$(fb_release_gate_json_path truth_dml_manifest)"
truth_manifest="$(fb_release_gate_json_path truth_manifest)"
schedule_file="$(fb_release_gate_json_path random_snapshot_schedule)"
random_log="$(fb_release_gate_text_path random_snapshot_capture)"
dml_log="$(fb_release_gate_text_path dml_snapshot_capture)"
runtime_file="$(fb_release_gate_json_path dml_pressure_runtime)"
scenario_matrix_file="$FB_RELEASE_GATE_CONFIG_DIR/scenario_matrix.json"

run_existing_table_import() {
	local table_name="$1"
	local rows_to_import="$2"
	local suffix="$3"
	local payload
	local job
	local job_id

	payload="$(jq -cn \
		--arg mode "existing_table" \
		--arg schema "$schema_name" \
		--arg table "$table_name" \
		--argjson rows_to_import "$rows_to_import" \
		--argjson batch_size "$FB_RELEASE_GATE_SIM_BATCH_SIZE" \
		--argjson import_workers "$FB_RELEASE_GATE_SIM_IMPORT_WORKERS" \
		'{mode:$mode, schema:$schema, table:$table, rows_to_import:$rows_to_import, batch_size:$batch_size, import_workers:$import_workers, truncate_before_import:false}')"
	printf '%s\n' "$payload" > "$(fb_release_gate_json_path "dml_import_request_${suffix}")"
	fb_release_gate_sim_connect > "$(fb_release_gate_json_path simulator_connect)"
	job="$(fb_release_gate_sim_request POST /api/generate/jobs "$payload")"
	printf '%s\n' "$job" > "$(fb_release_gate_json_path "dml_import_response_${suffix}")"
	job_id="$(printf '%s' "$job" | jq -r '.id')"
	[[ -n "$job_id" && "$job_id" != "null" ]] || fb_release_gate_fail "existing_table import missing job id for ${suffix}"
	fb_release_gate_sim_wait_job "$job_id" "$(fb_release_gate_json_path "dml_import_final_${suffix}")"
}

capture_bundle() {
	local scenario_id="$1"
	shift
	local tables=("$@")
	local script_file
	local capture_output
	local capture_ts=""
	local table_name
	local csv_path
	local qualified_table
	local order_by
	local table_class
	local sha256
	local row_count
	local item_json

	script_file="$(fb_release_gate_shared_tmp_file "capture_${scenario_id}")"
	{
		echo '\set ON_ERROR_STOP on'
		echo 'begin isolation level repeatable read;'
		echo "select to_char(transaction_timestamp() at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US+00') as capture_ts \\gset"
		for table_name in "${tables[@]}"; do
			qualified_table="$(fb_release_gate_sql_qualified_name "$schema_name" "$table_name")"
			order_by="$(fb_release_gate_table_pk_order "$FB_RELEASE_GATE_DBNAME" "$schema_name" "$table_name")"
			[[ -n "$order_by" ]] || fb_release_gate_fail "missing primary key order for ${schema_name}.${table_name}"
			csv_path="$(fb_release_gate_output_path "csv/truth/${scenario_id}__${table_name}.csv")"
			printf "\\copy (select * from %s order by %s) to %s with (format csv, header true)\n" \
				"$qualified_table" \
				"$order_by" \
				"'$csv_path'"
		done
		echo 'commit;'
		echo '\echo capture_ts=:capture_ts'
	} > "$script_file"

	capture_output="$(fb_release_gate_psql_file "$FB_RELEASE_GATE_DBNAME" "$script_file")"
	rm -f "$script_file"
	printf '%s\n' "$capture_output" >> "$random_log"
	printf '%s\n' "$capture_output" >> "$dml_log"
	capture_ts="$(printf '%s\n' "$capture_output" | awk -F= '/^capture_ts=/{print $2; exit}')"
	[[ -n "$capture_ts" ]] || fb_release_gate_fail "could not parse capture_ts for scenario ${scenario_id}"

	for table_name in "${tables[@]}"; do
		csv_path="$(fb_release_gate_output_path "csv/truth/${scenario_id}__${table_name}.csv")"
		sha256="$(fb_release_gate_sha256_file "$csv_path")"
		row_count="$(fb_release_gate_csv_row_count "$csv_path")"
		if [[ "$table_name" == "$target_table_name" ]]; then
			table_class="large_5gb_target"
		else
			table_class="medium"
		fi
		item_json="$(jq -cn \
			--arg scenario_id "$scenario_id" \
			--arg target_ts "$capture_ts" \
			--arg schema_name "$schema_name" \
			--arg table_name "$table_name" \
			--arg qualified_name "${schema_name}.${table_name}" \
			--arg csv_path "$csv_path" \
			--arg sha256 "$sha256" \
			--arg table_class "$table_class" \
			--arg mode "$MODE" \
			--argjson row_count "$row_count" \
			'{scenario_id:$scenario_id, target_ts:$target_ts, schema_name:$schema_name, table_name:$table_name, qualified_name:$qualified_name, csv_path:$csv_path, sha256:$sha256, row_count:$row_count, table_class:$table_class, capture_mode:$mode}')"
		if [[ "$scenario_id" == random_flashback_* ]]; then
			fb_release_gate_manifest_append "$truth_random_manifest" "$item_json"
		else
			fb_release_gate_manifest_append "$truth_dml_manifest" "$item_json"
		fi
	done
}

merge_manifests() {
	local tmp

	tmp="$(mktemp)"
	jq -s 'add' "$truth_random_manifest" "$truth_dml_manifest" > "$tmp"
	mv "$tmp" "$truth_manifest"
	chmod 0666 "$truth_manifest" 2>/dev/null || true
}

run_random_snapshots() {
	local -a scenario_ids=()
	local -a offsets=()
	local -a tables=()
	local start_ts
	local duration_sec
	local start_epoch
	local idx
	local offset_sec
	local scenario_id
	local scheduled_ts
	local schedule_item
	local job_id
	local job_status
	local table_name

	[[ -f "$runtime_file" ]] || fb_release_gate_fail "missing runtime file: $runtime_file"
	start_ts="$(jq -r '.pressure_start_ts' "$runtime_file")"
	duration_sec="$(jq -r '.duration_sec' "$runtime_file")"
	job_id="$(jq -r '.job_id' "$runtime_file")"
	start_epoch="$(fb_release_gate_timestamp_epoch "$start_ts")"
	mapfile -t scenario_ids < <(jq -r '.random_flashbacks[]' "$scenario_matrix_file")
	mapfile -t offsets < <(fb_release_gate_random_offsets "${#scenario_ids[@]}" "$duration_sec" "$FB_RELEASE_GATE_RANDOM_SEED" "$FB_RELEASE_GATE_RANDOM_SNAPSHOT_MARGIN_SEC")

	fb_release_gate_manifest_init "$truth_random_manifest"
	fb_release_gate_manifest_init "$schedule_file"
	tables=("$target_table_name")
	while IFS= read -r table_name; do
		tables+=("$table_name")
	done < <(fb_release_gate_parse_csv_list "$FB_RELEASE_GATE_MEDIUM_TABLE_NAMES")

	for idx in "${!scenario_ids[@]}"; do
		scenario_id="${scenario_ids[$idx]}"
		offset_sec="${offsets[$idx]}"
		scheduled_ts="$(fb_release_gate_timestamp_add_sec "$start_ts" "$offset_sec")"
		schedule_item="$(jq -cn \
			--arg scenario_id "$scenario_id" \
			--arg scheduled_ts "$scheduled_ts" \
			--argjson offset_sec "$offset_sec" \
			'{scenario_id:$scenario_id, scheduled_ts:$scheduled_ts, offset_sec:$offset_sec}')"
		fb_release_gate_manifest_append "$schedule_file" "$schedule_item"

		if [[ "$DRY_RUN" -eq 1 ]]; then
			fb_release_gate_log "would capture random snapshot scenario=${scenario_id} scheduled_ts=${scheduled_ts}"
			continue
		fi

		fb_release_gate_wait_until_epoch $((start_epoch + offset_sec))
		job_status="$(printf '%s' "$(fb_release_gate_sim_job_get "$job_id")" | jq -r '.status')"
		case "$job_status" in
			running|done) ;;
			*)
				fb_release_gate_fail "dml pressure job ${job_id} not running at ${scenario_id}, status=${job_status}"
				;;
		esac
		capture_bundle "$scenario_id" "${tables[@]}"
		fb_release_gate_log "captured random truth snapshot scenario=${scenario_id}"
	done
}

run_dml_snapshots() {
	local table_name="$FB_RELEASE_GATE_DML_TABLE_NAME"
	local qualified_table
	local max_id
	local single_insert_prev_max
	local single_insert_id
	local bulk_start_id
	local bulk_end_id
	local bulk_prev_max
	local mixed_prev_max
	local mixed_start_id
	local mixed_end_id

	fb_release_gate_manifest_init "$truth_dml_manifest"
	qualified_table="$(fb_release_gate_sql_qualified_name "$schema_name" "$table_name")"

	if [[ "$DRY_RUN" -eq 1 ]]; then
		fb_release_gate_log "would execute deterministic dml snapshot scenarios on ${schema_name}.${table_name}"
		return 0
	fi

	max_id="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select coalesce(max(id), 0) from ${qualified_table};")"
	[[ -n "$max_id" ]] || fb_release_gate_fail "could not read max(id) from ${schema_name}.${table_name}"

	single_insert_prev_max="$max_id"
	run_existing_table_import "$table_name" 1 "single_insert"
	single_insert_id="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select max(id) from ${qualified_table};")"
	[[ "$single_insert_id" -gt "$single_insert_prev_max" ]] || fb_release_gate_fail "single insert did not advance max(id) on ${schema_name}.${table_name}"
	capture_bundle "single_insert_flashback" "$table_name"

	fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "
update ${qualified_table}
set remarks = 'release_gate single update', updated_at = current_timestamp
where id = (select min(id) from ${qualified_table});
"
	capture_bundle "single_update_flashback" "$table_name"

	fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "
delete from ${qualified_table}
where id = ${single_insert_id};
"
	capture_bundle "single_delete_flashback" "$table_name"

	bulk_prev_max="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select coalesce(max(id), 0) from ${qualified_table};")"
	run_existing_table_import "$table_name" 10000 "bulk_insert_10k"
	bulk_end_id="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select max(id) from ${qualified_table};")"
	bulk_start_id=$((bulk_prev_max + 1))
	[[ $((bulk_end_id - bulk_prev_max)) -eq 10000 ]] || fb_release_gate_fail "bulk insert expected 10000 rows on ${schema_name}.${table_name}, got $((bulk_end_id - bulk_prev_max))"
	capture_bundle "bulk_insert_10k_flashback" "$table_name"

	fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "
update ${qualified_table}
set remarks = 'release_gate bulk update', updated_at = current_timestamp
where id between ${bulk_start_id} and ${bulk_end_id};
"
	capture_bundle "bulk_update_10k_flashback" "$table_name"

	fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "
delete from ${qualified_table}
where id between ${bulk_start_id} and ${bulk_end_id};
"
	capture_bundle "bulk_delete_10k_flashback" "$table_name"

	mixed_prev_max="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select coalesce(max(id), 0) from ${qualified_table};")"
	mixed_start_id=$((mixed_prev_max + 1))
	run_existing_table_import "$table_name" 500 "mixed_insert"
	mixed_end_id="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "select max(id) from ${qualified_table};")"
	[[ $((mixed_end_id - mixed_start_id + 1)) -eq 500 ]] || fb_release_gate_fail "mixed insert expected 500 rows on ${schema_name}.${table_name}, got $((mixed_end_id - mixed_start_id + 1))"
	fb_release_gate_psql_sql "$FB_RELEASE_GATE_DBNAME" "
begin;
update ${qualified_table}
set remarks = 'release_gate mixed update', updated_at = current_timestamp
where id in (select id from ${qualified_table} order by id limit 500);
delete from ${qualified_table}
where id between ${mixed_start_id} and $((mixed_start_id + 249));
commit;
"
	capture_bundle "mixed_dml_flashback" "$table_name"
	fb_release_gate_log "captured deterministic dml truth snapshots on ${schema_name}.${table_name}"
}

[[ -f "$truth_random_manifest" ]] || fb_release_gate_manifest_init "$truth_random_manifest"
[[ -f "$truth_dml_manifest" ]] || fb_release_gate_manifest_init "$truth_dml_manifest"

case "$MODE" in
	random)
		run_random_snapshots
		;;
	dml)
		run_dml_snapshots
		;;
	all)
		run_random_snapshots
		run_dml_snapshots
		;;
	*)
		fb_release_gate_fail "unsupported mode: $MODE"
		;;
esac

merge_manifests
fb_release_gate_log "truth snapshot capture finished mode=${MODE}"
