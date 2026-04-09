#!/usr/bin/env bash
set -euo pipefail

FB_RELEASE_GATE_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FB_RELEASE_GATE_ROOT="$FB_RELEASE_GATE_REPO_ROOT/tests/release_gate"
FB_RELEASE_GATE_BIN_DIR="$FB_RELEASE_GATE_ROOT/bin"
FB_RELEASE_GATE_SQL_DIR="$FB_RELEASE_GATE_ROOT/sql"
FB_RELEASE_GATE_CONFIG_DIR="$FB_RELEASE_GATE_ROOT/config"
FB_RELEASE_GATE_TEMPLATE_DIR="$FB_RELEASE_GATE_ROOT/templates"
FB_RELEASE_GATE_OUTPUT_ROOT="$FB_RELEASE_GATE_ROOT/output"

fb_release_gate_default_pg_port_from_major() {
	case "$1" in
		14) printf '%s\n' 5432 ;;
		15) printf '%s\n' 5532 ;;
		16) printf '%s\n' 5632 ;;
		17) printf '%s\n' 5732 ;;
		18) printf '%s\n' 5832 ;;
		*)
			printf '[release_gate] ERROR: unsupported PG major for release gate: %s\n' "$1" >&2
			exit 1
			;;
	esac
}

FB_RELEASE_GATE_PG_MAJOR="${FB_RELEASE_GATE_PG_MAJOR:-14}"
if [[ -z "${FB_RELEASE_GATE_ARCHIVE_ROOT+x}" ]]; then
	FB_RELEASE_GATE_ARCHIVE_ROOT="/walstorage"
	FB_RELEASE_GATE_ARCHIVE_ROOT_IS_DEFAULT=1
else
	FB_RELEASE_GATE_ARCHIVE_ROOT_IS_DEFAULT=0
fi
FB_RELEASE_GATE_PSQL="${FB_RELEASE_GATE_PSQL:-/home/${FB_RELEASE_GATE_PG_MAJOR}pg/local/bin/psql}"
FB_RELEASE_GATE_CREATEDB="${FB_RELEASE_GATE_CREATEDB:-/home/${FB_RELEASE_GATE_PG_MAJOR}pg/local/bin/createdb}"
FB_RELEASE_GATE_DROPDB="${FB_RELEASE_GATE_DROPDB:-/home/${FB_RELEASE_GATE_PG_MAJOR}pg/local/bin/dropdb}"
FB_RELEASE_GATE_PGPORT="${FB_RELEASE_GATE_PGPORT:-$(fb_release_gate_default_pg_port_from_major "$FB_RELEASE_GATE_PG_MAJOR")}"
FB_RELEASE_GATE_PGUSER="${FB_RELEASE_GATE_PGUSER:-${FB_RELEASE_GATE_PG_MAJOR}pg}"
FB_RELEASE_GATE_MAINT_DB="${FB_RELEASE_GATE_MAINT_DB:-postgres}"
FB_RELEASE_GATE_DBNAME="${FB_RELEASE_GATE_DBNAME:-alldb}"
FB_RELEASE_GATE_OUTPUT_DIR="${FB_RELEASE_GATE_OUTPUT_DIR:-$FB_RELEASE_GATE_OUTPUT_ROOT/latest}"
FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB="${FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB:-100}"
FB_RELEASE_GATE_OS_USER="${FB_RELEASE_GATE_OS_USER:-$FB_RELEASE_GATE_PGUSER}"
FB_RELEASE_GATE_SIM_BIN="${FB_RELEASE_GATE_SIM_BIN:-/root/alldbsimulator/bin/alldbsim}"
FB_RELEASE_GATE_SIM_LISTEN_ADDR="${FB_RELEASE_GATE_SIM_LISTEN_ADDR:-127.0.0.1:18080}"
FB_RELEASE_GATE_SIM_HEALTH_PATH="${FB_RELEASE_GATE_SIM_HEALTH_PATH:-/api/health}"
FB_RELEASE_GATE_SIM_DB_HOST="${FB_RELEASE_GATE_SIM_DB_HOST:-127.0.0.1}"
FB_RELEASE_GATE_SIM_DB_PORT="${FB_RELEASE_GATE_SIM_DB_PORT:-$FB_RELEASE_GATE_PGPORT}"
FB_RELEASE_GATE_SIM_DB_USER="${FB_RELEASE_GATE_SIM_DB_USER:-$FB_RELEASE_GATE_PGUSER}"
FB_RELEASE_GATE_SIM_DB_PASSWORD="${FB_RELEASE_GATE_SIM_DB_PASSWORD:-}"
FB_RELEASE_GATE_SIM_DB_NAME="${FB_RELEASE_GATE_SIM_DB_NAME:-$FB_RELEASE_GATE_DBNAME}"
FB_RELEASE_GATE_SIM_SCENARIO="${FB_RELEASE_GATE_SIM_SCENARIO:-oa}"
FB_RELEASE_GATE_SIM_TABLE_COUNT="${FB_RELEASE_GATE_SIM_TABLE_COUNT:-50}"
FB_RELEASE_GATE_SIM_ROWS_PER_TABLE="${FB_RELEASE_GATE_SIM_ROWS_PER_TABLE:-50000}"
FB_RELEASE_GATE_SIM_BATCH_SIZE="${FB_RELEASE_GATE_SIM_BATCH_SIZE:-1000}"
FB_RELEASE_GATE_SIM_IMPORT_WORKERS="${FB_RELEASE_GATE_SIM_IMPORT_WORKERS:-4}"
FB_RELEASE_GATE_SIM_DML_DURATION_SEC="${FB_RELEASE_GATE_SIM_DML_DURATION_SEC:-3600}"
FB_RELEASE_GATE_SIM_DML_WORKERS="${FB_RELEASE_GATE_SIM_DML_WORKERS:-20}"
FB_RELEASE_GATE_SIM_DML_RATE_LIMIT_OPS="${FB_RELEASE_GATE_SIM_DML_RATE_LIMIT_OPS:-2000}"
FB_RELEASE_GATE_TARGET_TABLE_NAME="${FB_RELEASE_GATE_TARGET_TABLE_NAME:-documents}"
FB_RELEASE_GATE_MEDIUM_TABLE_NAMES="${FB_RELEASE_GATE_MEDIUM_TABLE_NAMES:-users,meetings,approval_tasks}"
FB_RELEASE_GATE_DML_TABLE_NAME="${FB_RELEASE_GATE_DML_TABLE_NAME:-leave_requests}"
FB_RELEASE_GATE_TARGET_SIZE_BYTES="${FB_RELEASE_GATE_TARGET_SIZE_BYTES:-5368709120}"
FB_RELEASE_GATE_TARGET_GROW_ROWS_PER_BATCH="${FB_RELEASE_GATE_TARGET_GROW_ROWS_PER_BATCH:-100000}"
FB_RELEASE_GATE_RANDOM_SNAPSHOT_COUNT="${FB_RELEASE_GATE_RANDOM_SNAPSHOT_COUNT:-5}"
FB_RELEASE_GATE_RANDOM_SNAPSHOT_MARGIN_SEC="${FB_RELEASE_GATE_RANDOM_SNAPSHOT_MARGIN_SEC:-300}"
FB_RELEASE_GATE_WARMUP_RUNS="${FB_RELEASE_GATE_WARMUP_RUNS:-0}"
FB_RELEASE_GATE_MEASURED_RUNS="${FB_RELEASE_GATE_MEASURED_RUNS:-1}"
FB_RELEASE_GATE_RANDOM_SEED="${FB_RELEASE_GATE_RANDOM_SEED:-20260403}"

fb_release_gate_log() {
	printf '[release_gate][%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %z')" "$*"
}

fb_release_gate_fail() {
	printf '[release_gate][%s] ERROR: %s\n' "$(date '+%Y-%m-%d %H:%M:%S %z')" "$*" >&2
	exit 1
}

fb_release_gate_require_cmd() {
	command -v "$1" >/dev/null 2>&1 || fb_release_gate_fail "missing command: $1"
}

fb_release_gate_load_config() {
	local config_file="$FB_RELEASE_GATE_CONFIG_DIR/release_gate.conf"
	local archive_root_before="$FB_RELEASE_GATE_ARCHIVE_ROOT"

	if [[ -f "$config_file" ]]; then
		# shellcheck disable=SC1090
		source "$config_file"
	fi

	if [[ "$FB_RELEASE_GATE_ARCHIVE_ROOT" != "$archive_root_before" ]]; then
		FB_RELEASE_GATE_ARCHIVE_ROOT_IS_DEFAULT=0
	fi
}

fb_release_gate_run_as_os_user() {
	local cmd="$1"

	if [[ "$(id -un)" == "$FB_RELEASE_GATE_OS_USER" ]]; then
		bash -lc "$cmd"
	else
		su - "$FB_RELEASE_GATE_OS_USER" -c "$cmd"
	fi
}

fb_release_gate_archive_dir_from_major() {
	local major="$1"
	local wal_arch_link="/home/${major}pg/wal_arch"
	local resolved=""

	if [[ "${FB_RELEASE_GATE_ARCHIVE_ROOT_IS_DEFAULT:-0}" -eq 1 ]]; then
		resolved="$(readlink -f "$wal_arch_link" 2>/dev/null || true)"
		if [[ -n "$resolved" ]]; then
			printf '%s\n' "$resolved"
			return 0
		fi
	fi

	case "$major" in
		14|15|16|17|18)
			printf '%s/%swaldata\n' "$FB_RELEASE_GATE_ARCHIVE_ROOT" "$major"
			;;
		*)
			fb_release_gate_fail "unsupported PG major for release gate: $major"
			;;
	esac
}

fb_release_gate_realpath_or_self() {
	local path="$1"
	local resolved=""

	resolved="$(readlink -f "$path" 2>/dev/null || true)"
	if [[ -n "$resolved" ]]; then
		printf '%s\n' "$resolved"
	else
		printf '%s\n' "$path"
	fi
}

fb_release_gate_archive_command_target_dir() {
	local archive_command="$1"
	local target=""

	target="$(printf '%s\n' "$archive_command" | awk '{print $NF}')"
	target="${target%\"}"
	target="${target#\"}"
	target="${target%\'}"
	target="${target#\'}"
	target="${target%/}"

	if [[ "$target" == *"/%f" ]]; then
		printf '%s\n' "${target%/%f}"
		return 0
	fi
	if [[ "$target" == *"%f" ]]; then
		printf '%s\n' "$(dirname "${target%\%f}")"
		return 0
	fi
	if [[ "$target" == /* ]]; then
		printf '%s\n' "$(dirname "$target")"
		return 0
	fi
	return 1
}

fb_release_gate_archive_command_matches_dir() {
	local archive_command="$1"
	local expected_dir="$2"
	local command_dir=""
	local expected_real=""
	local command_real=""

	command_dir="$(fb_release_gate_archive_command_target_dir "$archive_command" || true)"
	[[ -n "$command_dir" ]] || return 1

	expected_real="$(fb_release_gate_realpath_or_self "$expected_dir")"
	command_real="$(fb_release_gate_realpath_or_self "$command_dir")"

	[[ "$command_real" == "$expected_real" ]]
}

fb_release_gate_psql_base() {
	printf '%q -X -v ON_ERROR_STOP=1 -p %q -U %q -d %q' \
		"$FB_RELEASE_GATE_PSQL" \
		"$FB_RELEASE_GATE_PGPORT" \
		"$FB_RELEASE_GATE_PGUSER" \
		"$FB_RELEASE_GATE_MAINT_DB"
}

fb_release_gate_psql_sql() {
	local dbname="$1"
	local sql="$2"

	local cmd

	printf -v cmd '%q -X -v ON_ERROR_STOP=1 -p %q -U %q -d %q -Atqc %q' \
		"$FB_RELEASE_GATE_PSQL" \
		"$FB_RELEASE_GATE_PGPORT" \
		"$FB_RELEASE_GATE_PGUSER" \
		"$dbname" \
		"$sql"
	fb_release_gate_run_as_os_user "$cmd"
}

fb_release_gate_psql_file() {
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

	fb_release_gate_run_as_os_user "$cmd"
}

fb_release_gate_detect_pg_major() {
	local version_num

	version_num="$(fb_release_gate_psql_sql "$FB_RELEASE_GATE_MAINT_DB" "show server_version_num;")"
	[[ -n "$version_num" ]] || fb_release_gate_fail "could not detect server_version_num"
	printf '%s\n' "${version_num:0:${#version_num}-4}"
}

fb_release_gate_ensure_output_dirs() {
	mkdir -p "$FB_RELEASE_GATE_OUTPUT_ROOT" "$FB_RELEASE_GATE_OUTPUT_DIR"
	chmod 0777 "$FB_RELEASE_GATE_OUTPUT_ROOT" "$FB_RELEASE_GATE_OUTPUT_DIR" 2>/dev/null || true
}

fb_release_gate_output_path() {
	local rel="$1"

	printf '%s/%s\n' "$FB_RELEASE_GATE_OUTPUT_DIR" "$rel"
}

fb_release_gate_json_path() {
	local name="$1"

	fb_release_gate_output_path "json/${name}.json"
}

fb_release_gate_csv_path() {
	local name="$1"

	fb_release_gate_output_path "csv/${name}.csv"
}

fb_release_gate_text_path() {
	local name="$1"

	fb_release_gate_output_path "logs/${name}.log"
}

fb_release_gate_prepare_output_tree() {
	fb_release_gate_ensure_output_dirs
	mkdir -p \
		"$(fb_release_gate_output_path json)" \
		"$(fb_release_gate_output_path csv)" \
		"$(fb_release_gate_output_path logs)" \
		"$(fb_release_gate_output_path wal)" \
		"$(fb_release_gate_output_path reports)" \
		"$(fb_release_gate_output_path csv/truth)" \
		"$(fb_release_gate_output_path csv/flashback)" \
		"$(fb_release_gate_output_path csv/materialized)"
	chmod -R 0777 "$FB_RELEASE_GATE_OUTPUT_DIR" 2>/dev/null || true
}

fb_release_gate_shared_tmp_file() {
	local prefix="${1:-release_gate}"
	local tmp_dir
	local path

	tmp_dir="$(fb_release_gate_output_path logs/tmp)"
	mkdir -p "$tmp_dir"
	chmod 0777 "$tmp_dir" 2>/dev/null || true
	path="$(mktemp "$tmp_dir/${prefix}.XXXXXX")"
	chmod 0666 "$path" 2>/dev/null || true
	printf '%s\n' "$path"
}

fb_release_gate_sim_pid_file() {
	fb_release_gate_output_path "logs/alldbsim.pid"
}

fb_release_gate_sim_log_file() {
	fb_release_gate_output_path "logs/alldbsim.log"
}

fb_release_gate_sim_health_url() {
	printf 'http://%s%s\n' "$FB_RELEASE_GATE_SIM_LISTEN_ADDR" "$FB_RELEASE_GATE_SIM_HEALTH_PATH"
}

fb_release_gate_sim_api_url() {
	local path="$1"

	printf 'http://%s%s\n' "$FB_RELEASE_GATE_SIM_LISTEN_ADDR" "$path"
}

fb_release_gate_sim_request() {
	local method="$1"
	local path="$2"
	local body="${3:-}"
	local url

	url="$(fb_release_gate_sim_api_url "$path")"
	if [[ -n "$body" ]]; then
		curl --noproxy '*' -fsS -X "$method" \
			-H 'Content-Type: application/json' \
			-d "$body" \
			"$url"
	else
		curl --noproxy '*' -fsS -X "$method" "$url"
	fi
}

fb_release_gate_sim_connect() {
	local payload

	payload="$(jq -cn \
		--arg host "$FB_RELEASE_GATE_SIM_DB_HOST" \
		--argjson port "$FB_RELEASE_GATE_SIM_DB_PORT" \
		--arg user "$FB_RELEASE_GATE_SIM_DB_USER" \
		--arg password "$FB_RELEASE_GATE_SIM_DB_PASSWORD" \
		--arg database "$FB_RELEASE_GATE_SIM_DB_NAME" \
		'{host:$host, port:$port, user:$user, password:$password, database:$database}')"
	fb_release_gate_sim_request POST /api/connect "$payload"
}

fb_release_gate_sim_job_get() {
	local job_id="$1"

	fb_release_gate_sim_request GET "/api/jobs/${job_id}"
}

fb_release_gate_sim_wait_job() {
	local job_id="$1"
	local output_file="$2"
	local max_wait_sec="${3:-7200}"
	local waited=0
	local body=""
	local status=""

	while (( waited <= max_wait_sec )); do
		body="$(fb_release_gate_sim_job_get "$job_id")"
		status="$(printf '%s' "$body" | jq -r '.status')"
		case "$status" in
			done)
				printf '%s\n' "$body" > "$output_file"
				return 0
				;;
			failed|stopped)
				printf '%s\n' "$body" > "$output_file"
				fb_release_gate_fail "simulator job ${job_id} finished with status=${status}"
				;;
		esac
		sleep 2
		waited=$((waited + 2))
	done

	if [[ -n "$body" ]]; then
		printf '%s\n' "$body" > "$output_file"
	fi
	fb_release_gate_fail "timed out waiting for simulator job ${job_id}"
}

fb_release_gate_scenario_schema() {
	printf 'scenario_%s_%st_%sr\n' \
		"$FB_RELEASE_GATE_SIM_SCENARIO" \
		"$FB_RELEASE_GATE_SIM_TABLE_COUNT" \
		"$FB_RELEASE_GATE_SIM_ROWS_PER_TABLE"
}

fb_release_gate_sql_ident() {
	local value="$1"

	value="${value//\"/\"\"}"
	printf '"%s"' "$value"
}

fb_release_gate_sql_qualified_name() {
	local schema_name="$1"
	local table_name="$2"

	printf '%s.%s' \
		"$(fb_release_gate_sql_ident "$schema_name")" \
		"$(fb_release_gate_sql_ident "$table_name")"
}

fb_release_gate_typed_null_expr() {
	local schema_name="$1"
	local table_name="$2"

	printf 'NULL::%s\n' "$(fb_release_gate_sql_qualified_name "$schema_name" "$table_name")"
}

fb_release_gate_json_string() {
	printf '%s' "$1" | jq -Rs .
}

fb_release_gate_sql_literal() {
	local value="$1"

	value="${value//\'/\'\'}"
	printf "'%s'" "$value"
}

fb_release_gate_sha256_file() {
	local path="$1"

	sha256sum "$path" | awk '{print $1}'
}

fb_release_gate_csv_row_count() {
	local path="$1"

	if [[ ! -f "$path" ]]; then
		printf '0\n'
		return 0
	fi
	tail -n +2 "$path" | wc -l | awk '{print $1}'
}

fb_release_gate_find_truth_entry() {
	local truth_manifest="$1"
	local truth_scenario_id="$2"
	local table_name="$3"

	jq -c \
		--arg scenario_id "$truth_scenario_id" \
		--arg table_name "$table_name" \
		'.[] | select(.scenario_id == $scenario_id and .table_name == $table_name)' \
		"$truth_manifest" | head -n 1
}

fb_release_gate_eval_correctness_json() {
	local scenario_id="$1"
	local truth_manifest="$2"
	local truth_scenario_id="$3"
	local table_name="$4"
	local result_sha256="$5"
	local result_row_count="${6:-0}"
	local result_csv_path="${7:-}"
	local truth_entry
	local correctness_status="pass"
	local reason=""
	local truth_sha256=""
	local truth_row_count="0"
	local truth_csv_path=""
	local diff_path=""

	truth_entry="$(fb_release_gate_find_truth_entry "$truth_manifest" "$truth_scenario_id" "$table_name")"
	if [[ -z "$truth_entry" ]]; then
		correctness_status="infra_fail"
		reason="missing truth snapshot"
	else
		truth_sha256="$(printf '%s' "$truth_entry" | jq -r '.sha256')"
		truth_row_count="$(printf '%s' "$truth_entry" | jq -r '.row_count')"
		truth_csv_path="$(printf '%s' "$truth_entry" | jq -r '.csv_path // empty')"
		if [[ "$truth_sha256" != "$result_sha256" || "$truth_row_count" != "$result_row_count" ]]; then
			correctness_status="fail"
			reason="row_count or sha256 mismatch"
			if [[ -n "$truth_csv_path" && -n "$result_csv_path" && -f "$truth_csv_path" && -f "$result_csv_path" ]]; then
				diff_path="$(fb_release_gate_output_path "logs/diff_${scenario_id}__${table_name}.diff")"
				diff -u "$truth_csv_path" "$result_csv_path" > "$diff_path" 2>&1 || true
				chmod 0666 "$diff_path" 2>/dev/null || true
			fi
		fi
	fi

	jq -cn \
		--arg correctness_status "$correctness_status" \
		--arg reason "$reason" \
		--arg diff_path "$diff_path" \
		--arg truth_sha256 "$truth_sha256" \
		--argjson truth_row_count "${truth_row_count:-0}" \
		'{correctness_status:$correctness_status, reason:$reason, diff_path:$diff_path, truth_sha256:$truth_sha256, truth_row_count:$truth_row_count}'
}

fb_release_gate_table_pk_order() {
	local dbname="$1"
	local schema_name="$2"
	local table_name="$3"
	local sql

	read -r -d '' sql <<EOF || true
select coalesce(
	string_agg(format('%I', a.attname), ', ' order by key_ord.ord),
	''
)
from pg_class c
join pg_namespace n
	on n.oid = c.relnamespace
join pg_index i
	on i.indrelid = c.oid
	and i.indisprimary
join unnest(i.indkey) with ordinality as key_ord(attnum, ord)
	on true
join pg_attribute a
	on a.attrelid = c.oid
	and a.attnum = key_ord.attnum
where n.nspname = '$schema_name'
  and c.relname = '$table_name';
EOF

	fb_release_gate_psql_sql "$dbname" "$sql"
}

fb_release_gate_table_columns_csv() {
	local dbname="$1"
	local schema_name="$2"
	local table_name="$3"
	local sql

	read -r -d '' sql <<EOF || true
select coalesce(string_agg(format('%I', a.attname), ', ' order by a.attnum), '')
from pg_class c
join pg_namespace n
	on n.oid = c.relnamespace
join pg_attribute a
	on a.attrelid = c.oid
where n.nspname = '$schema_name'
  and c.relname = '$table_name'
  and a.attnum > 0
  and not a.attisdropped;
EOF

	fb_release_gate_psql_sql "$dbname" "$sql"
}

fb_release_gate_table_size_bytes() {
	local dbname="$1"
	local schema_name="$2"
	local table_name="$3"
	local sql

	read -r -d '' sql <<EOF || true
select pg_total_relation_size($(fb_release_gate_sql_literal "${schema_name}.${table_name}")::regclass);
EOF

	fb_release_gate_psql_sql "$dbname" "$sql"
}

fb_release_gate_parse_csv_list() {
	local list="$1"

	printf '%s\n' "$list" | tr ',' '\n' | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | sed '/^$/d'
}

fb_release_gate_random_offsets() {
	local count="$1"
	local duration_sec="$2"
	local seed="$3"
	local margin_sec="$4"

	awk \
		-v count="$count" \
		-v duration="$duration_sec" \
		-v seed="$seed" \
		-v margin="$margin_sec" '
BEGIN {
	srand(seed);
	if (duration <= 0 || count <= 0) {
		exit 0;
	}
	if (margin * 2 >= duration) {
		margin = int(duration / 10);
		if (margin < 1) {
			margin = 1;
		}
	}
	span = duration - (margin * 2);
	if (span < 1) {
		span = 1;
	}
	while (n < count) {
		offset = margin + int(rand() * span);
		if (!(offset in seen)) {
			seen[offset] = 1;
			values[++n] = offset;
		}
	}
	for (i = 1; i <= n; i++) {
		for (j = i + 1; j <= n; j++) {
			if (values[j] < values[i]) {
				tmp = values[i];
				values[i] = values[j];
				values[j] = tmp;
			}
		}
	}
	for (i = 1; i <= n; i++) {
		print values[i];
	}
}'
}

fb_release_gate_timestamp_add_sec() {
	local base_ts="$1"
	local offset_sec="$2"

	date -u -d "${base_ts} + ${offset_sec} seconds" '+%Y-%m-%d %H:%M:%S+00'
}

fb_release_gate_timestamp_epoch() {
	local ts="$1"

	date -u -d "$ts" '+%s'
}

fb_release_gate_wait_until_epoch() {
	local target_epoch="$1"
	local now_epoch
	local sleep_sec

	while true; do
		now_epoch="$(date -u '+%s')"
		if (( now_epoch >= target_epoch )); then
			return 0
		fi
		sleep_sec=$((target_epoch - now_epoch))
		if (( sleep_sec > 5 )); then
			sleep_sec=5
		fi
		sleep "$sleep_sec"
	done
}

fb_release_gate_write_json() {
	local path="$1"
	local content="$2"

	printf '%s\n' "$content" > "$path"
	chmod 0666 "$path" 2>/dev/null || true
}

fb_release_gate_manifest_init() {
	local path="$1"

	fb_release_gate_write_json "$path" '[]'
}

fb_release_gate_manifest_append() {
	local path="$1"
	local item_json="$2"
	local tmp

	tmp="$(mktemp)"
	jq --argjson item "$item_json" '. + [$item]' "$path" > "$tmp"
	mv "$tmp" "$path"
	chmod 0666 "$path" 2>/dev/null || true
}
