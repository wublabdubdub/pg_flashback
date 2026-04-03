#!/usr/bin/env bash
set -euo pipefail

FB_RELEASE_GATE_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FB_RELEASE_GATE_ROOT="$FB_RELEASE_GATE_REPO_ROOT/tests/release_gate"
FB_RELEASE_GATE_BIN_DIR="$FB_RELEASE_GATE_ROOT/bin"
FB_RELEASE_GATE_SQL_DIR="$FB_RELEASE_GATE_ROOT/sql"
FB_RELEASE_GATE_CONFIG_DIR="$FB_RELEASE_GATE_ROOT/config"
FB_RELEASE_GATE_TEMPLATE_DIR="$FB_RELEASE_GATE_ROOT/templates"
FB_RELEASE_GATE_OUTPUT_ROOT="$FB_RELEASE_GATE_ROOT/output"

FB_RELEASE_GATE_PSQL="${FB_RELEASE_GATE_PSQL:-/home/18pg/local/bin/psql}"
FB_RELEASE_GATE_CREATEDB="${FB_RELEASE_GATE_CREATEDB:-/home/18pg/local/bin/createdb}"
FB_RELEASE_GATE_DROPDB="${FB_RELEASE_GATE_DROPDB:-/home/18pg/local/bin/dropdb}"
FB_RELEASE_GATE_PGPORT="${FB_RELEASE_GATE_PGPORT:-5832}"
FB_RELEASE_GATE_PGUSER="${FB_RELEASE_GATE_PGUSER:-18pg}"
FB_RELEASE_GATE_MAINT_DB="${FB_RELEASE_GATE_MAINT_DB:-postgres}"
FB_RELEASE_GATE_DBNAME="${FB_RELEASE_GATE_DBNAME:-alldb}"
FB_RELEASE_GATE_ARCHIVE_ROOT="${FB_RELEASE_GATE_ARCHIVE_ROOT:-/walstorage}"
FB_RELEASE_GATE_OUTPUT_DIR="${FB_RELEASE_GATE_OUTPUT_DIR:-$FB_RELEASE_GATE_OUTPUT_ROOT/latest}"
FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB="${FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB:-100}"
FB_RELEASE_GATE_OS_USER="${FB_RELEASE_GATE_OS_USER:-18pg}"
FB_RELEASE_GATE_SIM_BIN="${FB_RELEASE_GATE_SIM_BIN:-/root/alldbsimulator/bin/alldbsim}"
FB_RELEASE_GATE_SIM_LISTEN_ADDR="${FB_RELEASE_GATE_SIM_LISTEN_ADDR:-127.0.0.1:18080}"
FB_RELEASE_GATE_SIM_HEALTH_PATH="${FB_RELEASE_GATE_SIM_HEALTH_PATH:-/api/health}"

fb_release_gate_log() {
	printf '[release_gate] %s\n' "$*"
}

fb_release_gate_fail() {
	echo "[release_gate] ERROR: $*" >&2
	exit 1
}

fb_release_gate_require_cmd() {
	command -v "$1" >/dev/null 2>&1 || fb_release_gate_fail "missing command: $1"
}

fb_release_gate_load_config() {
	local config_file="$FB_RELEASE_GATE_CONFIG_DIR/release_gate.conf"

	if [[ -f "$config_file" ]]; then
		# shellcheck disable=SC1090
		source "$config_file"
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

	case "$major" in
		14|15|16|17|18)
			printf '%s/%swaldata\n' "$FB_RELEASE_GATE_ARCHIVE_ROOT" "$major"
			;;
		*)
			fb_release_gate_fail "unsupported PG major for release gate: $major"
			;;
	esac
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

	printf -v cmd '%q -X -v ON_ERROR_STOP=1 -p %q -U %q -d %q -f %q' \
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
		"$(fb_release_gate_output_path reports)"
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
