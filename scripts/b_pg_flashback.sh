#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
VERSION=$(<"${REPO_ROOT}/VERSION")
ENV_PGDATA="${PGDATA:-}"
ENV_PGDATABASE="${PGDATABASE:-}"
ENV_PGUSER="${PGUSER:-}"
ENV_PGPASSWORD="${PGPASSWORD:-}"
ENV_PGHOST="${PGHOST:-}"
ENV_PGPORT="${PGPORT:-}"
ENV_PGFLASHBACK_ARCHIVE_DEST="${PGFLASHBACK_ARCHIVE_DEST:-}"
ENV_PGFLASHBACK_INTERVAL_MS="${PGFLASHBACK_INTERVAL_MS:-}"

PG_CONFIG_BIN=""
PGDATA=""
DB_NAME=""
DB_USER=""
DB_PASSWORD=""
DB_HOST=""
DB_PORT=""
ARCHIVE_DEST=""
INTERVAL_MS=""
SERVICE_NAME="pg_flashback-summaryd"
ACTION="setup"
DRY_RUN=0
OS_RELEASE_FILE="/etc/os-release"
PGFLASHBACK_DATA_DIR=""
SAFE_CLEANUP_RESULT="not_applicable"
SERVICE_OS_USER=""
DEFAULT_INTERVAL_MS="1000"
RUNNER_SCRIPT="${REPO_ROOT}/scripts/pg_flashback_summary.sh"
RUNNER_PID_PATH=""
RUNNER_LOG_PATH=""

usage() {
  cat <<'EOF'
Usage:
  scripts/b_pg_flashback.sh
  scripts/b_pg_flashback.sh [options]

This script must be run as a non-root OS user.

This script:
  setup:
    0. safely cleans known temporary files under PGDATA/pg_flashback when present
    1. builds and installs pg_flashback with PGXS
    2. creates or upgrades the pg_flashback extension
    3. writes a summaryd config file
    4. starts pg_flashback-summaryd through scripts/pg_flashback_summary.sh

  remove:
    1. stops the script-managed pg_flashback summary daemon
    2. removes generated config / pid / log files
    3. drops the pg_flashback extension
    4. preserves PGDATA/pg_flashback and tells you to remove it manually if needed

interactive mode:
  Run the script without required options and it will prompt for:
    - pg_config path (default: `which pg_config`)
    - PGDATA path (default: `$PGDATA`)
    - database name (default: `$PGDATABASE` or `postgres`)
    - database user (default: `$PGUSER`)
    - database password (default: `$PGPASSWORD`, hidden)
    - database port (default: `$PGPORT` or `5432`)

Options:
  --pg-config         optional, path to pg_config; prompt if omitted
  --pgdata            optional, PostgreSQL data directory; prompt if omitted
  --dbname            optional, target database name; prompt if omitted
  --dbuser            optional, database login role; prompt if omitted
  --db-password       optional, database password; if omitted, prompt securely
  --db-host           optional, psql host/socket directory
  --db-port           optional, psql port, default 5432 or PGPORT
  --archive-dest      optional, summary archive path; defaults to archive_command autodiscovery, then PGDATA/pg_wal
  --interval-ms       optional, summaryd scan interval, default 1000
  --remove            remove bootstrap-managed service and drop extension
  --dry-run           print planned actions without changing the host
  --help              show this help

After bootstrap, manage the daemon manually with start/stop/status/run-once:
  scripts/pg_flashback_summary.sh --config ~/.config/pg_flashback/pg_flashback-summaryd.conf start
  scripts/pg_flashback_summary.sh --config ~/.config/pg_flashback/pg_flashback-summaryd.conf stop
  scripts/pg_flashback_summary.sh --config ~/.config/pg_flashback/pg_flashback-summaryd.conf status
  scripts/pg_flashback_summary.sh --config ~/.config/pg_flashback/pg_flashback-summaryd.conf run-once
EOF
}

ensure_nonroot_user() {
  [[ ${EUID} -ne 0 ]] ||
    die "scripts/b_pg_flashback.sh must be run as a non-root user"
}

log() {
  echo "[b_pg_flashback] $*" >&2
}

stage() {
  printf '\n========== %s ==========\n' "$1" >&2
}

stage_detail() {
  printf '[b_pg_flashback] %s\n' "$*" >&2
}

warn() {
  printf '[b_pg_flashback] WARN: %s\n' "$*" >&2
}

die() {
  log "$*"
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

run_cmd() {
  if [[ ${DRY_RUN} -eq 1 ]]; then
    printf 'dry_run:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi

  "$@"
}

run_root_cmd() {
  if [[ ${DRY_RUN} -eq 1 ]]; then
    printf 'dry_run_root:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi

  if [[ ${EUID} -eq 0 ]]; then
    "$@"
    return 0
  fi

  if command -v sudo >/dev/null 2>&1; then
    sudo "$@"
    return 0
  fi

  die "this operation requires root privileges, but sudo is unavailable"
}

sanitize_name() {
  local input="$1"

  input=$(printf '%s' "${input}" | tr '/:' '--' | tr -cs 'A-Za-z0-9._-' '-')
  input=${input##-}
  input=${input%%-}
  printf '%s' "${input}"
}

realpath_or_self() {
  local path="$1"
  local resolved=""

  resolved=$(resolve_path "${path}" 2>/dev/null || true)
  if [[ -n "${resolved}" ]]; then
    printf '%s' "${resolved}"
  else
    printf '%s' "${path}"
  fi
}

sql_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

detect_platform() {
  local file="$1"
  local id=""
  local version_id=""
  local major=""

  [[ -f "${file}" ]] || die "os release file not found: ${file}"

  # shellcheck disable=SC1090
  source "${file}"

  id="${ID:-unknown}"
  version_id="${VERSION_ID:-0}"
  major="${version_id%%.*}"

  case "${id}:${major}" in
    centos:7) echo "centos7" ;;
    centos:8) echo "centos8" ;;
    centos:9) echo "centos9" ;;
    rocky:7) echo "rocky7" ;;
    rocky:8) echo "rocky8" ;;
    rocky:9) echo "rocky9" ;;
    almalinux:7) echo "almalinux7" ;;
    almalinux:8) echo "almalinux8" ;;
    almalinux:9) echo "almalinux9" ;;
    rhel:7) echo "rhel7" ;;
    rhel:8) echo "rhel8" ;;
    rhel:9) echo "rhel9" ;;
    *) echo "${id}${major}" ;;
  esac
}

default_scope_for_platform() {
  case "$1" in
    centos7|rocky7|almalinux7|rhel7)
      echo "system"
      ;;
    *)
      echo "user"
      ;;
  esac
}

ensure_user_bus_env() {
  local runtime_dir="/run/user/$(id -u)"

  if [[ -z "${XDG_RUNTIME_DIR:-}" && -d "${runtime_dir}" ]]; then
    export XDG_RUNTIME_DIR="${runtime_dir}"
  fi
  if [[ -z "${DBUS_SESSION_BUS_ADDRESS:-}" &&
        -n "${XDG_RUNTIME_DIR:-}" &&
        -S "${XDG_RUNTIME_DIR}/bus" ]]; then
    export DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"
  fi
}

prompt_password_if_needed() {
  local default_value="${1:-}"
  local input=""

  if [[ -n "${DB_PASSWORD}" ]]; then
    return 0
  fi

  if [[ -n "${default_value}" ]]; then
    read -r -s -p "Database password for ${DB_USER} [default]: " input || die "failed to read DB_PASSWORD"
    printf '\n'
    input=${input:-${default_value}}
  else
    read -r -s -p "Database password for ${DB_USER}: " input || die "failed to read DB_PASSWORD"
    printf '\n'
  fi

  DB_PASSWORD="${input}"
  [[ -n "${DB_PASSWORD}" ]] || die "DB_PASSWORD cannot be empty"
}

validate_pg_config_input() {
  [[ -x "$1" ]] || die "pg_config is not executable: $1"
}

validate_pgdata_input() {
  [[ -d "$1" ]] || die "PGDATA directory not found: $1"
}

validate_db_port_input() {
  [[ "$1" =~ ^[0-9]+$ ]] || die "DB_PORT must be a numeric port: $1"
}

resolve_pgdata_owner_user() {
  local owner

  owner=$(stat -Lc '%U' "$1" 2>/dev/null || true)
  [[ -n "${owner}" && "${owner}" != "UNKNOWN" ]] ||
    die "failed to resolve PGDATA owner user: $1"
  printf '%s' "${owner}"
}

resolve_path() {
  if command -v realpath >/dev/null 2>&1; then
    realpath "$1"
    return 0
  fi

  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$1"
    return 0
  fi

  die "missing required command: realpath or readlink"
}

prompt_value_if_needed() {
  local var_name="$1"
  local prompt_text="$2"
  local default_value="${3:-}"
  local validator="${4:-}"
  local current_value="${!var_name}"
  local input=""

  if [[ -n "${current_value}" ]]; then
    if [[ -n "${validator}" ]]; then
      "${validator}" "${current_value}"
    fi
    return 0
  fi

  if [[ -n "${default_value}" ]]; then
    read -r -p "${prompt_text} [${default_value}]: " input || die "failed to read ${var_name}"
    input=${input:-${default_value}}
  else
    read -r -p "${prompt_text}: " input || die "failed to read ${var_name}"
  fi

  [[ -n "${input}" ]] || die "${var_name} cannot be empty"
  if [[ -n "${validator}" ]]; then
    "${validator}" "${input}"
  fi
  printf -v "${var_name}" '%s' "${input}"
}

prompt_required_inputs() {
  local pg_config_default=""
  local pgdata_default="${ENV_PGDATA}"
  local db_name_default="${ENV_PGDATABASE:-postgres}"
  local db_user_default="${ENV_PGUSER}"
  local db_port_default="${ENV_PGPORT:-5432}"

  pg_config_default=$(command -v pg_config 2>/dev/null || true)

  prompt_value_if_needed PG_CONFIG_BIN "Path to pg_config" "${pg_config_default}" validate_pg_config_input
  prompt_value_if_needed PGDATA "Path to PGDATA" "${pgdata_default}" validate_pgdata_input
  prompt_value_if_needed DB_NAME "Database name" "${db_name_default}"
  prompt_value_if_needed DB_USER "Database user" "${db_user_default}"
  prompt_password_if_needed "${ENV_PGPASSWORD}"
  prompt_value_if_needed DB_PORT "Database port" "${db_port_default}" validate_db_port_input
}

resolve_optional_env_defaults() {
  if [[ -z "${DB_HOST}" && -n "${ENV_PGHOST}" ]]; then
    DB_HOST="${ENV_PGHOST}"
  fi
  if [[ -z "${ARCHIVE_DEST}" && -n "${ENV_PGFLASHBACK_ARCHIVE_DEST}" ]]; then
    ARCHIVE_DEST="${ENV_PGFLASHBACK_ARCHIVE_DEST}"
  fi
  if [[ -z "${INTERVAL_MS}" && -n "${ENV_PGFLASHBACK_INTERVAL_MS}" ]]; then
    INTERVAL_MS="${ENV_PGFLASHBACK_INTERVAL_MS}"
  fi
  if [[ -z "${INTERVAL_MS}" ]]; then
    INTERVAL_MS="${DEFAULT_INTERVAL_MS}"
  fi
}

init_pgflashback_data_dir() {
  local pgdata_real

  pgdata_real=$(resolve_path "${PGDATA}") || die "failed to resolve PGDATA: ${PGDATA}"
  PGFLASHBACK_DATA_DIR="${pgdata_real}/pg_flashback"

  [[ "${PGFLASHBACK_DATA_DIR}" != "/" ]] || die "refusing unsafe pg_flashback data dir: ${PGFLASHBACK_DATA_DIR}"
  [[ "${PGFLASHBACK_DATA_DIR}" != "${pgdata_real}" ]] || die "refusing unsafe pg_flashback data dir: ${PGFLASHBACK_DATA_DIR}"
  [[ "$(basename "${PGFLASHBACK_DATA_DIR}")" == "pg_flashback" ]] ||
    die "refusing unexpected pg_flashback data dir: ${PGFLASHBACK_DATA_DIR}"
}

guard_expected_subdir() {
  local path="$1"
  local expected_name="$2"
  local expected_parent="$3"

  [[ "$(basename "${path}")" == "${expected_name}" ]] ||
    die "refusing unexpected cleanup path: ${path}"
  [[ "$(dirname "${path}")" == "${expected_parent}" ]] ||
    die "refusing unexpected cleanup parent: ${path}"
  [[ ! -L "${path}" ]] || die "refusing to clean symlink path: ${path}"
}

remove_direct_children() {
  local dir="$1"
  local had_entries=0
  local entry

  [[ -d "${dir}" ]] || return 0
  [[ ! -L "${dir}" ]] || die "refusing to clean symlink directory: ${dir}"

  shopt -s nullglob dotglob
  for entry in "${dir}"/*; do
    had_entries=1
    if [[ ${DRY_RUN} -eq 1 ]]; then
      printf 'dry_run_cleanup: %q\n' "${entry}"
    else
      rm -rf -- "${entry}"
    fi
  done
  shopt -u nullglob dotglob

  if [[ ${had_entries} -eq 1 ]]; then
    stage_detail "cleared runtime scratch entries under ${dir}"
  else
    stage_detail "no runtime scratch entries to clear under ${dir}"
  fi
}

remove_matching_files() {
  local dir="$1"
  local pattern="$2"
  local label="$3"
  local had_matches=0
  local entry

  [[ -d "${dir}" ]] || return 0
  [[ ! -L "${dir}" ]] || die "refusing to clean symlink directory: ${dir}"

  shopt -s nullglob
  for entry in "${dir}"/${pattern}; do
    had_matches=1
    if [[ ${DRY_RUN} -eq 1 ]]; then
      printf 'dry_run_cleanup: %q\n' "${entry}"
    else
      rm -f -- "${entry}"
    fi
  done
  shopt -u nullglob

  if [[ ${had_matches} -eq 1 ]]; then
    stage_detail "removed ${label} under ${dir}"
  else
    stage_detail "no ${label} found under ${dir}"
  fi
}

safe_cleanup_pgflashback_data_dir_if_present() {
  local runtime_dir
  local meta_dir
  local summary_dir
  local summaryd_dir

  SAFE_CLEANUP_RESULT="skipped_absent"

  if [[ ! -e "${PGFLASHBACK_DATA_DIR}" ]]; then
    stage_detail "no existing pg_flashback data directory found at ${PGFLASHBACK_DATA_DIR}"
    return 0
  fi

  [[ -d "${PGFLASHBACK_DATA_DIR}" ]] || die "pg_flashback data path is not a directory: ${PGFLASHBACK_DATA_DIR}"
  [[ ! -L "${PGFLASHBACK_DATA_DIR}" ]] || die "refusing to clean symlink data dir: ${PGFLASHBACK_DATA_DIR}"

  runtime_dir="${PGFLASHBACK_DATA_DIR}/runtime"
  meta_dir="${PGFLASHBACK_DATA_DIR}/meta"
  summary_dir="${meta_dir}/summary"
  summaryd_dir="${meta_dir}/summaryd"

  guard_expected_subdir "${runtime_dir}" "runtime" "${PGFLASHBACK_DATA_DIR}"
  guard_expected_subdir "${meta_dir}" "meta" "${PGFLASHBACK_DATA_DIR}"
  guard_expected_subdir "${summary_dir}" "summary" "${meta_dir}"
  guard_expected_subdir "${summaryd_dir}" "summaryd" "${meta_dir}"

  stage_detail "existing data directory detected: ${PGFLASHBACK_DATA_DIR}"
  stage_detail "preserving recovered WAL and committed summary metadata"

  remove_direct_children "${runtime_dir}"
  remove_matching_files "${summaryd_dir}" "state.json" "stale summaryd state files"
  remove_matching_files "${summaryd_dir}" "debug.json" "stale summaryd debug files"
  remove_matching_files "${summaryd_dir}" "*.lock" "stale summaryd lock files"
  remove_matching_files "${summary_dir}" ".tmp.*" "temporary summary build files"

  SAFE_CLEANUP_RESULT="performed"
}

print_retained_data_dir_guidance() {
  stage "Retained Data Directory"

  if [[ -d "${PGFLASHBACK_DATA_DIR}" ]]; then
    stage_detail "preserved extension data directory: ${PGFLASHBACK_DATA_DIR}"
    stage_detail "manual cleanup only, because this is under PGDATA:"
    stage_detail "rm -rf '${PGFLASHBACK_DATA_DIR}'"
  else
    stage_detail "no pg_flashback data directory found under PGDATA"
  fi
}

write_file_as_user() {
  local path="$1"
  local mode="$2"
  local tmp

  tmp=$(mktemp)
  cat > "${tmp}"
  install -Dm"${mode}" "${tmp}" "${path}"
  rm -f "${tmp}"
}

write_file_as_root() {
  local path="$1"
  local mode="$2"
  local tmp

  tmp=$(mktemp)
  cat > "${tmp}"
  run_root_cmd install -Dm"${mode}" "${tmp}" "${path}"
  rm -f "${tmp}"
}

psql_args=()

psql_scalar() {
  local psql_bin="$1"
  local sql="$2"

  PGPASSWORD="${DB_PASSWORD}" "${psql_bin}" "${psql_args[@]}" -Atqc "${sql}" 2>/dev/null || true
}

archive_command_target_dir() {
  local archive_command="$1"
  local target=""

  target="$(printf '%s\n' "${archive_command}" | awk '{print $NF}')"
  target="${target%\"}"
  target="${target#\"}"
  target="${target%\'}"
  target="${target#\'}"
  target="${target%/}"

  if [[ "${target}" == *"/%f" ]]; then
    printf '%s\n' "${target%/%f}"
    return 0
  fi
  if [[ "${target}" == *"%f" ]]; then
    printf '%s\n' "$(dirname "${target%\%f}")"
    return 0
  fi
  if [[ "${target}" == /* ]]; then
    printf '%s\n' "$(dirname "${target}")"
    return 0
  fi
  return 1
}

autodetect_archive_dest_from_server() {
  local psql_bin="$1"
  local archive_info=""
  local archive_library=""
  local archive_command=""
  local detected=""

  archive_info=$(psql_scalar "${psql_bin}" "SELECT current_setting('archive_library', true), current_setting('archive_command', true);")
  archive_library="${archive_info%%|*}"
  archive_command="${archive_info#*|}"

  if [[ "${archive_info}" != *"|"* ]]; then
    archive_library=""
    archive_command=""
  fi

  if [[ -n "${archive_library}" ]]; then
    return 1
  fi

  detected=$(archive_command_target_dir "${archive_command}" 2>/dev/null || true)
  [[ -n "${detected}" ]] || return 1
  realpath_or_self "${detected}"
}

config_has_exact_line() {
  local path="$1"
  local expected="$2"

  [[ -f "${path}" ]] || return 1
  grep -Fxq "${expected}" "${path}"
}

config_targets_pgdata() {
  local path="$1"

  [[ -f "${path}" ]] || return 1
  grep -Fxq "pgdata=${PGDATA}" "${path}"
}

summaryd_stop_disable_service() {
  local scope="$1"
  local service_file_name="$2"

  if [[ "${scope}" == "user" ]]; then
    systemctl --user stop "${service_file_name}" >/dev/null 2>&1 || true
    systemctl --user disable "${service_file_name}" >/dev/null 2>&1 || true
    return 0
  fi

  if [[ ${EUID} -eq 0 ]]; then
    systemctl stop "${service_file_name}" >/dev/null 2>&1 || true
    systemctl disable "${service_file_name}" >/dev/null 2>&1 || true
  elif command -v sudo >/dev/null 2>&1; then
    sudo systemctl stop "${service_file_name}" >/dev/null 2>&1 || true
    sudo systemctl disable "${service_file_name}" >/dev/null 2>&1 || true
  else
    die "system scope remove requires root privileges or sudo"
  fi
}

summaryd_remove_file() {
  local path="$1"

  rm -f "${path}"
}

cleanup_summaryd_service_artifacts() {
  local scope="$1"
  local unit_dir="$2"
  local config_dir="$3"
  local service_file_name="$4"
  local config_path="$5"
  local unit_path="$6"
  local summaryd_bin="$7"
  local config_file
  local derived_service
  local derived_unit

  summaryd_stop_disable_service "${scope}" "${service_file_name}"
  terminate_summaryd_processes_for_pgdata "${summaryd_bin}"
  summaryd_remove_file "${unit_path}"
  summaryd_remove_file "${config_path}"

  [[ -d "${config_dir}" ]] || return 0

  shopt -s nullglob
  for config_file in "${config_dir}"/pg_flashback-summaryd*.conf; do
    if ! config_targets_pgdata "${config_file}"; then
      continue
    fi

    derived_service="$(basename "${config_file}" .conf).service"
    derived_unit="${unit_dir}/${derived_service}"
    summaryd_stop_disable_service "${scope}" "${derived_service}"
    terminate_summaryd_processes_for_pgdata "${summaryd_bin}"
    summaryd_remove_file "${derived_unit}"
    summaryd_remove_file "${config_file}"
  done
  shopt -u nullglob
}

terminate_summaryd_processes_for_pgdata() {
  local summaryd_bin="$1"
  local pid=""
  local config_path=""
  local -a fields=()
  local i=0

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    pid=""
    config_path=""
    fields=()

    read -ra fields <<<"${line}"
    [[ ${#fields[@]} -ge 4 ]] || continue
    pid="${fields[0]}"

    for ((i = 1; i + 2 < ${#fields[@]}; i++)); do
      if [[ "${fields[i]}" == "${summaryd_bin}" && "${fields[i + 1]}" == "--config" ]]; then
        config_path="${fields[i + 2]}"
        break
      fi
    done

    [[ -n "${pid}" ]] || continue
    [[ -n "${config_path}" ]] || continue
    if ! config_targets_pgdata "${config_path}"; then
      continue
    fi

    kill "${pid}" >/dev/null 2>&1 || true
    sleep 1
    kill -9 "${pid}" >/dev/null 2>&1 || true
  done < <(ps -eo pid=,args=)
}

summaryd_process_exists_for_config() {
  local summaryd_bin="$1"
  local config_path="$2"
  local line=""
  local pid=""
  local found_bin=0
  local found_config=0
  local -a fields=()
  local i=0

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    fields=()
    read -ra fields <<<"${line}"
    [[ ${#fields[@]} -ge 4 ]] || continue
    pid="${fields[0]}"
    found_bin=0
    found_config=0

    for ((i = 1; i < ${#fields[@]}; i++)); do
      if [[ "${fields[i]}" == "${summaryd_bin}" ]]; then
        found_bin=1
      fi
      if [[ "${fields[i]}" == "--config" &&
            $((i + 1)) -lt ${#fields[@]} &&
            "${fields[i + 1]}" == "${config_path}" ]]; then
        found_config=1
      fi
    done

    if [[ ${found_bin} -eq 1 && ${found_config} -eq 1 ]]; then
      return 0
    fi
  done < <(ps -eo pid=,args=)

  return 1
}
setup_already_initialized() {
  local psql_bin="$1"
  local summaryd_bin="$2"
  local config_path="$3"
  local extversion=""
  local archive_dest_setting=""

  [[ -x "${summaryd_bin}" ]] || return 1
  [[ -x "${RUNNER_SCRIPT}" ]] || return 1
  config_has_exact_line "${config_path}" "pgdata=${PGDATA}" || return 1
  config_has_exact_line "${config_path}" "archive_dest=${ARCHIVE_DEST}" || return 1
  config_has_exact_line "${config_path}" "interval_ms=${INTERVAL_MS}" || return 1
  summaryd_process_exists_for_config "${summaryd_bin}" "${config_path}" || return 1

  extversion=$(psql_scalar "${psql_bin}" "SELECT extversion FROM pg_extension WHERE extname = 'pg_flashback';")
  [[ "${extversion}" == "${VERSION}" ]] || return 1

  archive_dest_setting=$(psql_scalar "${psql_bin}" "SELECT current_setting('pg_flashback.archive_dest', true);")
  [[ "${archive_dest_setting}" == "${ARCHIVE_DEST}" ]] || return 1

  return 0
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --pg-config)
        PG_CONFIG_BIN="$2"
        shift 2
        ;;
      --pgdata)
        PGDATA="$2"
        shift 2
        ;;
      --dbname)
        DB_NAME="$2"
        shift 2
        ;;
      --dbuser)
        DB_USER="$2"
        shift 2
        ;;
      --db-password)
        DB_PASSWORD="$2"
        shift 2
        ;;
      --db-host)
        DB_HOST="$2"
        shift 2
        ;;
      --db-port)
        DB_PORT="$2"
        shift 2
        ;;
      --archive-dest)
        ARCHIVE_DEST="$2"
        shift 2
        ;;
      --interval-ms)
        INTERVAL_MS="$2"
        shift 2
        ;;
      --remove)
        ACTION="remove"
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --os-release-file)
        OS_RELEASE_FILE="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "unknown option: $1"
        ;;
    esac
  done
}

main() {
  local bindir
  local sharedir
  local psql_bin
  local summaryd_bin
  local current_os_user
  local config_path
  local extension_sql
  local archive_dest_sql

  ensure_nonroot_user
  parse_args "$@"
  resolve_optional_env_defaults

  prompt_required_inputs
  init_pgflashback_data_dir

  [[ -x "${PG_CONFIG_BIN}" ]] || die "pg_config is not executable: ${PG_CONFIG_BIN}"
  require_cmd make
  [[ -x "${RUNNER_SCRIPT}" ]] || die "missing runner script: ${RUNNER_SCRIPT}"
  SERVICE_OS_USER=$(resolve_pgdata_owner_user "${PGDATA}")
  current_os_user=$(id -un)
  [[ "${current_os_user}" == "${SERVICE_OS_USER}" ]] ||
    die "current OS user (${current_os_user}) must own PGDATA; PGDATA owner is ${SERVICE_OS_USER}"

  bindir=$("${PG_CONFIG_BIN}" --bindir)
  sharedir=$("${PG_CONFIG_BIN}" --sharedir)
  psql_bin="${bindir}/psql"
  summaryd_bin="${bindir}/pg_flashback-summaryd"
  config_path="${HOME}/.config/pg_flashback/${SERVICE_NAME}.conf"
  RUNNER_PID_PATH="${HOME}/.config/pg_flashback/${SERVICE_NAME}.pid"
  RUNNER_LOG_PATH="${HOME}/.config/pg_flashback/${SERVICE_NAME}.log"

  [[ ${DRY_RUN} -eq 1 || -x "${psql_bin}" || -x "$(command -v psql 2>/dev/null || true)" ]] ||
    die "psql not found under ${bindir} and not found in PATH"

  if [[ ${DRY_RUN} -eq 1 ]]; then
    cat <<EOF
action=${ACTION}
service_name=${SERVICE_NAME}
service_os_user=${SERVICE_OS_USER}
runner_script=${RUNNER_SCRIPT}
config_path=${config_path}
pid_path=${RUNNER_PID_PATH}
log_path=${RUNNER_LOG_PATH}
summaryd_bin=${summaryd_bin}
psql_bin=${psql_bin}
sharedir=${sharedir}
db_port=${DB_PORT}
pgflashback_data_dir=${PGFLASHBACK_DATA_DIR}
EOF
    exit 0
  fi

  prompt_password_if_needed

  psql_args=(-v ON_ERROR_STOP=1 -U "${DB_USER}" -p "${DB_PORT}" -d "${DB_NAME}")
  if [[ -n "${DB_HOST}" ]]; then
    psql_args=(-h "${DB_HOST}" "${psql_args[@]}")
  fi

  if [[ "${ACTION}" == "setup" ]]; then
    if [[ -z "${ARCHIVE_DEST}" ]]; then
      ARCHIVE_DEST=$(autodetect_archive_dest_from_server "${psql_bin}" || true)
      if [[ -z "${ARCHIVE_DEST}" ]]; then
        ARCHIVE_DEST="${PGDATA}/pg_wal"
      fi
    fi

    stage "[1/6] Preflight"
    stage_detail "service_user=${SERVICE_OS_USER}"
    stage_detail "pg_config=${PG_CONFIG_BIN}"
    stage_detail "pgdata=${PGDATA}"
    stage_detail "db=${DB_NAME} user=${DB_USER} port=${DB_PORT}"
    stage_detail "archive_dest=${ARCHIVE_DEST}"
    stage_detail "runner_script=${RUNNER_SCRIPT}"

    if setup_already_initialized "${psql_bin}" "${summaryd_bin}" "${config_path}"; then
      stage "[2/6] Existing Data Directory Check"
      stage_detail "environment already initialized; skipping rebuild and safe cleanup"
      cat <<EOF
bootstrap_result=already_initialized
action=${ACTION}
service_name=${SERVICE_NAME}
service_os_user=${SERVICE_OS_USER}
runner_script=${RUNNER_SCRIPT}
config_path=${config_path}
pid_path=${RUNNER_PID_PATH}
log_path=${RUNNER_LOG_PATH}
summaryd_bin=${summaryd_bin}
psql_bin=${psql_bin}
db_port=${DB_PORT}
pgflashback_data_dir=${PGFLASHBACK_DATA_DIR}
status_command=${RUNNER_SCRIPT} --config ${config_path} --summaryd-bin ${summaryd_bin} status
EOF
      exit 0
    fi

    stage "[2/6] Existing Data Directory Check"
    if [[ -d "${PGFLASHBACK_DATA_DIR}" ]]; then
      stage_detail "found existing pg_flashback data directory"
    else
      stage_detail "no existing pg_flashback data directory found"
    fi

    stage "[3/6] Safe Cleanup"
    safe_cleanup_pgflashback_data_dir_if_present

    stage "[4/6] Build / Install"
    stage_detail "building pg_flashback with ${PG_CONFIG_BIN}"
    run_cmd make -C "${REPO_ROOT}" "PG_CONFIG=${PG_CONFIG_BIN}"
    run_cmd make -C "${REPO_ROOT}" "PG_CONFIG=${PG_CONFIG_BIN}" install

    [[ -x "${summaryd_bin}" ]] || die "installed daemon binary not found: ${summaryd_bin}"
    [[ -x "${psql_bin}" ]] || die "installed psql not found: ${psql_bin}"

    extension_sql=$(cat <<EOF
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
    CREATE EXTENSION pg_flashback;
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'pg_flashback'
      AND extversion <> '${VERSION}'
  ) THEN
    EXECUTE format('ALTER EXTENSION pg_flashback UPDATE TO %L', '${VERSION}');
  END IF;
END;
\$\$;
EOF
    )

    stage "[5/6] Database Changes"
    stage_detail "creating or upgrading extension pg_flashback in ${DB_NAME}"
    PGPASSWORD="${DB_PASSWORD}" "${psql_bin}" "${psql_args[@]}" -c "${extension_sql}"

    archive_dest_sql=$(cat <<EOF
DO \$\$
BEGIN
  EXECUTE format(
    'ALTER DATABASE %I SET pg_flashback.archive_dest = %L',
    '$(sql_literal "${DB_NAME}")',
    '$(sql_literal "${ARCHIVE_DEST}")'
  );
END;
\$\$;
EOF
)
    stage_detail "setting database default pg_flashback.archive_dest for ${DB_NAME}"
    PGPASSWORD="${DB_PASSWORD}" "${psql_bin}" "${psql_args[@]}" -c "${archive_dest_sql}"

    stage "[6/6] Runner Setup"
    stage_detail "writing summaryd config to ${config_path}"
    write_file_as_user "${config_path}" 0644 <<EOF
pgdata=${PGDATA}
archive_dest=${ARCHIVE_DEST}
interval_ms=${INTERVAL_MS}
EOF
    stage_detail "starting script-managed summaryd"
    run_cmd "${RUNNER_SCRIPT}" --config "${config_path}" --summaryd-bin "${summaryd_bin}" start

    cat <<EOF
bootstrap_result=ok
action=${ACTION}
service_name=${SERVICE_NAME}
service_os_user=${SERVICE_OS_USER}
runner_script=${RUNNER_SCRIPT}
config_path=${config_path}
pid_path=${RUNNER_PID_PATH}
log_path=${RUNNER_LOG_PATH}
summaryd_bin=${summaryd_bin}
psql_bin=${psql_bin}
db_port=${DB_PORT}
pgflashback_data_dir=${PGFLASHBACK_DATA_DIR}
safe_cleanup_result=${SAFE_CLEANUP_RESULT}
status_command=${RUNNER_SCRIPT} --config ${config_path} --summaryd-bin ${summaryd_bin} status
EOF
    exit 0
  fi

  stage "[1/4] Preflight"
  stage_detail "service_user=${SERVICE_OS_USER}"
  stage_detail "pg_config=${PG_CONFIG_BIN}"
  stage_detail "pgdata=${PGDATA}"
  stage_detail "db=${DB_NAME} user=${DB_USER} port=${DB_PORT}"
  stage_detail "runner_script=${RUNNER_SCRIPT}"

  [[ -x "${psql_bin}" ]] || die "installed psql not found: ${psql_bin}"

  stage "[2/4] Runner Removal"
  if [[ -f "${config_path}" && -x "${summaryd_bin}" ]]; then
    run_cmd "${RUNNER_SCRIPT}" --config "${config_path}" --summaryd-bin "${summaryd_bin}" stop || true
  else
    terminate_summaryd_processes_for_pgdata "${summaryd_bin}"
  fi
  cleanup_summaryd_service_artifacts "user" "${HOME}/.config/systemd/user" "${HOME}/.config/pg_flashback" \
    "${SERVICE_NAME}.service" "${config_path}" "${HOME}/.config/systemd/user/${SERVICE_NAME}.service" "${summaryd_bin}"
  rm -f "${config_path}" "${RUNNER_PID_PATH}" "${RUNNER_LOG_PATH}" "${summaryd_bin}"

  extension_sql="DROP EXTENSION IF EXISTS pg_flashback CASCADE;"
  archive_dest_sql=$(cat <<EOF
DO \$\$
BEGIN
  EXECUTE format('ALTER DATABASE %I RESET pg_flashback.archive_dest', '${DB_NAME}');
  EXECUTE format('ALTER DATABASE %I RESET pg_flashback.archive_dir', '${DB_NAME}');
EXCEPTION
  WHEN undefined_object THEN
    NULL;
END;
\$\$;
EOF
)
  stage "[3/4] Database Cleanup"
  stage_detail "resetting database pg_flashback archive defaults on ${DB_NAME}"
  PGPASSWORD="${DB_PASSWORD}" "${psql_bin}" "${psql_args[@]}" -c "${archive_dest_sql}"
  stage_detail "dropping extension pg_flashback from ${DB_NAME}"
  PGPASSWORD="${DB_PASSWORD}" "${psql_bin}" "${psql_args[@]}" -c "${extension_sql}"

  print_retained_data_dir_guidance

  cat <<EOF
bootstrap_result=ok
action=${ACTION}
service_name=${SERVICE_NAME}
service_os_user=${SERVICE_OS_USER}
runner_script=${RUNNER_SCRIPT}
config_path=${config_path}
pid_path=${RUNNER_PID_PATH}
log_path=${RUNNER_LOG_PATH}
psql_bin=${psql_bin}
db_port=${DB_PORT}
pgflashback_data_dir=${PGFLASHBACK_DATA_DIR}
EOF
}

main "$@"
