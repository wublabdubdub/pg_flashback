#!/usr/bin/env bash

set -euo pipefail

CONFIG_PATH=""
SUMMARYD_BIN=""
PID_PATH=""
LOG_PATH=""
ACTION=""

usage() {
  cat <<'EOF'
Usage:
  scripts/pg_flashback_summary.sh --config PATH [--summaryd-bin PATH] [--pid-file PATH] [--log-file PATH] start
  scripts/pg_flashback_summary.sh --config PATH [--summaryd-bin PATH] [--pid-file PATH] [--log-file PATH] stop
  scripts/pg_flashback_summary.sh --config PATH [--summaryd-bin PATH] [--pid-file PATH] [--log-file PATH] status
  scripts/pg_flashback_summary.sh --config PATH [--summaryd-bin PATH] [--pid-file PATH] [--log-file PATH] run-once

Manual runner for pg_flashback summaryd.
Actions:
  start     launch pg_flashback-summaryd in the background
  stop      stop the matching background daemon
  status    report whether the matching daemon is running
  run-once  run one summary sweep in the foreground and exit

Options:
  --config        required, summaryd key=value config file
  --summaryd-bin  optional, defaults to pg_flashback-summaryd from PATH
  --pid-file      optional, defaults to <config-basename>.pid
  --log-file      optional, defaults to <config-basename>.log
  --help          show this help
EOF
}

fail() {
  echo "[pg_flashback_summary] $*" >&2
  exit 1
}

derive_default_paths() {
  local config_dir
  local config_name
  local config_base

  [[ -n "${CONFIG_PATH}" ]] || fail "--config is required"

  config_dir=$(dirname "${CONFIG_PATH}")
  config_name=$(basename "${CONFIG_PATH}")
  config_base="${config_name%.conf}"

  if [[ -z "${PID_PATH}" ]]; then
    PID_PATH="${config_dir}/${config_base}.pid"
  fi
  if [[ -z "${LOG_PATH}" ]]; then
    LOG_PATH="${config_dir}/${config_base}.log"
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      start|stop|status|run-once)
        [[ -z "${ACTION}" ]] || fail "only one action is allowed"
        ACTION="$1"
        shift
        ;;
      --config)
        CONFIG_PATH="$2"
        shift 2
        ;;
      --summaryd-bin)
        SUMMARYD_BIN="$2"
        shift 2
        ;;
      --pid-file)
        PID_PATH="$2"
        shift 2
        ;;
      --log-file)
        LOG_PATH="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        fail "unknown option: $1"
        ;;
    esac
  done

  [[ -n "${ACTION}" ]] || fail "missing action: start, stop, status, or run-once"
  [[ -n "${CONFIG_PATH}" ]] || fail "--config is required"
  [[ -f "${CONFIG_PATH}" ]] || fail "config file not found: ${CONFIG_PATH}"

  if [[ -z "${SUMMARYD_BIN}" ]]; then
    SUMMARYD_BIN=$(command -v pg_flashback-summaryd || true)
  fi
  [[ -n "${SUMMARYD_BIN}" ]] || fail "pg_flashback-summaryd not found; pass --summaryd-bin"
  [[ -x "${SUMMARYD_BIN}" ]] || fail "summaryd binary is not executable: ${SUMMARYD_BIN}"

  derive_default_paths
}

config_value() {
  local key="$1"
  awk -F= -v target="${key}" '$1==target { print substr($0, length($1) + 2); exit }' "${CONFIG_PATH}"
}

state_ready() {
  local pgdata="$1"
  local state_path="${pgdata}/pg_flashback/meta/summaryd/state.json"
  local debug_path="${pgdata}/pg_flashback/meta/summaryd/debug.json"

  [[ -s "${state_path}" || -s "${debug_path}" ]]
}

process_matches() {
  local pid="$1"
  local args=""

  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" >/dev/null 2>&1 || return 1
  args=$(ps -p "${pid}" -o args= 2>/dev/null || true)
  [[ -n "${args}" ]] || return 1
  [[ "${args}" == *"${SUMMARYD_BIN}"* ]] || return 1
  [[ "${args}" == *"--config ${CONFIG_PATH}"* ]] || return 1
  return 0
}

find_matching_pid() {
  local line=""
  local pid=""
  local args=""

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    pid="${line%% *}"
    args="${line#* }"
    [[ "${args}" == *"${SUMMARYD_BIN}"* ]] || continue
    [[ "${args}" == *"--config ${CONFIG_PATH}"* ]] || continue
    if kill -0 "${pid}" >/dev/null 2>&1; then
      printf '%s\n' "${pid}"
      return 0
    fi
  done < <(ps -eo pid=,args=)

  return 1
}

read_pid_file() {
  [[ -f "${PID_PATH}" ]] || return 1
  head -n 1 "${PID_PATH}"
}

remove_pid_file() {
  rm -f "${PID_PATH}"
}

current_pid() {
  local pid=""

  pid=$(read_pid_file 2>/dev/null || true)
  if process_matches "${pid}"; then
    printf '%s\n' "${pid}"
    return 0
  fi

  remove_pid_file
  find_matching_pid
}

write_pid_file() {
  local pid="$1"

  mkdir -p "$(dirname "${PID_PATH}")"
  printf '%s\n' "${pid}" > "${PID_PATH}"
}

wait_for_ready() {
  local pid="$1"
  local pgdata="$2"
  local attempt

  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if process_matches "${pid}" && state_ready "${pgdata}"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

start_runner() {
  local pgdata
  local pid=""

  pid=$(current_pid || true)
  if [[ -n "${pid}" ]]; then
    echo "running pid=${pid} config=${CONFIG_PATH}"
    return 0
  fi

  pgdata=$(config_value "pgdata")
  [[ -n "${pgdata}" ]] || fail "config is missing pgdata=..."

  mkdir -p "$(dirname "${LOG_PATH}")"
  nohup "${SUMMARYD_BIN}" --config "${CONFIG_PATH}" --foreground >>"${LOG_PATH}" 2>&1 &
  pid=$!
  write_pid_file "${pid}"

  if ! wait_for_ready "${pid}" "${pgdata}"; then
    tail -n 100 "${LOG_PATH}" >&2 || true
    remove_pid_file
    fail "summaryd failed to publish state files"
  fi

  echo "started pid=${pid} config=${CONFIG_PATH}"
}

stop_runner() {
  local pid=""
  local attempt

  pid=$(current_pid || true)
  if [[ -z "${pid}" ]]; then
    remove_pid_file
    echo "stopped config=${CONFIG_PATH}"
    return 0
  fi

  kill "${pid}" >/dev/null 2>&1 || true
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if ! process_matches "${pid}"; then
      remove_pid_file
      echo "stopped config=${CONFIG_PATH}"
      return 0
    fi
    sleep 1
  done

  kill -9 "${pid}" >/dev/null 2>&1 || true
  remove_pid_file
  echo "stopped config=${CONFIG_PATH}"
}

status_runner() {
  local pid=""

  pid=$(current_pid || true)
  if [[ -n "${pid}" ]]; then
    echo "running pid=${pid} config=${CONFIG_PATH}"
    return 0
  fi

  echo "stopped config=${CONFIG_PATH}"
  return 1
}

run_once_runner() {
  remove_pid_file
  exec "${SUMMARYD_BIN}" --config "${CONFIG_PATH}" --foreground --once
}

main() {
  parse_args "$@"

  case "${ACTION}" in
    start)
      start_runner
      ;;
    stop)
      stop_runner
      ;;
    status)
      status_runner
      ;;
    run-once)
      run_once_runner
      ;;
  esac
}

main "$@"
