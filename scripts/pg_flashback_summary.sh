#!/usr/bin/env bash

set -euo pipefail

SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
SERVICE_NAME="pg_flashback-summaryd"
CONFIG_DIR="${HOME}/.config/pg_flashback"
CONFIG_PATH="${CONFIG_DIR}/${SERVICE_NAME}.conf"
WATCHDOG_PID_PATH="${CONFIG_DIR}/${SERVICE_NAME}.watchdog.pid"
CHILD_PID_PATH="${CONFIG_DIR}/${SERVICE_NAME}.pid"
LOG_PATH="${CONFIG_DIR}/${SERVICE_NAME}.log"

ACTION=""
SUMMARYD_BIN=""
WATCHDOG_STOP_REQUESTED=0
WATCHDOG_CHILD_PID=""

usage() {
  cat <<EOF
Usage:
  scripts/pg_flashback_summary.sh start
  scripts/pg_flashback_summary.sh stop
  scripts/pg_flashback_summary.sh status

Manual runner for pg_flashback summaryd.

Actions:
  start     start the shell watchdog and let it keep summaryd alive
  stop      stop both the watchdog and the current summaryd child
  status    report watchdog/child status

Fixed paths:
  config    ${CONFIG_PATH}
  watchdog  ${WATCHDOG_PID_PATH}
  child     ${CHILD_PID_PATH}
  log       ${LOG_PATH}

This script intentionally does not expose --config/--pid-file/--log-file.
EOF
}

fail() {
  echo "[pg_flashback_summary] $*" >&2
  exit 1
}

parse_args() {
  case "${1:-}" in
    start|stop|status|__watchdog-loop)
      ACTION="$1"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fail "missing or unknown action: start, stop, or status"
      ;;
  esac

  [[ $# -eq 0 ]] || fail "unexpected extra arguments"
}

ensure_config_present() {
  [[ -f "${CONFIG_PATH}" ]] || fail "config file not found: ${CONFIG_PATH}"
}

config_value() {
  local key="$1"

  awk -F= -v target="${key}" '$1==target { print substr($0, length($1) + 2); exit }' "${CONFIG_PATH}"
}

resolve_summaryd_bin() {
  if [[ -n "${SUMMARYD_BIN}" ]]; then
    printf '%s\n' "${SUMMARYD_BIN}"
    return 0
  fi

  SUMMARYD_BIN=$(command -v pg_flashback-summaryd || true)
  [[ -n "${SUMMARYD_BIN}" ]] || fail "pg_flashback-summaryd not found in PATH"
  [[ -x "${SUMMARYD_BIN}" ]] || fail "summaryd binary is not executable: ${SUMMARYD_BIN}"
  printf '%s\n' "${SUMMARYD_BIN}"
}

read_pid_file() {
  local path="$1"

  [[ -f "${path}" ]] || return 1
  head -n 1 "${path}"
}

write_pid_file() {
  local path="$1"
  local pid="$2"

  mkdir -p "$(dirname "${path}")"
  printf '%s\n' "${pid}" > "${path}"
}

remove_pid_file() {
  local path="$1"

  rm -f "${path}"
}

process_matches_watchdog() {
  local pid="$1"
  local args=""

  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" >/dev/null 2>&1 || return 1
  args=$(ps -p "${pid}" -o args= 2>/dev/null || true)
  [[ -n "${args}" ]] || return 1
  [[ "${args}" == *"${SCRIPT_PATH}"* ]] || return 1
  [[ "${args}" == *"__watchdog-loop"* ]] || return 1
  return 0
}

process_matches_child() {
  local pid="$1"
  local args=""
  local bin_name

  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" >/dev/null 2>&1 || return 1
  args=$(ps -p "${pid}" -o args= 2>/dev/null || true)
  [[ -n "${args}" ]] || return 1
  bin_name=$(basename "$(resolve_summaryd_bin)")
  [[ "${args}" == *"${bin_name}"* ]] || return 1
  [[ "${args}" == *"--config ${CONFIG_PATH}"* ]] || return 1
  return 0
}

find_matching_pid() {
  local mode="$1"
  local line=""
  local pid=""
  local args=""
  local bin_name=""

  if [[ "${mode}" == "child" ]]; then
    bin_name=$(basename "$(resolve_summaryd_bin)")
  fi

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    pid="${line%% *}"
    args="${line#* }"
    if [[ "${mode}" == "watchdog" ]]; then
      [[ "${args}" == *"${SCRIPT_PATH}"* ]] || continue
      [[ "${args}" == *"__watchdog-loop"* ]] || continue
    else
      [[ "${args}" == *"${bin_name}"* ]] || continue
      [[ "${args}" == *"--config ${CONFIG_PATH}"* ]] || continue
    fi
    if kill -0 "${pid}" >/dev/null 2>&1; then
      printf '%s\n' "${pid}"
      return 0
    fi
  done < <(ps -eo pid=,args=)

  return 1
}

current_watchdog_pid() {
  local pid=""

  pid=$(read_pid_file "${WATCHDOG_PID_PATH}" 2>/dev/null || true)
  if process_matches_watchdog "${pid}"; then
    printf '%s\n' "${pid}"
    return 0
  fi

  remove_pid_file "${WATCHDOG_PID_PATH}"
  find_matching_pid watchdog
}

current_child_pid() {
  local pid=""

  pid=$(read_pid_file "${CHILD_PID_PATH}" 2>/dev/null || true)
  if process_matches_child "${pid}"; then
    printf '%s\n' "${pid}"
    return 0
  fi

  remove_pid_file "${CHILD_PID_PATH}"
  find_matching_pid child
}

stop_pid_gracefully() {
  local pid="$1"
  local matcher="$2"
  local attempt

  [[ -n "${pid}" ]] || return 0
  kill "${pid}" >/dev/null 2>&1 || true
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if ! "${matcher}" "${pid}"; then
      return 0
    fi
    sleep 1
  done
  kill -9 "${pid}" >/dev/null 2>&1 || true
}

stop_child_if_running() {
  local pid=""

  pid=$(current_child_pid || true)
  if [[ -n "${pid}" ]]; then
    stop_pid_gracefully "${pid}" process_matches_child
  fi
  remove_pid_file "${CHILD_PID_PATH}"
}

state_ready() {
  local pgdata="$1"
  local state_path="${pgdata}/pg_flashback/meta/summaryd/state.json"
  local debug_path="${pgdata}/pg_flashback/meta/summaryd/debug.json"

  [[ -s "${state_path}" || -s "${debug_path}" ]]
}

wait_for_ready() {
  local pgdata="$1"
  local attempt

  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    if [[ -n "$(current_watchdog_pid || true)" ]] &&
       [[ -n "$(current_child_pid || true)" ]] &&
       state_ready "${pgdata}"; then
      return 0
    fi
    sleep 1
  done

  return 1
}

watchdog_log() {
  local message="$1"

  mkdir -p "${CONFIG_DIR}"
  printf '[pg_flashback_summary] %s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "${message}" >> "${LOG_PATH}"
}

watchdog_handle_signal() {
  WATCHDOG_STOP_REQUESTED=1
  if [[ -n "${WATCHDOG_CHILD_PID}" ]]; then
    kill "${WATCHDOG_CHILD_PID}" >/dev/null 2>&1 || true
  fi
}

watchdog_loop() {
  local pgdata=""
  local summaryd_bin=""
  local child_status=0

  ensure_config_present
  summaryd_bin=$(resolve_summaryd_bin)
  pgdata=$(config_value "pgdata")
  [[ -n "${pgdata}" ]] || fail "config is missing pgdata=..."

  mkdir -p "${CONFIG_DIR}"
  trap watchdog_handle_signal TERM INT
  write_pid_file "${WATCHDOG_PID_PATH}" "$$"

  while true; do
    if [[ ${WATCHDOG_STOP_REQUESTED} -eq 1 ]]; then
      break
    fi

    watchdog_log "starting child daemon"
    "${summaryd_bin}" --config "${CONFIG_PATH}" --foreground >>"${LOG_PATH}" 2>&1 &
    WATCHDOG_CHILD_PID=$!
    write_pid_file "${CHILD_PID_PATH}" "${WATCHDOG_CHILD_PID}"

    if ! wait "${WATCHDOG_CHILD_PID}"; then
      child_status=$?
    else
      child_status=0
    fi

    remove_pid_file "${CHILD_PID_PATH}"
    watchdog_log "child exited status=${child_status}"
    WATCHDOG_CHILD_PID=""

    if [[ ${WATCHDOG_STOP_REQUESTED} -eq 1 ]]; then
      break
    fi
    sleep 1
  done

  stop_child_if_running
  remove_pid_file "${WATCHDOG_PID_PATH}"
}

start_runner() {
  local pgdata=""
  local watchdog_pid=""
  local stray_child_pid=""

  ensure_config_present
  resolve_summaryd_bin >/dev/null
  pgdata=$(config_value "pgdata")
  [[ -n "${pgdata}" ]] || fail "config is missing pgdata=..."

  watchdog_pid=$(current_watchdog_pid || true)
  if [[ -n "${watchdog_pid}" ]]; then
    echo "running watchdog_pid=${watchdog_pid} child_pid=$(current_child_pid || echo missing) config=${CONFIG_PATH}"
    return 0
  fi

  stray_child_pid=$(current_child_pid || true)
  if [[ -n "${stray_child_pid}" ]]; then
    stop_child_if_running
  fi

  mkdir -p "${CONFIG_DIR}"
  nohup env PATH="${PATH}" "${SCRIPT_PATH}" __watchdog-loop >>"${LOG_PATH}" 2>&1 &
  watchdog_pid=$!
  write_pid_file "${WATCHDOG_PID_PATH}" "${watchdog_pid}"

  if ! wait_for_ready "${pgdata}"; then
    tail -n 100 "${LOG_PATH}" >&2 || true
    stop_pid_gracefully "${watchdog_pid}" process_matches_watchdog
    remove_pid_file "${WATCHDOG_PID_PATH}"
    fail "summaryd watchdog failed to publish state files"
  fi

  echo "running watchdog_pid=${watchdog_pid} child_pid=$(current_child_pid) config=${CONFIG_PATH}"
}

stop_runner() {
  local watchdog_pid=""

  watchdog_pid=$(current_watchdog_pid || true)
  if [[ -n "${watchdog_pid}" ]]; then
    stop_pid_gracefully "${watchdog_pid}" process_matches_watchdog
  fi
  remove_pid_file "${WATCHDOG_PID_PATH}"
  stop_child_if_running
  echo "stopped config=${CONFIG_PATH}"
}

status_runner() {
  local watchdog_pid=""
  local child_pid=""

  if [[ -f "${CONFIG_PATH}" ]]; then
    resolve_summaryd_bin >/dev/null 2>&1 || true
  fi
  watchdog_pid=$(current_watchdog_pid || true)
  child_pid=$(current_child_pid || true)

  if [[ -n "${watchdog_pid}" ]]; then
    echo "running watchdog_pid=${watchdog_pid} child_pid=${child_pid:-missing} config=${CONFIG_PATH}"
    return 0
  fi
  if [[ -n "${child_pid}" ]]; then
    echo "degraded watchdog_pid=missing child_pid=${child_pid} config=${CONFIG_PATH}"
    return 1
  fi

  echo "stopped config=${CONFIG_PATH}"
  return 1
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
    __watchdog-loop)
      watchdog_loop
      ;;
  esac
}

main "$@"
