#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
RUNNER="${REPO_ROOT}/scripts/pg_flashback_summary.sh"

fail() {
  echo "[summaryd:summary_runner_smoke] $*" >&2
  exit 1
}

[[ -x "${RUNNER}" ]] || fail "missing executable: ${RUNNER}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

PGDATA="${TMPDIR}/pgdata"
ARCHIVE_DEST="${TMPDIR}/archive"
CONFIG_PATH="${TMPDIR}/pg_flashback-summaryd.conf"
PID_PATH="${TMPDIR}/pg_flashback-summaryd.pid"
LOG_PATH="${TMPDIR}/pg_flashback-summaryd.log"
FAKE_BIN="${TMPDIR}/fake-bin"
FAKE_DAEMON="${FAKE_BIN}/pg_flashback-summaryd"

mkdir -p "${PGDATA}" "${ARCHIVE_DEST}" "${FAKE_BIN}"

cat > "${CONFIG_PATH}" <<EOF
pgdata=${PGDATA}
archive_dest=${ARCHIVE_DEST}
interval_ms=1000
EOF

cat > "${FAKE_DAEMON}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

config_path=""
once=0
foreground=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      config_path="$2"
      shift 2
      ;;
    --once)
      once=1
      shift
      ;;
    --foreground)
      foreground=1
      shift
      ;;
    *)
      shift
      ;;
  esac
done

[[ -n "${config_path}" ]] || exit 90
[[ ${foreground} -eq 1 ]] || exit 91

pgdata=$(awk -F= '$1=="pgdata"{print $2}' "${config_path}")
[[ -n "${pgdata}" ]] || exit 92

mkdir -p "${pgdata}/pg_flashback/meta/summaryd"
printf '%s\n' '{"service_enabled":true}' > "${pgdata}/pg_flashback/meta/summaryd/state.json"
printf '%s\n' '{"service_enabled":true}' > "${pgdata}/pg_flashback/meta/summaryd/debug.json"

if [[ ${once} -eq 1 ]]; then
  printf '%s\n' "run-once" >> "${pgdata}/pg_flashback/meta/summaryd/invocations.log"
  exit 0
fi

printf '%s\n' "start" >> "${pgdata}/pg_flashback/meta/summaryd/invocations.log"
trap 'exit 0' TERM INT
while true; do
  sleep 60
done
EOF

chmod +x "${FAKE_DAEMON}"

HELP_OUTPUT=$("${RUNNER}" --help 2>&1) || fail "--help failed"
grep -Fq "start" <<<"${HELP_OUTPUT}" || fail "help should mention start"
grep -Fq "stop" <<<"${HELP_OUTPUT}" || fail "help should mention stop"
grep -Fq "status" <<<"${HELP_OUTPUT}" || fail "help should mention status"
grep -Fq "run-once" <<<"${HELP_OUTPUT}" || fail "help should mention run-once"
if grep -Fqi "systemd" <<<"${HELP_OUTPUT}"; then
  fail "help should not mention systemd"
fi

STATUS_BEFORE=$("${RUNNER}" --config "${CONFIG_PATH}" --summaryd-bin "${FAKE_DAEMON}" status 2>&1) || true
grep -Fq "stopped" <<<"${STATUS_BEFORE}" || fail "status should report stopped before start"

"${RUNNER}" --config "${CONFIG_PATH}" --summaryd-bin "${FAKE_DAEMON}" start >/dev/null

[[ -f "${PID_PATH}" ]] || fail "start should create a pid file"
[[ -f "${LOG_PATH}" ]] || fail "start should create a log file"
[[ -f "${PGDATA}/pg_flashback/meta/summaryd/state.json" ]] ||
  fail "start should publish state.json"

STATUS_RUNNING=$("${RUNNER}" --config "${CONFIG_PATH}" --summaryd-bin "${FAKE_DAEMON}" status 2>&1) ||
  fail "status should succeed while running"
grep -Fq "running" <<<"${STATUS_RUNNING}" || fail "status should report running after start"

"${RUNNER}" --config "${CONFIG_PATH}" --summaryd-bin "${FAKE_DAEMON}" stop >/dev/null

STATUS_STOPPED=$("${RUNNER}" --config "${CONFIG_PATH}" --summaryd-bin "${FAKE_DAEMON}" status 2>&1) || true
grep -Fq "stopped" <<<"${STATUS_STOPPED}" || fail "status should report stopped after stop"

rm -f "${PGDATA}/pg_flashback/meta/summaryd/invocations.log"
"${RUNNER}" --config "${CONFIG_PATH}" --summaryd-bin "${FAKE_DAEMON}" run-once >/dev/null

grep -Fq "run-once" "${PGDATA}/pg_flashback/meta/summaryd/invocations.log" ||
  fail "run-once should invoke the daemon once"
[[ ! -f "${PID_PATH}" ]] || fail "run-once should not leave a pid file behind"

echo "[summaryd:summary_runner_smoke] PASS"
