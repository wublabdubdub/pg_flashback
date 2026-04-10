#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_manual_runner_smoke] $*" >&2
  exit 1
}

[[ -x "${BOOTSTRAP}" ]] || fail "missing executable: ${BOOTSTRAP}"
id 18pg >/dev/null 2>&1 || fail "required test user does not exist: 18pg"

run_as_18pg() {
  runuser -u 18pg -- "$@"
}

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

PGDATA="${TMPDIR}/pgdata"
FAKE_HOME="${TMPDIR}/fake-home"
FAKE_PG_ROOT="${TMPDIR}/fake-pg"
FAKE_BINDIR="${FAKE_PG_ROOT}/bin"
FAKE_SHAREDIR="${FAKE_PG_ROOT}/share"
FAKE_BIN="${TMPDIR}/fake-bin"
CONFIG_PATH="${FAKE_HOME}/.config/pg_flashback/pg_flashback-summaryd.conf"
PID_PATH="${FAKE_HOME}/.config/pg_flashback/pg_flashback-summaryd.pid"
LOG_PATH="${FAKE_HOME}/.config/pg_flashback/pg_flashback-summaryd.log"
STATE_PATH="${PGDATA}/pg_flashback/meta/summaryd/state.json"
RUNNER_PATH="${REPO_ROOT}/scripts/pg_flashback_summary.sh"

mkdir -p \
  "${PGDATA}" \
  "${FAKE_HOME}" \
  "${FAKE_BINDIR}" \
  "${FAKE_SHAREDIR}" \
  "${FAKE_BIN}"

chown -R 18pg:18pg "${TMPDIR}"

cat > "${FAKE_BINDIR}/pg_config" <<EOF
#!/usr/bin/env bash
set -euo pipefail
case "\${1:-}" in
  --bindir) printf '%s\n' "${FAKE_BINDIR}" ;;
  --sharedir) printf '%s\n' "${FAKE_SHAREDIR}" ;;
  *) exit 1 ;;
esac
EOF

cat > "${FAKE_BINDIR}/psql" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF

cat > "${FAKE_BINDIR}/pg_flashback-summaryd" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

config_path=""
once=0

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
    *)
      shift
      ;;
  esac
done

[[ -n "${config_path}" ]] || exit 90
pgdata=$(awk -F= '$1=="pgdata"{print $2}' "${config_path}")
[[ -n "${pgdata}" ]] || exit 91

mkdir -p "${pgdata}/pg_flashback/meta/summaryd"
printf '%s\n' '{"service_enabled":true}' > "${pgdata}/pg_flashback/meta/summaryd/state.json"
printf '%s\n' '{"service_enabled":true}' > "${pgdata}/pg_flashback/meta/summaryd/debug.json"

if [[ ${once} -eq 1 ]]; then
  exit 0
fi

trap 'exit 0' TERM INT
while true; do
  sleep 60
done
EOF

cat > "${FAKE_BIN}/make" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

chmod +x \
  "${FAKE_BINDIR}/pg_config" \
  "${FAKE_BINDIR}/psql" \
  "${FAKE_BINDIR}/pg_flashback-summaryd" \
  "${FAKE_BIN}/make"

HELP_OUTPUT=$(run_as_18pg env PATH="${PATH}" "${BOOTSTRAP}" --help 2>&1) || fail "--help failed"
grep -Fq -- "start/stop/status/run-once" <<<"${HELP_OUTPUT}" ||
  fail "help should describe the script runner surface"
if grep -Fq -- "--systemd-scope" <<<"${HELP_OUTPUT}"; then
  fail "help should not mention --systemd-scope"
fi

DRY_OUTPUT=$(
  run_as_18pg env \
    HOME="${FAKE_HOME}" \
    PATH="${FAKE_BIN}:${FAKE_BINDIR}:${PATH}" \
    bash -c '
      printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
        "$7" --dry-run
    ' _ \
    "${FAKE_BINDIR}/pg_config" \
    "${PGDATA}" \
    "postgres" \
    "18pg" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
  2>&1
) || fail "dry-run should succeed"

grep -Fq "runner_script=${RUNNER_PATH}" <<<"${DRY_OUTPUT}" ||
  fail "dry-run should report the runner script"
grep -Fq "config_path=${CONFIG_PATH}" <<<"${DRY_OUTPUT}" ||
  fail "dry-run should report the config path"
grep -Fq "pid_path=${PID_PATH}" <<<"${DRY_OUTPUT}" ||
  fail "dry-run should report the pid path"
grep -Fq "log_path=${LOG_PATH}" <<<"${DRY_OUTPUT}" ||
  fail "dry-run should report the log path"
if grep -Fq "systemd_scope=" <<<"${DRY_OUTPUT}"; then
  fail "dry-run should not report systemd scope"
fi
if grep -Fq "unit_path=" <<<"${DRY_OUTPUT}"; then
  fail "dry-run should not report a systemd unit path"
fi

SETUP_OUTPUT=$(
  run_as_18pg env \
    HOME="${FAKE_HOME}" \
    PATH="${FAKE_BIN}:${FAKE_BINDIR}:${PATH}" \
    bash -c '
      printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
        "$7"
    ' _ \
    "${FAKE_BINDIR}/pg_config" \
    "${PGDATA}" \
    "postgres" \
    "18pg" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
  2>&1
) || fail "setup should succeed with the script runner"

grep -Fq "bootstrap_result=ok" <<<"${SETUP_OUTPUT}" ||
  fail "setup should report success"
grep -Fq "runner_script=${RUNNER_PATH}" <<<"${SETUP_OUTPUT}" ||
  fail "setup should report the runner script"
[[ -f "${CONFIG_PATH}" ]] || fail "setup should write the config"
[[ -f "${PID_PATH}" ]] || fail "setup should leave a pid file"
[[ -f "${STATE_PATH}" ]] || fail "setup should let the daemon publish state.json"

REMOVE_OUTPUT=$(
  run_as_18pg env \
    HOME="${FAKE_HOME}" \
    PATH="${FAKE_BIN}:${FAKE_BINDIR}:${PATH}" \
    bash -c '
      printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
        "$7" --remove
    ' _ \
    "${FAKE_BINDIR}/pg_config" \
    "${PGDATA}" \
    "postgres" \
    "18pg" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
  2>&1
) || fail "remove should succeed with the script runner"

grep -Fq "bootstrap_result=ok" <<<"${REMOVE_OUTPUT}" ||
  fail "remove should report success"
[[ ! -e "${CONFIG_PATH}" ]] || fail "remove should delete the config"
[[ ! -e "${PID_PATH}" ]] || fail "remove should delete the pid file"
[[ ! -e "${LOG_PATH}" ]] || fail "remove should delete the log file"

echo "[summaryd:bootstrap_manual_runner_smoke] PASS"
