#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"
PG_CONFIG_BIN=${PG_CONFIG:-$(command -v pg_config || true)}

fail() {
  echo "[summaryd:bootstrap_help_smoke] $*" >&2
  exit 1
}

[[ -x "${BOOTSTRAP}" ]] || fail "missing executable: ${BOOTSTRAP}"
[[ -n "${PG_CONFIG_BIN}" ]] || fail "pg_config not found in PATH"
id 18pg >/dev/null 2>&1 || fail "required test user does not exist: 18pg"

run_as_18pg() {
  runuser -u 18pg -- "$@"
}

HELP_OUTPUT=$(run_as_18pg env PATH="${PATH}" "${BOOTSTRAP}" --help 2>&1) || fail "--help failed"

grep -Fq -- "scripts/b_pg_flashback.sh" <<<"${HELP_OUTPUT}" || fail "help missing bootstrap script name"
grep -Fq -- "interactive" <<<"${HELP_OUTPUT}" || fail "help should describe interactive mode"
grep -Fq -- "--db-password" <<<"${HELP_OUTPUT}" || fail "help missing --db-password"
grep -Fq -- "--db-port" <<<"${HELP_OUTPUT}" || fail "help missing --db-port"
grep -Fq -- "--archive-dest" <<<"${HELP_OUTPUT}" || fail "help missing --archive-dest"
grep -Fq -- "--dry-run" <<<"${HELP_OUTPUT}" || fail "help missing --dry-run"
grep -Fq -- "--remove" <<<"${HELP_OUTPUT}" || fail "help missing --remove"
grep -Fq -- "start/stop/status/run-once" <<<"${HELP_OUTPUT}" ||
  fail "help should describe the script runner surface"
if grep -Fq -- "--systemd-scope" <<<"${HELP_OUTPUT}"; then
  fail "help should not mention --systemd-scope"
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT
mkdir -p "${TMPDIR}/pgdata"
chown -R 18pg:18pg "${TMPDIR}"

DRY_OUTPUT=$(
  run_as_18pg env PATH="${PATH}" bash -c '
    printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
      "$7" --dry-run
  ' _ \
    "${PG_CONFIG_BIN}" \
    "${TMPDIR}/pgdata" \
    "postgres" \
    "postgres" \
    "secret" \
    "5432" \
    "${BOOTSTRAP}"
) || fail "dry-run failed"

grep -Fq "action=setup" <<<"${DRY_OUTPUT}" || fail "setup action missing in dry-run"
grep -Fq "service_name=pg_flashback-summaryd" <<<"${DRY_OUTPUT}" ||
  fail "service name should remain fixed"
grep -Fq "runner_script=${REPO_ROOT}/scripts/pg_flashback_summary.sh" <<<"${DRY_OUTPUT}" ||
  fail "dry-run should report the runner script"
grep -Fq "config_path=" <<<"${DRY_OUTPUT}" || fail "dry-run should report config_path"
grep -Fq "pid_path=" <<<"${DRY_OUTPUT}" || fail "dry-run should report pid_path"
grep -Fq "log_path=" <<<"${DRY_OUTPUT}" || fail "dry-run should report log_path"
if grep -Fq "systemd_scope=" <<<"${DRY_OUTPUT}"; then
  fail "dry-run should not report systemd scope"
fi
if grep -Fq "unit_path=" <<<"${DRY_OUTPUT}"; then
  fail "dry-run should not report a systemd unit path"
fi

DRY_REMOVE=$(
  run_as_18pg env PATH="${PATH}" bash -c '
    printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
      "$7" --dry-run --remove
  ' _ \
    "${PG_CONFIG_BIN}" \
    "${TMPDIR}/pgdata" \
    "postgres" \
    "postgres" \
    "secret" \
    "5432" \
    "${BOOTSTRAP}"
) || fail "dry-run failed for remove action"

grep -Fq "action=remove" <<<"${DRY_REMOVE}" || fail "remove action missing in dry-run"

echo "[summaryd:bootstrap_help_smoke] PASS"
