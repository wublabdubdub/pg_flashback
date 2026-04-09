#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BIN="${REPO_ROOT}/summaryd/pg_flashback-summaryd"

fail() {
  echo "[summaryd:help_smoke] $*" >&2
  exit 1
}

[[ -x "${BIN}" ]] || fail "missing executable: ${BIN}"

HELP_OUTPUT=$("${BIN}" --help 2>&1) || fail "--help failed"

grep -Fq "pg_flashback-summaryd" <<<"${HELP_OUTPUT}" || fail "help missing program name"
grep -Fq -- "--pgdata" <<<"${HELP_OUTPUT}" || fail "help missing --pgdata"
grep -Fq -- "--archive-dest" <<<"${HELP_OUTPUT}" || fail "help missing --archive-dest"
grep -Fq -- "--interval-ms" <<<"${HELP_OUTPUT}" || fail "help missing --interval-ms"
grep -Fq -- "--once" <<<"${HELP_OUTPUT}" || fail "help missing --once"

echo "[summaryd:help_smoke] PASS"
