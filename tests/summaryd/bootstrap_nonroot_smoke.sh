#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_nonroot_smoke] $*" >&2
  exit 1
}

[[ -x "${BOOTSTRAP}" ]] || fail "missing executable: ${BOOTSTRAP}"
[[ $(id -u) -eq 0 ]] || fail "this smoke expects to run as root"

set +e
OUTPUT=$(
  "${BOOTSTRAP}" --help 2>&1
)
STATUS=$?
set -e

[[ ${STATUS} -ne 0 ]] || fail "root execution should fail"
grep -Fq "must be run as a non-root user" <<<"${OUTPUT}" ||
  fail "root execution should explain the non-root requirement"

echo "[summaryd:bootstrap_nonroot_smoke] PASS"
