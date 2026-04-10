#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_prompt_validation_smoke] $*" >&2
  exit 1
}

[[ -x "${BOOTSTRAP}" ]] || fail "missing executable: ${BOOTSTRAP}"
id 18pg >/dev/null 2>&1 || fail "required test user does not exist: 18pg"

run_as_18pg() {
  runuser -u 18pg -- "$@"
}

set +e
OUTPUT=$(
  printf '%s\n' '/home/18pg/local/bin/pg+config' |
    run_as_18pg env PATH="${PATH}" "${BOOTSTRAP}" --remove 2>&1
)
STATUS=$?
set -e

[[ ${STATUS} -ne 0 ]] || fail "invalid pg_config should fail"
grep -Fq "pg_config is not executable" <<<"${OUTPUT}" ||
  fail "script should fail immediately on invalid pg_config"
if grep -Fq "failed to read PGDATA" <<<"${OUTPUT}"; then
  fail "script should not continue reading PGDATA after invalid pg_config"
fi

VALID_PG_CONFIG=${PG_CONFIG:-$(command -v pg_config || true)}
[[ -n "${VALID_PG_CONFIG}" ]] || fail "pg_config not found in PATH"

set +e
OUTPUT=$(
  printf '%s\n%s\n' "${VALID_PG_CONFIG}" '/path/does/not/exist' |
    run_as_18pg env PATH="${PATH}" "${BOOTSTRAP}" --remove 2>&1
)
STATUS=$?
set -e

[[ ${STATUS} -ne 0 ]] || fail "invalid PGDATA should fail"
grep -Fq "PGDATA directory not found" <<<"${OUTPUT}" ||
  fail "script should fail immediately on invalid PGDATA"
if grep -Fq "failed to read DB_NAME" <<<"${OUTPUT}"; then
  fail "script should not continue reading database name after invalid PGDATA"
fi

echo "[summaryd:bootstrap_prompt_validation_smoke] PASS"
