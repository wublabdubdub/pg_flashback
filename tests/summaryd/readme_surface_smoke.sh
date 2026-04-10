#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)

fail() {
  echo "[summaryd:readme_surface_smoke] $*" >&2
  exit 1
}

assert_no_conninfo() {
  local file="$1"

  [[ -f "${file}" ]] || fail "missing file: ${file}"
  if rg -n --fixed-strings -- '--conninfo' "${file}" >/dev/null; then
    fail "${file} should not mention --conninfo"
  fi
  if rg -n --fixed-strings 'conninfo=' "${file}" >/dev/null; then
    fail "${file} should not mention conninfo="
  fi
}

assert_contains() {
  local file="$1"
  local pattern="$2"

  rg -n --fixed-strings -- "${pattern}" "${file}" >/dev/null ||
    fail "${file} should mention ${pattern}"
}

assert_no_conninfo "${REPO_ROOT}/README.md"
assert_no_conninfo "${REPO_ROOT}/summaryd/README.md"
assert_no_conninfo "${REPO_ROOT}/summaryd/pg_flashback-summaryd.conf.sample"

assert_contains "${REPO_ROOT}/README.md" "pg_flashback-summaryd"
assert_contains "${REPO_ROOT}/README.md" "scripts/b_pg_flashback.sh"
assert_contains "${REPO_ROOT}/README.md" "scripts/pg_flashback_summary.sh"
assert_contains "${REPO_ROOT}/README.md" "交互"
assert_contains "${REPO_ROOT}/README.md" "interactive"
assert_contains "${REPO_ROOT}/summaryd/README.md" "scripts/b_pg_flashback.sh"
assert_contains "${REPO_ROOT}/summaryd/README.md" "scripts/pg_flashback_summary.sh"

if rg -n --fixed-strings "bootstrap_pg_flashback.sh" "${REPO_ROOT}/README.md" "${REPO_ROOT}/summaryd/README.md" >/dev/null; then
  fail "README surfaces should not mention bootstrap_pg_flashback.sh"
fi

if rg -n --fixed-strings "systemd" "${REPO_ROOT}/README.md" "${REPO_ROOT}/summaryd/README.md" >/dev/null; then
  fail "README surfaces should not mention systemd"
fi

if rg -n --fixed-strings "direct manager" "${REPO_ROOT}/README.md" "${REPO_ROOT}/summaryd/README.md" >/dev/null; then
  fail "README surfaces should not mention direct manager"
fi

echo "[summaryd:readme_surface_smoke] PASS"
