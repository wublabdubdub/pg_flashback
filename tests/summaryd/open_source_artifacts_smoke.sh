#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
MIRROR_ROOT="${REPO_ROOT}/open_source/pg_flashback"

fail() {
  echo "[summaryd:open_source_artifacts_smoke] $*" >&2
  exit 1
}

[[ -d "${MIRROR_ROOT}" ]] || fail "missing mirror root: ${MIRROR_ROOT}"

if find "${MIRROR_ROOT}" -type f \( -name '*.o' -o -name '*.so' -o -name '*.bc' \) | grep -q .; then
  fail "build artifacts leaked into open_source mirror"
fi

[[ ! -e "${MIRROR_ROOT}/summaryd/pg_flashback-summaryd" ]] ||
  fail "built summaryd binary leaked into open_source mirror"
[[ ! -e "${MIRROR_ROOT}/summaryd/pg_flashback-summaryd.conf" ]] ||
  fail "generated summaryd config leaked into open_source mirror"
[[ -e "${MIRROR_ROOT}/scripts/pg_flashback_summary.sh" ]] ||
  fail "summary runner script should be mirrored into open_source"

echo "[summaryd:open_source_artifacts_smoke] PASS"
