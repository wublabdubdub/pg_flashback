#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BIN="${REPO_ROOT}/summaryd/pg_flashback-summaryd"
TMPDIR=$(mktemp -d)
CONF="${TMPDIR}/summaryd.conf"
STATE_JSON="${TMPDIR}/pgdata/pg_flashback/meta/summaryd/state.json"

cleanup() {
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

fail() {
  echo "[summaryd:config_smoke] $*" >&2
  exit 1
}

[[ -x "${BIN}" ]] || fail "missing executable: ${BIN}"

cat >"${CONF}" <<EOF
pgdata=${TMPDIR}/pgdata
archive_dest=${TMPDIR}/archive
interval_ms=1
foreground=true
EOF

mkdir -p "${TMPDIR}/pgdata/pg_wal" "${TMPDIR}/archive"

"${BIN}" --config "${CONF}" --once >/dev/null 2>&1 || fail "config file should allow standalone sweep"
[[ -f "${STATE_JSON}" ]] || fail "config path should publish state.json"

echo "[summaryd:config_smoke] PASS"
