#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
BIN="${REPO_ROOT}/summaryd/pg_flashback-summaryd"
TMPDIR=$(mktemp -d)
PGDATA="${TMPDIR}/pgdata"
ARCHIVE="${TMPDIR}/archive"
STATE_JSON="${PGDATA}/pg_flashback/meta/summaryd/state.json"

cleanup() {
  rm -rf "${TMPDIR}"
}
trap cleanup EXIT

fail() {
  echo "[summaryd:no_conninfo_smoke] $*" >&2
  exit 1
}

mkdir -p "${PGDATA}/pg_wal" "${ARCHIVE}"

"${BIN}" \
  --pgdata "${PGDATA}" \
  --archive-dest "${ARCHIVE}" \
  --once \
  --foreground >/dev/null 2>&1 || fail "daemon should succeed without --conninfo"

[[ -f "${STATE_JSON}" ]] || fail "daemon should publish state.json without --conninfo"

echo "[summaryd:no_conninfo_smoke] PASS"
