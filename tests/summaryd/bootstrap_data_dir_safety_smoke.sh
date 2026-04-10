#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_data_dir_safety_smoke] $*" >&2
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
FAKE_PSQL_LOG="${TMPDIR}/psql.log"
OS_RELEASE="${TMPDIR}/os-release-8"
DAEMON_BIN="${FAKE_BINDIR}/pg_flashback-summaryd"

mkdir -p \
  "${PGDATA}/pg_flashback/runtime" \
  "${PGDATA}/pg_flashback/recovered_wal" \
  "${PGDATA}/pg_flashback/meta/summary" \
  "${PGDATA}/pg_flashback/meta/summaryd" \
  "${FAKE_HOME}" \
  "${FAKE_BINDIR}" \
  "${FAKE_SHAREDIR}" \
  "${FAKE_BIN}"

chown -R 18pg:18pg "${TMPDIR}"
chown 18pg:18pg "${FAKE_HOME}"

cat > "${OS_RELEASE}" <<'EOF'
ID="centos"
NAME="CentOS Linux"
VERSION_ID="8"
EOF

cat > "${FAKE_BINDIR}/pg_config" <<EOF
#!/usr/bin/env bash
set -euo pipefail
case "\${1:-}" in
  --bindir) printf '%s\n' "${FAKE_BINDIR}" ;;
  --sharedir) printf '%s\n' "${FAKE_SHAREDIR}" ;;
  *) exit 1 ;;
esac
EOF

cat > "${FAKE_BINDIR}/psql" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "\$*" >> "${FAKE_PSQL_LOG}"
exit 0
EOF

cat > "${DAEMON_BIN}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
config_path=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      config_path="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
pgdata=$(awk -F= '$1=="pgdata"{print $2}' "${config_path}")
mkdir -p "${pgdata}/pg_flashback/meta/summaryd"
printf '%s\n' '{"status":"ok"}' > "${pgdata}/pg_flashback/meta/summaryd/state.json"
printf '%s\n' '{"status":"ok"}' > "${pgdata}/pg_flashback/meta/summaryd/debug.json"
trap 'exit 0' TERM INT
while true; do
  sleep 60
done
EOF

cat > "${FAKE_BIN}/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--user" ]]; then
  shift
fi
exit 0
EOF

cat > "${FAKE_BIN}/make" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat > "${DAEMON_BIN}" <<'INNER'
#!/usr/bin/env bash
set -euo pipefail
config_path=""
while [[ \$# -gt 0 ]]; do
  case "\$1" in
    --config)
      config_path="\$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
pgdata=\$(awk -F= '\$1=="pgdata"{print \$2}' "\${config_path}")
mkdir -p "\${pgdata}/pg_flashback/meta/summaryd"
printf '%s\n' '{"status":"ok"}' > "\${pgdata}/pg_flashback/meta/summaryd/state.json"
printf '%s\n' '{"status":"ok"}' > "\${pgdata}/pg_flashback/meta/summaryd/debug.json"
trap 'exit 0' TERM INT
while true; do
  sleep 60
done
INNER
chmod +x "${DAEMON_BIN}"
exit 0
EOF

chmod +x \
  "${FAKE_BINDIR}/pg_config" \
  "${FAKE_BINDIR}/psql" \
  "${DAEMON_BIN}" \
  "${FAKE_BIN}/systemctl" \
  "${FAKE_BIN}/make"

printf 'keep-runtime\n' > "${PGDATA}/pg_flashback/runtime/fbspill-old"
printf 'keep-recovered\n' > "${PGDATA}/pg_flashback/recovered_wal/000000010000000000000001"
printf 'keep-summary\n' > "${PGDATA}/pg_flashback/meta/summary/summary-keep.meta"
printf 'tmp-summary\n' > "${PGDATA}/pg_flashback/meta/summary/.tmp.build"
printf 'state\n' > "${PGDATA}/pg_flashback/meta/summaryd/state.json"
printf 'debug\n' > "${PGDATA}/pg_flashback/meta/summaryd/debug.json"
printf 'lock\n' > "${PGDATA}/pg_flashback/meta/summaryd/daemon.lock"

REMOVE_OUTPUT=$(
  run_as_18pg env \
    HOME="${FAKE_HOME}" \
    PATH="${FAKE_BIN}:${FAKE_BINDIR}:${PATH}" \
    bash -c '
    printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
      "$7" --remove --os-release-file "$8"
  ' _ \
    "${FAKE_BINDIR}/pg_config" \
    "${PGDATA}" \
    "alldb" \
    "admin" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
    "${OS_RELEASE}" \
  2>&1
) || fail "remove should exit successfully"

[[ -d "${PGDATA}/pg_flashback" ]] ||
  fail "remove should preserve PGDATA/pg_flashback"
[[ -f "${PGDATA}/pg_flashback/runtime/fbspill-old" ]] ||
  fail "remove should not delete runtime contents"
grep -Fq "Retained Data Directory" <<<"${REMOVE_OUTPUT}" ||
  fail "remove should print retained data-directory guidance"
grep -Fq "${PGDATA}/pg_flashback" <<<"${REMOVE_OUTPUT}" ||
  fail "remove should print the retained pg_flashback path"
grep -Fq "[1/" <<<"${REMOVE_OUTPUT}" ||
  fail "remove output should use stage separators"

SETUP_OUTPUT=$(
  run_as_18pg env \
    HOME="${FAKE_HOME}" \
    PATH="${FAKE_BIN}:${FAKE_BINDIR}:${PATH}" \
    bash -c '
    printf "%s\n" "$1" "$2" "$3" "$4" "$5" "$6" |
      "$7" --os-release-file "$8"
  ' _ \
    "${FAKE_BINDIR}/pg_config" \
    "${PGDATA}" \
    "alldb" \
    "admin" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
    "${OS_RELEASE}" \
  2>&1
) || fail "setup should exit successfully"

[[ ! -e "${PGDATA}/pg_flashback/runtime/fbspill-old" ]] ||
  fail "setup safe cleanup should remove runtime spill files"
[[ -f "${PGDATA}/pg_flashback/meta/summaryd/state.json" ]] ||
  fail "runner start should republish summaryd state"
[[ -f "${PGDATA}/pg_flashback/meta/summaryd/debug.json" ]] ||
  fail "runner start should republish summaryd debug state"
if grep -Fxq "state" "${PGDATA}/pg_flashback/meta/summaryd/state.json"; then
  fail "setup should replace stale summaryd state content"
fi
if grep -Fxq "debug" "${PGDATA}/pg_flashback/meta/summaryd/debug.json"; then
  fail "setup should replace stale summaryd debug content"
fi
[[ ! -e "${PGDATA}/pg_flashback/meta/summaryd/daemon.lock" ]] ||
  fail "setup safe cleanup should remove stale summaryd lock files"
[[ ! -e "${PGDATA}/pg_flashback/meta/summary/.tmp.build" ]] ||
  fail "setup safe cleanup should remove temporary summary files"
[[ -f "${PGDATA}/pg_flashback/recovered_wal/000000010000000000000001" ]] ||
  fail "setup safe cleanup should preserve recovered_wal files"
[[ -f "${PGDATA}/pg_flashback/meta/summary/summary-keep.meta" ]] ||
  fail "setup safe cleanup should preserve committed summary files"
grep -Fq "Safe Cleanup" <<<"${SETUP_OUTPUT}" ||
  fail "setup should print a safe-cleanup stage"
grep -Fq "[1/" <<<"${SETUP_OUTPUT}" ||
  fail "setup output should use stage separators"

echo "[summaryd:bootstrap_data_dir_safety_smoke] PASS"
