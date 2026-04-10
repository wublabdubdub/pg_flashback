#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_archive_autodiscovery_smoke] $*" >&2
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
OS_RELEASE="${TMPDIR}/os-release-8"
FAKE_ARCHIVE_ROOT="${TMPDIR}/archive-root"
FAKE_ARCHIVE_DEST="${FAKE_ARCHIVE_ROOT}/wal"
CONFIG_PATH="${FAKE_HOME}/.config/pg_flashback/pg_flashback-summaryd.conf"
PSQL_LOG="${TMPDIR}/psql.log"
DAEMON_BIN="${FAKE_BINDIR}/pg_flashback-summaryd"

mkdir -p \
  "${PGDATA}" \
  "${FAKE_HOME}" \
  "${FAKE_BINDIR}" \
  "${FAKE_SHAREDIR}" \
  "${FAKE_BIN}" \
  "${FAKE_ARCHIVE_DEST}"

chown -R 18pg:18pg "${TMPDIR}"

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
printf '%s\n' "\$*" >> "${PSQL_LOG}"
args="\$*"
if [[ "\${args}" == *"SELECT current_setting('archive_library', true), current_setting('archive_command', true);"* ]]; then
  printf '|cp %%p %s/%%f\n' "${FAKE_ARCHIVE_DEST}"
  exit 0
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

cat > "${FAKE_BIN}/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--user" ]]; then
  echo "Failed to connect to bus: No such file or directory" >&2
  exit 1
fi
exit 0
EOF

chmod +x \
  "${FAKE_BINDIR}/pg_config" \
  "${FAKE_BINDIR}/psql" \
  "${FAKE_BIN}/make" \
  "${FAKE_BIN}/systemctl"

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
    "postgres" \
    "18pg" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
    "${OS_RELEASE}" \
  2>&1
) || fail "setup should succeed"

grep -Fq "bootstrap_result=ok" <<<"${SETUP_OUTPUT}" ||
  fail "setup should report success"
[[ -f "${CONFIG_PATH}" ]] ||
  fail "setup should write a config file"
grep -Fxq "archive_dest=${FAKE_ARCHIVE_DEST}" "${CONFIG_PATH}" ||
  fail "bootstrap should autodetect archive_dest from archive_command"
if grep -Fq "archive_dest=${PGDATA}/pg_wal" "${CONFIG_PATH}"; then
  fail "bootstrap should not fall back to pg_wal when archive_command is autodetectable"
fi
grep -Fq "${FAKE_ARCHIVE_DEST}" "${PSQL_LOG}" ||
  fail "ALTER DATABASE should use the autodetected archive_dest"

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
    "postgres" \
    "18pg" \
    "secret" \
    "5832" \
    "${BOOTSTRAP}" \
    "${OS_RELEASE}" \
  2>&1
) || fail "remove should succeed"

grep -Fq "bootstrap_result=ok" <<<"${REMOVE_OUTPUT}" ||
  fail "remove should report success"

echo "[summaryd:bootstrap_archive_autodiscovery_smoke] PASS"
