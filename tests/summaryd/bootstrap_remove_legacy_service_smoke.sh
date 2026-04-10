#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_remove_legacy_service_smoke] $*" >&2
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
SYSTEMCTL_LOG="${TMPDIR}/systemctl.log"
OS_RELEASE="${TMPDIR}/os-release-8"
LEGACY_SERVICE="pg_flashback-summaryd-releasegate-18pg.service"
LEGACY_CONFIG="${FAKE_HOME}/.config/pg_flashback/pg_flashback-summaryd-releasegate-18pg.conf"
LEGACY_UNIT="${FAKE_HOME}/.config/systemd/user/${LEGACY_SERVICE}"
DAEMON_BIN="${FAKE_BINDIR}/pg_flashback-summaryd"

mkdir -p \
  "${PGDATA}" \
  "${FAKE_HOME}/.config/pg_flashback" \
  "${FAKE_HOME}/.config/systemd/user" \
  "${FAKE_BINDIR}" \
  "${FAKE_SHAREDIR}" \
  "${FAKE_BIN}"

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
printf '%s\n' "\$*" >> "${FAKE_PSQL_LOG}"
exit 0
EOF

cat > "${DAEMON_BIN}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
while true; do
  sleep 60
done
EOF

cat > "${FAKE_BIN}/systemctl" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "\$*" >> "${SYSTEMCTL_LOG}"
exit 0
EOF

chmod +x \
  "${FAKE_BINDIR}/pg_config" \
  "${FAKE_BINDIR}/psql" \
  "${DAEMON_BIN}" \
  "${FAKE_BIN}/systemctl"

cat > "${LEGACY_CONFIG}" <<EOF
pgdata=${PGDATA}
archive_dest=${PGDATA}/pg_wal
interval_ms=1000
EOF

cat > "${LEGACY_UNIT}" <<'EOF'
[Unit]
Description=fake legacy summaryd
EOF

LEGACY_PID=$(
  run_as_18pg bash -c '
    "$1" --config "$2" --foreground >/dev/null 2>&1 &
    echo $!
  ' _ \
  "${DAEMON_BIN}" \
  "${LEGACY_CONFIG}"
)

kill -0 "${LEGACY_PID}" 2>/dev/null ||
  fail "test setup should start a fake legacy summaryd process"

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

[[ ! -e "${LEGACY_CONFIG}" ]] ||
  fail "remove should delete legacy config for the same PGDATA"
[[ ! -e "${LEGACY_UNIT}" ]] ||
  fail "remove should delete legacy unit for the same PGDATA"
[[ ! -e "${DAEMON_BIN}" ]] ||
  fail "remove should delete the installed summaryd binary"
if kill -0 "${LEGACY_PID}" 2>/dev/null; then
  fail "remove should terminate legacy summaryd processes for the same PGDATA"
fi
grep -Fq "stop ${LEGACY_SERVICE}" "${SYSTEMCTL_LOG}" ||
  fail "remove should stop legacy service for the same PGDATA"
grep -Fq "disable ${LEGACY_SERVICE}" "${SYSTEMCTL_LOG}" ||
  fail "remove should disable legacy service for the same PGDATA"
grep -Fq "action=remove" <<<"${REMOVE_OUTPUT}" ||
  fail "remove should still report remove action"

echo "[summaryd:bootstrap_remove_legacy_service_smoke] PASS"
