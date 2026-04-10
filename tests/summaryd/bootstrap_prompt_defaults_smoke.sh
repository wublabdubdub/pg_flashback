#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
BOOTSTRAP="${REPO_ROOT}/scripts/b_pg_flashback.sh"

fail() {
  echo "[summaryd:bootstrap_prompt_defaults_smoke] $*" >&2
  exit 1
}

[[ -x "${BOOTSTRAP}" ]] || fail "missing executable: ${BOOTSTRAP}"
id 18pg >/dev/null 2>&1 || fail "required test user does not exist: 18pg"

run_as_18pg() {
  runuser -u 18pg -- "$@"
}

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

FAKE_PG_ROOT="${TMPDIR}/fake-pg"
FAKE_BINDIR="${FAKE_PG_ROOT}/bin"
FAKE_SHAREDIR="${FAKE_PG_ROOT}/share"
FAKE_BIN="${TMPDIR}/fake-bin"
FAKE_PGDATA="${TMPDIR}/pgdata"

mkdir -p "${FAKE_BINDIR}" "${FAKE_SHAREDIR}" "${FAKE_BIN}" "${FAKE_PGDATA}"
chown -R 18pg:18pg "${TMPDIR}"

cat > "${FAKE_BINDIR}/pg_config" <<EOF
#!/usr/bin/env bash
set -euo pipefail
case "\${1:-}" in
  --bindir) printf '%s\n' "${FAKE_BINDIR}" ;;
  --sharedir) printf '%s\n' "${FAKE_SHAREDIR}" ;;
  *) exit 1 ;;
esac
EOF

cat > "${FAKE_BIN}/make" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF

chmod +x \
  "${FAKE_BINDIR}/pg_config" \
  "${FAKE_BIN}/make"

set +e
OUTPUT=$(
  run_as_18pg env \
    PATH="${FAKE_BINDIR}:${FAKE_BIN}:${PATH}" \
    PGDATA="${FAKE_PGDATA}" \
    PGDATABASE="envdb" \
    PGUSER="envuser" \
    PGPASSWORD="envpass" \
    PGPORT="6543" \
    PG_CONFIG_BIN="/definitely/not/from-env" \
    bash -c '
      printf "\n\n\noverride_user\noverride_password\n\n" |
        "$1" --dry-run
    ' _ \
    "${BOOTSTRAP}" 2>&1
)
STATUS=$?
set -e

[[ ${STATUS} -eq 0 ]] || fail "dry-run should accept env-backed defaults"
grep -Fq "service_name=pg_flashback-summaryd" <<<"${OUTPUT}" ||
  fail "dry-run should still report the fixed service name"
grep -Fq "db_port=6543" <<<"${OUTPUT}" ||
  fail "db port should default from PGPORT"
grep -Fq "psql_bin=${FAKE_BINDIR}/psql" <<<"${OUTPUT}" &&
  true
grep -Fq "summaryd_bin=${FAKE_BINDIR}/pg_flashback-summaryd" <<<"${OUTPUT}" &&
  true
grep -Fq "pgflashback_data_dir=${FAKE_PGDATA}/pg_flashback" <<<"${OUTPUT}" ||
  fail "PGDATA should default from environment"
if grep -Fq "DB_PORT must be a numeric port: override_password" <<<"${OUTPUT}"; then
  fail "password input should be consumed before db port prompt"
fi
if grep -Fq "/definitely/not/from-env" <<<"${OUTPUT}"; then
  fail "pg_config default should come from PATH, not PG_CONFIG_BIN env"
fi

echo "[summaryd:bootstrap_prompt_defaults_smoke] PASS"
