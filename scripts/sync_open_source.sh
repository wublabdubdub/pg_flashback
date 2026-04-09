#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
OPEN_SOURCE_ROOT="${REPO_ROOT}/open_source"
DEST_ROOT="${OPEN_SOURCE_ROOT}/pg_flashback"

REQUIRED_PATHS=(
  "Makefile"
  "README.md"
  "LICENSE"
  "VERSION"
  "pg_flashback.control"
  "summaryd"
  "include"
  "src"
  "sql"
  "expected"
)

PUBLIC_FILES=(
  "Makefile"
  "README.md"
  "LICENSE"
  "VERSION"
  "pg_flashback.control"
)

PUBLIC_DIRS=(
  "summaryd"
  "include"
  "src"
  "sql"
  "expected"
)

EXCLUDED_PATHS=(
  "docs"
  "docs/reports"
  "docs/superpowers"
  "tests"
  "tests/deep"
  "tests/release_gate"
  "results"
)

fail() {
  echo "sync_open_source: $*" >&2
  exit 1
}

copy_file() {
  local relpath="$1"
  mkdir -p "${DEST_ROOT}/$(dirname "${relpath}")"
  cp "${REPO_ROOT}/${relpath}" "${DEST_ROOT}/${relpath}"
}

copy_dir() {
  local relpath="$1"
  mkdir -p "${DEST_ROOT}/$(dirname "${relpath}")"
  cp -a "${REPO_ROOT}/${relpath}" "${DEST_ROOT}/${relpath}"
}

write_mirror_gitignore() {
  cat > "${DEST_ROOT}/.gitignore" <<'EOF'
*.o
*.so
*.bc
*.gcno
*.gcda
results/
tmp/
.DS_Store
EOF
}

verify_required_paths() {
  local relpath
  for relpath in "${REQUIRED_PATHS[@]}"; do
    [[ -e "${REPO_ROOT}/${relpath}" ]] || fail "missing required path: ${relpath}"
  done
}

verify_excluded_paths() {
  local relpath
  for relpath in "${EXCLUDED_PATHS[@]}"; do
    [[ ! -e "${DEST_ROOT}/${relpath}" ]] || fail "excluded path leaked into mirror: ${relpath}"
  done
  [[ ! -e "${DEST_ROOT}/summaryd/pg_flashback-summaryd" ]] || fail "built daemon binary leaked into mirror"
}

verify_required_outputs() {
  local relpath
  local required_outputs=(
    "Makefile"
    "README.md"
    "LICENSE"
    "VERSION"
    "pg_flashback.control"
    "summaryd"
    "include"
    "src"
    "sql"
    "expected"
    ".gitignore"
  )

  for relpath in "${required_outputs[@]}"; do
    [[ -e "${DEST_ROOT}/${relpath}" ]] || fail "missing mirrored output: ${relpath}"
  done
}

main() {
  local relpath

  verify_required_paths

  rm -rf "${DEST_ROOT}"
  mkdir -p "${DEST_ROOT}"

  for relpath in "${PUBLIC_FILES[@]}"; do
    copy_file "${relpath}"
  done

  for relpath in "${PUBLIC_DIRS[@]}"; do
    copy_dir "${relpath}"
  done

  find "${DEST_ROOT}/src" -type f \( -name '*.o' -o -name '*.so' -o -name '*.bc' \) -delete
  rm -f "${DEST_ROOT}/summaryd/pg_flashback-summaryd"

  write_mirror_gitignore

  verify_required_outputs
  verify_excluded_paths

  echo "Open-source mirror refreshed at ${DEST_ROOT}"
}

main "$@"
