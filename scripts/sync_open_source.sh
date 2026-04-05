#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
OPEN_SOURCE_ROOT="${REPO_ROOT}/open_source"
DEST_ROOT="${OPEN_SOURCE_ROOT}/pg_flashback"

REQUIRED_PATHS=(
  "Makefile"
  "README.md"
  "pg_flashback.control"
  "include"
  "src"
  "sql"
  "expected"
)

PUBLIC_FILES=(
  "Makefile"
  "README.md"
  "pg_flashback.control"
)

PUBLIC_DIRS=(
  "include"
  "src"
  "sql"
  "expected"
)

PUBLIC_DOCS=(
  "docs/architecture/overview.md"
  "docs/architecture/error-model.md"
  "docs/architecture/reverse-op-stream.md"
  "docs/architecture/row-identity.md"
  "docs/architecture/wal-decode.md"
  "docs/architecture/wal-source-resolution.md"
  "docs/decisions/ADR-0001-reverse-op-stream.md"
  "docs/decisions/ADR-0002-read-only-first.md"
  "docs/decisions/ADR-0003-user-interface.md"
  "docs/decisions/ADR-0004-wal-source-resolution.md"
  "docs/decisions/ADR-0005-streaming-query-interface.md"
  "docs/decisions/ADR-0009-value-per-call-only-srf.md"
  "docs/decisions/ADR-0010-custom-scan-for-pg-flashback-from.md"
  "docs/decisions/ADR-0026-open-source-mirror-directory.md"
  "docs/decisions/ADR-0027-summary-payload-locator-section.md"
)

EXCLUDED_PATHS=(
  "docs/reports"
  "docs/superpowers"
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

write_docs_readme() {
  mkdir -p "${DEST_ROOT}/docs"
  cat > "${DEST_ROOT}/docs/README.md" <<'EOF'
# docs

本目录只保留面向开源用户仍有价值的公开文档：

- `architecture/`：当前实现架构与运行模型
- `decisions/`：仍代表当前公开口径的关键 ADR

内部研发计划、实验报告、状态跟踪与 agent 过程文档不会进入开源镜像。
EOF
}

write_tests_readme() {
  mkdir -p "${DEST_ROOT}/tests"
  cat > "${DEST_ROOT}/tests/README.md" <<'EOF'
# tests

当前仓库的 PGXS 基础回归资产使用标准布局：

- 回归 SQL 位于根目录 `sql/`
- 期望输出位于根目录 `expected/`
- `make installcheck` 直接读取这些文件

`tests/deep/` 与 `tests/release_gate/` 属于内部重型验证资产，不进入本开源镜像。
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
}

verify_required_outputs() {
  local relpath
  local required_outputs=(
    "Makefile"
    "README.md"
    "pg_flashback.control"
    "include"
    "src"
    "sql"
    "expected"
    "docs/README.md"
    "tests/README.md"
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

  for relpath in "${PUBLIC_DOCS[@]}"; do
    copy_file "${relpath}"
  done

  find "${DEST_ROOT}/src" -type f \( -name '*.o' -o -name '*.so' -o -name '*.bc' \) -delete

  write_mirror_gitignore
  write_docs_readme
  write_tests_readme

  verify_required_outputs
  verify_excluded_paths

  echo "Open-source mirror refreshed at ${DEST_ROOT}"
}

main "$@"
