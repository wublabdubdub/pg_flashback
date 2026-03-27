#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCK_FILE="${REPO_DIR}/.git/cron_daily_update.lock"

export PATH="/usr/local/bin:/usr/bin:/bin"

cd "${REPO_DIR}"

log() {
    printf '[%s] %s\n' "$(date '+%F %T %Z')" "$*"
}

exec 9>"${LOCK_FILE}"
flock -n 9 || exit 0

if [[ -z "$(git status --porcelain)" ]]; then
    log "no changes detected"
    exit 0
fi

if [[ "${FB_CRON_DRY_RUN:-0}" == "1" ]]; then
    log "dry-run: changes detected for origin/main auto update"
    git status --short
    exit 0
fi

git add -A

if git diff --cached --quiet; then
    log "nothing staged after git add -A"
    exit 0
fi

log "committing changes to origin/main"
git commit -m "update"
log "pushing to origin/main"
git push origin main
log "auto update finished"
