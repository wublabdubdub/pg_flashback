#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

MODE="pilot"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--pilot)
			MODE="pilot"
			shift
			;;
		--full)
			MODE="full"
			shift
			;;
		--dry-run)
			DRY_RUN=1
			shift
			;;
		*)
			echo "unknown option: $1" >&2
			exit 1
			;;
	esac
done

FB_DEEP_MODE="$MODE"
fb_deep_apply_mode

if [[ "$DRY_RUN" -eq 1 ]]; then
	fb_deep_log "dry-run"
	fb_deep_log "mode=$FB_DEEP_MODE"
	fb_deep_log "steps=bootstrap -> load_baseline -> batch_a -> batch_b -> batch_c -> batch_d -> batch_e"
	exit 0
fi

bash "$(dirname "$0")/bootstrap_env.sh" "--$FB_DEEP_MODE"
bash "$(dirname "$0")/load_baseline.sh" "--$FB_DEEP_MODE"
bash "$(dirname "$0")/run_batch_a.sh" "--$FB_DEEP_MODE"
bash "$(dirname "$0")/run_batch_b.sh" "--$FB_DEEP_MODE"
bash "$(dirname "$0")/run_batch_c.sh" "--$FB_DEEP_MODE"
bash "$(dirname "$0")/run_batch_d.sh" "--$FB_DEEP_MODE"
bash "$(dirname "$0")/run_batch_e.sh" "--$FB_DEEP_MODE"

fb_deep_log "deep test run completed: mode=$FB_DEEP_MODE"
