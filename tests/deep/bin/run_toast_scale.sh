#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

if [[ "${1:-}" == "--full" ]]; then
	FB_DEEP_MODE=full
elif [[ "${1:-}" == "--pilot" || $# -eq 0 ]]; then
	FB_DEEP_MODE=pilot
else
	echo "unknown option: ${1:-}" >&2
	exit 1
fi

fb_deep_apply_mode

run_toast_full_round() {
	local start_lsn

	bash "$(dirname "$0")/bootstrap_env.sh" --full || return $?
	start_lsn="$(fb_deep_current_lsn)" || return $?

	mkdir -p "$(fb_deep_output_dir toast)"

	fb_deep_psql_file "$FB_DEEP_SQL_DIR/80_toast_scale.sql" \
		"toast_row_count=$FB_DEEP_TOAST_ROW_COUNT" \
		"toast_update_count=$FB_DEEP_TOAST_UPDATE_COUNT" \
		"toast_delete_count=$FB_DEEP_TOAST_DELETE_COUNT" \
		"toast_insert_count=$FB_DEEP_TOAST_INSERT_COUNT" \
		"toast_rollback_count=$FB_DEEP_TOAST_ROLLBACK_COUNT" || return $?

	fb_deep_log_wal_budget_since "$start_lsn" "toast_full"
}

if [[ "$FB_DEEP_MODE" == "full" ]]; then
	fb_deep_run_round_with_retry "toast_full" run_toast_full_round
else
	mkdir -p "$(fb_deep_output_dir toast)"

	fb_deep_psql_file "$FB_DEEP_SQL_DIR/80_toast_scale.sql" \
		"toast_row_count=$FB_DEEP_TOAST_ROW_COUNT" \
		"toast_update_count=$FB_DEEP_TOAST_UPDATE_COUNT" \
		"toast_delete_count=$FB_DEEP_TOAST_DELETE_COUNT" \
		"toast_insert_count=$FB_DEEP_TOAST_INSERT_COUNT" \
		"toast_rollback_count=$FB_DEEP_TOAST_ROLLBACK_COUNT"
fi

fb_deep_log "toast scale test completed: mode=$FB_DEEP_MODE"
