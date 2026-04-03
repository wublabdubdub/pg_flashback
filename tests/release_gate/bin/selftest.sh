#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMMON_SH="$REPO_ROOT/tests/release_gate/bin/common.sh"
RUN_SH="$REPO_ROOT/tests/release_gate/bin/run_release_gate.sh"
PREPARE_SH="$REPO_ROOT/tests/release_gate/bin/prepare_empty_instance.sh"
SIM_SH="$REPO_ROOT/tests/release_gate/bin/start_alldbsim.sh"
CONFIG_DIR="$REPO_ROOT/tests/release_gate/config"
TEMPLATE_DIR="$REPO_ROOT/tests/release_gate/templates"

fail() {
	echo "[release_gate:selftest] $*" >&2
	exit 1
}

[[ -f "$COMMON_SH" ]] || fail "missing common helper: $COMMON_SH"
[[ -f "$RUN_SH" ]] || fail "missing runner: $RUN_SH"
[[ -f "$PREPARE_SH" ]] || fail "missing prepare script: $PREPARE_SH"
[[ -f "$SIM_SH" ]] || fail "missing simulator wrapper: $SIM_SH"

bash -n "$COMMON_SH" "$RUN_SH" "$PREPARE_SH" "$SIM_SH"

# shellcheck disable=SC1090
source "$COMMON_SH"

for major in 14 15 16 17 18; do
	expected="/walstorage/${major}waldata"
	actual="$(fb_release_gate_archive_dir_from_major "$major")"
	[[ "$actual" == "$expected" ]] || fail "archive dir mismatch for PG${major}: $actual"
done

[[ -f "$CONFIG_DIR/release_gate.conf" ]] || fail "missing release_gate.conf"
[[ -f "$CONFIG_DIR/scenario_matrix.json" ]] || fail "missing scenario_matrix.json"
[[ -f "$CONFIG_DIR/thresholds.json" ]] || fail "missing thresholds.json"
[[ -f "$TEMPLATE_DIR/report.md.tpl" ]] || fail "missing report template"

jq empty \
	"$CONFIG_DIR/scenario_matrix.json" \
	"$CONFIG_DIR/thresholds.json" >/dev/null

echo "[release_gate:selftest] PASS"
