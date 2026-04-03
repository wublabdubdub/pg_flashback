#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"
fb_release_gate_load_config
fb_release_gate_prepare_output_tree

MODE="${1:-start}"
PID_FILE="$(fb_release_gate_sim_pid_file)"
LOG_FILE="$(fb_release_gate_sim_log_file)"
HEALTH_URL="$(fb_release_gate_sim_health_url)"

fb_release_gate_require_cmd curl

start_sim() {
	local pid
	local i

	[[ -x "$FB_RELEASE_GATE_SIM_BIN" ]] || \
		fb_release_gate_fail "simulator binary is not executable: $FB_RELEASE_GATE_SIM_BIN"

	if [[ -f "$PID_FILE" ]]; then
		pid="$(cat "$PID_FILE")"
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
			fb_release_gate_log "alldbsimulator already running pid=$pid"
			return 0
		fi
	fi

	fb_release_gate_log "starting alldbsimulator listen_addr=$FB_RELEASE_GATE_SIM_LISTEN_ADDR"
	SIM_LISTEN_ADDR="$FB_RELEASE_GATE_SIM_LISTEN_ADDR" \
		"$FB_RELEASE_GATE_SIM_BIN" >"$LOG_FILE" 2>&1 &
	pid=$!
	printf '%s\n' "$pid" > "$PID_FILE"

	for i in $(seq 1 30); do
		if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
			fb_release_gate_log "alldbsimulator healthy pid=$pid"
			return 0
		fi
		sleep 1
	done

	fb_release_gate_fail "alldbsimulator did not become healthy: $HEALTH_URL"
}

stop_sim() {
	local pid

	if [[ ! -f "$PID_FILE" ]]; then
		fb_release_gate_log "alldbsimulator pid file not found; nothing to stop"
		return 0
	fi

	pid="$(cat "$PID_FILE")"
	if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid"
		wait "$pid" 2>/dev/null || true
		fb_release_gate_log "stopped alldbsimulator pid=$pid"
	fi
	rm -f "$PID_FILE"
}

case "$MODE" in
	start)
		start_sim
		;;
	stop)
		stop_sim
		;;
	restart)
		stop_sim
		start_sim
		;;
	*)
		fb_release_gate_fail "usage: $0 [start|stop|restart]"
		;;
esac
