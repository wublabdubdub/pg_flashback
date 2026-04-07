#!/usr/bin/env bash
set -euo pipefail

FB_DEEP_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FB_DEEP_SQL_DIR="$FB_DEEP_REPO_ROOT/tests/deep/sql"
FB_DEEP_OUTPUT_ROOT="$FB_DEEP_REPO_ROOT/tests/deep/output"

FB_DEEP_PSQL="${FB_DEEP_PSQL:-/home/18pg/local/bin/psql}"
FB_DEEP_CREATEDB="${FB_DEEP_CREATEDB:-/home/18pg/local/bin/createdb}"
FB_DEEP_DROPDB="${FB_DEEP_DROPDB:-/home/18pg/local/bin/dropdb}"
FB_DEEP_PGCTL="${FB_DEEP_PGCTL:-/home/18pg/local/bin/pg_ctl}"
FB_DEEP_PGPORT="${FB_DEEP_PGPORT:-5832}"
FB_DEEP_DBNAME="${FB_DEEP_DBNAME:-fb_deep_test}"

FB_DEEP_ROOT_DIR="${FB_DEEP_ROOT_DIR:-/tmp/pg_flashback_deep}"
FB_DEEP_ARCHIVE_DIR="${FB_DEEP_ARCHIVE_DIR:-$FB_DEEP_ROOT_DIR/archive}"
FB_DEEP_CKWAL_DIR="${FB_DEEP_CKWAL_DIR:-$FB_DEEP_ROOT_DIR/ckwal}"
FB_DEEP_FAKE_PGWAL_DIR="${FB_DEEP_FAKE_PGWAL_DIR:-$FB_DEEP_ROOT_DIR/fake_pg_wal}"
FB_DEEP_ARCHIVE_CLEAN_DIR="${FB_DEEP_ARCHIVE_CLEAN_DIR:-/isoTest/18waldata}"
FB_DEEP_SNAPSHOT_ROOT="${FB_DEEP_SNAPSHOT_ROOT:-/isoTest/pg_flashback_deep_snapshots}"
FB_DEEP_FULL_WAL_BUDGET_BYTES="${FB_DEEP_FULL_WAL_BUDGET_BYTES:-10737418240}"
FB_DEEP_DISK_WATERMARK_PCT="${FB_DEEP_DISK_WATERMARK_PCT:-85}"
FB_DEEP_GUARD_POLL_SEC="${FB_DEEP_GUARD_POLL_SEC:-5}"
FB_DEEP_RETRY_STATUS=75

FB_DEEP_MODE="${FB_DEEP_MODE:-pilot}"

fb_deep_log() {
	printf '[fb_deep] %s\n' "$*"
}

fb_deep_require_mode() {
	case "${FB_DEEP_MODE}" in
		pilot|full) ;;
		*)
			echo "unsupported mode: ${FB_DEEP_MODE}" >&2
			exit 1
			;;
	esac
}

fb_deep_filesystem_use_pct() {
	local path="$1"

	df -P "$path" | awk 'NR==2 { gsub(/%/, "", $5); print $5 }'
}

fb_deep_filesystem_avail_bytes() {
	local path="$1"

	df -PB1 "$path" | awk 'NR==2 { print $4 }'
}

fb_deep_dir_size_bytes() {
	local path="$1"

	du -sb "$path" | awk '{ print $1 }'
}

fb_deep_disk_watermark_hit() {
	local path
	local usage

	for path in /isoTest / /tmp; do
		usage="$(fb_deep_filesystem_use_pct "$path")"
		if [[ -n "$usage" && "$usage" -ge "$FB_DEEP_DISK_WATERMARK_PCT" ]]; then
			fb_deep_log "disk watermark hit: path=$path usage=${usage}% threshold=${FB_DEEP_DISK_WATERMARK_PCT}%"
			return 0
		fi
	done

	return 1
}

fb_deep_as_18pg_raw() {
	local cmd="$1"
	su - 18pg -c "$cmd"
}

fb_deep_terminate_db_sessions() {
	local cmd

	printf -v cmd 'PGPORT=%q %q -d postgres -Atqc %q' \
		"$FB_DEEP_PGPORT" "$FB_DEEP_PSQL" \
		"select pg_terminate_backend(pid) from pg_stat_activity where datname = '$FB_DEEP_DBNAME' and pid <> pg_backend_pid();"
	fb_deep_as_18pg_raw "$cmd" >/dev/null 2>&1 || true
}

fb_deep_live_archive_mode() {
	[[ "$FB_DEEP_ARCHIVE_DIR" == "$FB_DEEP_ARCHIVE_CLEAN_DIR" ]]
}

fb_deep_cleanup_round_artifacts() {
	mkdir -p "$FB_DEEP_ROOT_DIR" "$FB_DEEP_FAKE_PGWAL_DIR"
	find "$FB_DEEP_ROOT_DIR" -mindepth 1 -delete 2>/dev/null || true
	if ! fb_deep_live_archive_mode; then
		mkdir -p "$FB_DEEP_ARCHIVE_DIR"
		find "$FB_DEEP_ARCHIVE_DIR" -mindepth 1 -delete 2>/dev/null || true
	fi
	find "$FB_DEEP_FAKE_PGWAL_DIR" -mindepth 1 -delete 2>/dev/null || true
	mkdir -p "$FB_DEEP_ARCHIVE_DIR" "$FB_DEEP_CKWAL_DIR" "$FB_DEEP_FAKE_PGWAL_DIR"
}

fb_deep_baseline_snapshot_dir() {
	printf '%s/%s-baseline\n' "$FB_DEEP_SNAPSHOT_ROOT" "$FB_DEEP_DBNAME"
}

fb_deep_full_state_file() {
	printf '%s/full_state.env\n' "$FB_DEEP_OUTPUT_ROOT"
}

fb_deep_pgdata_dir() {
	local cmd

	if [[ -n "${FB_DEEP_PGDATA_DIR_CACHE:-}" ]]; then
		printf '%s\n' "$FB_DEEP_PGDATA_DIR_CACHE"
		return 0
	fi

	printf -v cmd 'PGPORT=%q %q -d %q -Atqc %q' \
		"$FB_DEEP_PGPORT" "$FB_DEEP_PSQL" "$FB_DEEP_DBNAME" \
		"select current_setting('data_directory');"
	FB_DEEP_PGDATA_DIR_CACHE="$(fb_deep_as_18pg_raw "$cmd")"
	printf '%s\n' "$FB_DEEP_PGDATA_DIR_CACHE"
}

fb_deep_write_full_state() {
	local completed_batches="$1"
	local baseline_snapshot_ready="$2"
	local baseline_snapshot_dir="$3"
	local state_file

	state_file="$(fb_deep_full_state_file)"
	mkdir -p "$(dirname "$state_file")"
	cat > "$state_file" <<EOF
completed_batches='$completed_batches'
baseline_snapshot_ready='$baseline_snapshot_ready'
baseline_snapshot_dir='$baseline_snapshot_dir'
EOF
}

fb_deep_load_full_state() {
	local state_file

	FULL_STATE_COMPLETED_BATCHES=""
	FULL_STATE_BASELINE_SNAPSHOT_READY=0
	FULL_STATE_BASELINE_SNAPSHOT_DIR="$(fb_deep_baseline_snapshot_dir)"

	state_file="$(fb_deep_full_state_file)"
	if [[ ! -f "$state_file" ]]; then
		return 0
	fi

	# shellcheck disable=SC1090
	source "$state_file"
	FULL_STATE_COMPLETED_BATCHES="${completed_batches:-}"
	FULL_STATE_BASELINE_SNAPSHOT_READY="${baseline_snapshot_ready:-0}"
	FULL_STATE_BASELINE_SNAPSHOT_DIR="${baseline_snapshot_dir:-$(fb_deep_baseline_snapshot_dir)}"
}

fb_deep_init_full_state() {
	fb_deep_write_full_state "" 0 "$(fb_deep_baseline_snapshot_dir)"
}

fb_deep_reset_full_state() {
	rm -f "$(fb_deep_full_state_file)"
}

fb_deep_full_state_value() {
	local key="$1"

	fb_deep_load_full_state
	case "$key" in
		completed_batches)
			printf '%s\n' "$FULL_STATE_COMPLETED_BATCHES"
			;;
		baseline_snapshot_ready)
			printf '%s\n' "$FULL_STATE_BASELINE_SNAPSHOT_READY"
			;;
		baseline_snapshot_dir)
			printf '%s\n' "$FULL_STATE_BASELINE_SNAPSHOT_DIR"
			;;
		*)
			echo "unknown full state key: $key" >&2
			return 1
			;;
	esac
}

fb_deep_mark_full_batch_completed() {
	local batch="$1"
	local completed
	local item
	local updated=""
	local seen=0

	fb_deep_load_full_state
	completed="$FULL_STATE_COMPLETED_BATCHES"
	IFS=',' read -r -a items <<< "$completed"
	for item in "${items[@]}"; do
		[[ -n "$item" ]] || continue
		if [[ "$item" == "$batch" ]]; then
			seen=1
		fi
		if [[ -z "$updated" ]]; then
			updated="$item"
		else
			updated="$updated,$item"
		fi
	done
	if [[ "$seen" -eq 0 ]]; then
		if [[ -z "$updated" ]]; then
			updated="$batch"
		else
			updated="$updated,$batch"
		fi
	fi

	fb_deep_write_full_state "$updated" "$FULL_STATE_BASELINE_SNAPSHOT_READY" "$FULL_STATE_BASELINE_SNAPSHOT_DIR"
}

fb_deep_full_batch_completed() {
	local batch="$1"
	local completed
	local item

	completed="$(fb_deep_full_state_value completed_batches)"
	IFS=',' read -r -a items <<< "$completed"
	for item in "${items[@]}"; do
		if [[ "$item" == "$batch" ]]; then
			return 0
		fi
	done
	return 1
}

fb_deep_stop_cluster() {
	local datadir
	local status_cmd
	local stop_cmd

	datadir="$(fb_deep_pgdata_dir)"
	printf -v status_cmd '%q -D %q status' "$FB_DEEP_PGCTL" "$datadir"
	if ! fb_deep_as_18pg_raw "$status_cmd" >/dev/null 2>&1; then
		return 0
	fi

	printf -v stop_cmd '%q -D %q -m fast -w stop' "$FB_DEEP_PGCTL" "$datadir"
	fb_deep_as_18pg_raw "$stop_cmd"
}

fb_deep_start_cluster() {
	local datadir
	local logfile
	local status_cmd
	local start_cmd

	datadir="$(fb_deep_pgdata_dir)"
	printf -v status_cmd '%q -D %q status' "$FB_DEEP_PGCTL" "$datadir"
	if fb_deep_as_18pg_raw "$status_cmd" >/dev/null 2>&1; then
		return 0
	fi

	logfile="$datadir/logfile"
	printf -v start_cmd '%q -D %q -l %q -w start' "$FB_DEEP_PGCTL" "$datadir" "$logfile"
	fb_deep_as_18pg_raw "$start_cmd"
}

fb_deep_require_snapshot_capacity() {
	local datadir
	local snapshot_dir
	local pgdata_bytes
	local avail_bytes
	local extra_headroom
	local required_bytes

	datadir="$(fb_deep_pgdata_dir)"
	snapshot_dir="$(fb_deep_baseline_snapshot_dir)"
	mkdir -p "$FB_DEEP_SNAPSHOT_ROOT"

	pgdata_bytes="$(fb_deep_dir_size_bytes "$datadir")"
	avail_bytes="$(fb_deep_filesystem_avail_bytes "$FB_DEEP_SNAPSHOT_ROOT")"
	extra_headroom=$((pgdata_bytes / 5))
	if [[ "$extra_headroom" -lt $((2 * 1024 * 1024 * 1024)) ]]; then
		extra_headroom=$((2 * 1024 * 1024 * 1024))
	fi
	required_bytes=$((pgdata_bytes + extra_headroom))

	if [[ "$avail_bytes" -lt "$required_bytes" ]]; then
		echo "insufficient disk space for baseline snapshot: snapshot_root=$FB_DEEP_SNAPSHOT_ROOT need=${required_bytes}B avail=${avail_bytes}B datadir_size=${pgdata_bytes}B" >&2
		return 1
	fi

	if fb_deep_disk_watermark_hit; then
		echo "cannot create baseline snapshot while filesystem watermark is exceeded" >&2
		return "$FB_DEEP_RETRY_STATUS"
	fi

	fb_deep_log "snapshot capacity ok: snapshot_root=$FB_DEEP_SNAPSHOT_ROOT datadir_size=${pgdata_bytes}B avail=${avail_bytes}B"
}

fb_deep_create_baseline_snapshot() {
	local datadir
	local snapshot_dir
	local tmp_dir
	local status=0
	local start_status=0

	fb_deep_require_snapshot_capacity
	datadir="$(fb_deep_pgdata_dir)"
	snapshot_dir="$(fb_deep_baseline_snapshot_dir)"
	tmp_dir="${snapshot_dir}.tmp"
	mkdir -p "$FB_DEEP_SNAPSHOT_ROOT"

	fb_deep_stop_cluster
	set +e
	rm -rf "$tmp_dir"
	mkdir -p "$tmp_dir"
	rsync -a --delete "$datadir"/ "$tmp_dir"/
	status=$?
	if [[ "$status" -eq 0 ]]; then
		rm -rf "$snapshot_dir"
		mv "$tmp_dir" "$snapshot_dir"
		status=$?
	fi
	fb_deep_start_cluster
	start_status=$?
	set -e
	if [[ "$status" -ne 0 ]]; then
		return "$status"
	fi
	if [[ "$start_status" -ne 0 ]]; then
		return "$start_status"
	fi
	fb_deep_load_full_state
	fb_deep_write_full_state "$FULL_STATE_COMPLETED_BATCHES" 1 "$snapshot_dir"
	fb_deep_log "baseline snapshot created: $snapshot_dir"
}

fb_deep_restore_baseline_snapshot() {
	local datadir
	local snapshot_dir
	local status=0
	local start_status=0

	fb_deep_load_full_state
	snapshot_dir="$FULL_STATE_BASELINE_SNAPSHOT_DIR"
	if [[ ! -d "$snapshot_dir" ]]; then
		echo "baseline snapshot not found: $snapshot_dir" >&2
		return 1
	fi

	if fb_deep_disk_watermark_hit; then
		echo "cannot restore baseline snapshot while filesystem watermark is exceeded" >&2
		return "$FB_DEEP_RETRY_STATUS"
	fi

	datadir="$(fb_deep_pgdata_dir)"
	fb_deep_stop_cluster
	set +e
	rsync -a --delete "$snapshot_dir"/ "$datadir"/
	status=$?
	fb_deep_start_cluster
	start_status=$?
	set -e
	if [[ "$status" -ne 0 ]]; then
		return "$status"
	fi
	if [[ "$start_status" -ne 0 ]]; then
		return "$start_status"
	fi
	fb_deep_log "baseline snapshot restored: $snapshot_dir"
}

fb_deep_remove_baseline_snapshot() {
	local snapshot_dir

	snapshot_dir="$(fb_deep_baseline_snapshot_dir)"
	rm -rf "$snapshot_dir" "${snapshot_dir}.tmp"
}

fb_deep_as_18pg() {
	local cmd="$1"
	local runner_pid
	local status

	fb_deep_as_18pg_raw "$cmd" &
	runner_pid=$!

	while kill -0 "$runner_pid" 2>/dev/null; do
		if fb_deep_disk_watermark_hit; then
			fb_deep_log "aborting current round due to disk watermark"
			kill "$runner_pid" 2>/dev/null || true
			sleep 1
			kill -9 "$runner_pid" 2>/dev/null || true
			wait "$runner_pid" 2>/dev/null || true
			fb_deep_terminate_db_sessions
			return "$FB_DEEP_RETRY_STATUS"
		fi
		sleep "$FB_DEEP_GUARD_POLL_SEC"
	done

	wait "$runner_pid"
	status=$?
	return "$status"
}

fb_deep_apply_mode() {
	fb_deep_require_mode

	if [[ "$FB_DEEP_MODE" == "full" ]]; then
		FB_DEEP_ARCHIVE_DIR="$FB_DEEP_ARCHIVE_CLEAN_DIR"
		FB_DEEP_FAKE_PGWAL_DIR="/isoTest/pg_flashback_deep_fake_pg_wal"
		FB_DEEP_ROW_COUNT=5000000
		FB_DEEP_BAG_DISTINCT=250000
		FB_DEEP_OPERATION_ROW_COUNT=2500000
		FB_DEEP_OPERATION_BUCKET_COUNT=125000
		FB_DEEP_INSERT_COUNT=200000
		FB_DEEP_STRESS_ROUNDS=12
		FB_DEEP_STRESS_INSERT_COUNT=100000
		FB_DEEP_FIXTURE_SEGMENTS=160
		FB_DEEP_MEMORY_LIMIT_KB=2097151
		FB_DEEP_TOAST_ROW_COUNT=25000
		FB_DEEP_TOAST_UPDATE_COUNT=12500
		FB_DEEP_TOAST_DELETE_COUNT=2500
		FB_DEEP_TOAST_INSERT_COUNT=5000
		FB_DEEP_TOAST_ROLLBACK_COUNT=7000
	else
		FB_DEEP_ROW_COUNT=50000
		FB_DEEP_BAG_DISTINCT=2500
		FB_DEEP_OPERATION_ROW_COUNT=50000
		FB_DEEP_OPERATION_BUCKET_COUNT=2500
		FB_DEEP_INSERT_COUNT=3000
		FB_DEEP_STRESS_ROUNDS=4
		FB_DEEP_STRESS_INSERT_COUNT=4000
		FB_DEEP_FIXTURE_SEGMENTS=48
		FB_DEEP_MEMORY_LIMIT_KB=1048576
		FB_DEEP_TOAST_ROW_COUNT=4000
		FB_DEEP_TOAST_UPDATE_COUNT=2000
		FB_DEEP_TOAST_DELETE_COUNT=500
		FB_DEEP_TOAST_INSERT_COUNT=500
		FB_DEEP_TOAST_ROLLBACK_COUNT=1001
	fi

	export FB_DEEP_ROW_COUNT
	export FB_DEEP_BAG_DISTINCT
	export FB_DEEP_OPERATION_ROW_COUNT
	export FB_DEEP_OPERATION_BUCKET_COUNT
	export FB_DEEP_INSERT_COUNT
	export FB_DEEP_STRESS_ROUNDS
	export FB_DEEP_STRESS_INSERT_COUNT
	export FB_DEEP_FIXTURE_SEGMENTS
	export FB_DEEP_MEMORY_LIMIT_KB
	export FB_DEEP_TOAST_ROW_COUNT
	export FB_DEEP_TOAST_UPDATE_COUNT
	export FB_DEEP_TOAST_DELETE_COUNT
	export FB_DEEP_TOAST_INSERT_COUNT
	export FB_DEEP_TOAST_ROLLBACK_COUNT
}

fb_deep_output_dir() {
	local name="$1"
	printf '%s/%s/%s\n' "$FB_DEEP_OUTPUT_ROOT" "$FB_DEEP_MODE" "$name"
}

fb_deep_psql_file() {
	local sql_file="$1"
	shift
	local cmd
	local assignment

	printf -v cmd 'cd %q && PGPORT=%q %q -v ON_ERROR_STOP=1 -d %q -f %q' \
		"$FB_DEEP_REPO_ROOT" "$FB_DEEP_PGPORT" "$FB_DEEP_PSQL" "$FB_DEEP_DBNAME" "$sql_file"

	for assignment in "$@"; do
		printf -v cmd '%s -v %q' "$cmd" "$assignment"
	done

	fb_deep_as_18pg "$cmd"
}

fb_deep_psql_sql() {
	local sql="$1"
	local tmp

	tmp="$(mktemp /tmp/fb_deep_sql.XXXXXX.sql)"
	printf '%s\n' "$sql" > "$tmp"
	chmod 644 "$tmp"
	fb_deep_psql_file "$tmp"
	rm -f "$tmp"
}

fb_deep_db_exists() {
	local cmd
	printf -v cmd 'PGPORT=%q %q -d postgres -Atqc %q' \
		"$FB_DEEP_PGPORT" "$FB_DEEP_PSQL" "select 1 from pg_database where datname = '$FB_DEEP_DBNAME';"
	if [[ "$(fb_deep_as_18pg_raw "$cmd")" == "1" ]]; then
		return 0
	fi
	return 1
}

fb_deep_current_lsn() {
	local cmd

	printf -v cmd 'PGPORT=%q %q -d %q -Atqc %q' \
		"$FB_DEEP_PGPORT" "$FB_DEEP_PSQL" "$FB_DEEP_DBNAME" \
		"select pg_current_wal_insert_lsn();"
	fb_deep_as_18pg_raw "$cmd"
}

fb_deep_wal_bytes_since() {
	local start_lsn="$1"
	local cmd

	printf -v cmd 'PGPORT=%q %q -d %q -Atqc %q' \
		"$FB_DEEP_PGPORT" "$FB_DEEP_PSQL" "$FB_DEEP_DBNAME" \
		"select pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '$start_lsn')::bigint;"
	fb_deep_as_18pg_raw "$cmd"
}

fb_deep_log_wal_budget_since() {
	local start_lsn="$1"
	local label="$2"
	local wal_bytes

	wal_bytes="$(fb_deep_wal_bytes_since "$start_lsn")"
	fb_deep_log "round=$label wal_bytes=${wal_bytes} wal_budget_bytes=${FB_DEEP_FULL_WAL_BUDGET_BYTES}"
}

fb_deep_run_round_with_retry() {
	local label="$1"
	shift
	local status
	local attempt=1

	while true; do
		fb_deep_log "starting round=$label mode=$FB_DEEP_MODE attempt=$attempt"
		fb_deep_cleanup_round_artifacts
		set +e
		"$@"
		status=$?
		set -e
		if [[ "$status" -eq 0 ]]; then
			fb_deep_cleanup_round_artifacts
			fb_deep_log "round completed: $label"
			return 0
		fi

		fb_deep_cleanup_round_artifacts
		if [[ "$status" -eq "$FB_DEEP_RETRY_STATUS" ]]; then
			if fb_deep_live_archive_mode; then
				fb_deep_log "round=$label requested retry after disk cleanup, but live archive is preserved; stop and clean disk/archive manually before resume"
				return "$status"
			fi
			attempt=$((attempt + 1))
			fb_deep_log "retrying round=$label after disk cleanup"
			continue
		fi

		return "$status"
	done
}

fb_deep_write_env_file() {
	local env_file="$FB_DEEP_OUTPUT_ROOT/deep_env.sh"

	cat > "$env_file" <<EOF
export FB_DEEP_PSQL='$FB_DEEP_PSQL'
export FB_DEEP_CREATEDB='$FB_DEEP_CREATEDB'
export FB_DEEP_DROPDB='$FB_DEEP_DROPDB'
export FB_DEEP_PGPORT='$FB_DEEP_PGPORT'
export FB_DEEP_DBNAME='$FB_DEEP_DBNAME'
export FB_DEEP_ROOT_DIR='$FB_DEEP_ROOT_DIR'
export FB_DEEP_ARCHIVE_DIR='$FB_DEEP_ARCHIVE_DIR'
export FB_DEEP_CKWAL_DIR='$FB_DEEP_CKWAL_DIR'
export FB_DEEP_FAKE_PGWAL_DIR='$FB_DEEP_FAKE_PGWAL_DIR'
export FB_DEEP_ARCHIVE_CLEAN_DIR='$FB_DEEP_ARCHIVE_CLEAN_DIR'
export FB_DEEP_PGCTL='$FB_DEEP_PGCTL'
export FB_DEEP_SNAPSHOT_ROOT='$FB_DEEP_SNAPSHOT_ROOT'
export FB_DEEP_FULL_WAL_BUDGET_BYTES='$FB_DEEP_FULL_WAL_BUDGET_BYTES'
export FB_DEEP_DISK_WATERMARK_PCT='$FB_DEEP_DISK_WATERMARK_PCT'
export FB_DEEP_OPERATION_ROW_COUNT='$FB_DEEP_OPERATION_ROW_COUNT'
export FB_DEEP_OPERATION_BUCKET_COUNT='$FB_DEEP_OPERATION_BUCKET_COUNT'
export FB_DEEP_MEMORY_LIMIT_KB='$FB_DEEP_MEMORY_LIMIT_KB'
export FB_DEEP_TOAST_ROW_COUNT='$FB_DEEP_TOAST_ROW_COUNT'
export FB_DEEP_TOAST_UPDATE_COUNT='$FB_DEEP_TOAST_UPDATE_COUNT'
export FB_DEEP_TOAST_DELETE_COUNT='$FB_DEEP_TOAST_DELETE_COUNT'
export FB_DEEP_TOAST_INSERT_COUNT='$FB_DEEP_TOAST_INSERT_COUNT'
export FB_DEEP_TOAST_ROLLBACK_COUNT='$FB_DEEP_TOAST_ROLLBACK_COUNT'
EOF
}

fb_deep_prepare_batch_d_fixture() {
	local seg
	local poison_seg

	fb_deep_refresh_archive_fixture
	rm -rf "$FB_DEEP_FAKE_PGWAL_DIR"
	mkdir -p "$FB_DEEP_FAKE_PGWAL_DIR"

	while IFS= read -r seg; do
		cp "$FB_DEEP_ARCHIVE_DIR/$seg" "$FB_DEEP_FAKE_PGWAL_DIR/$seg"
	done < <(find "$FB_DEEP_ARCHIVE_DIR" -maxdepth 1 -type f -regextype posix-extended \
		-regex '.*/[0-9A-F]{24}(\.partial)?' -printf '%f\n' | sort)

	poison_seg="$(find "$FB_DEEP_ARCHIVE_DIR" -maxdepth 1 -type f -regextype posix-extended \
		-regex '.*/[0-9A-F]{24}$' -printf '%f\n' | sort | tail -n 2 | head -n 1)"

	if [[ -z "$poison_seg" ]]; then
		echo "could not prepare batch D fixture: no WAL segment to poison" >&2
		exit 1
	fi

	printf 'FBPOISON' | dd of="$FB_DEEP_FAKE_PGWAL_DIR/$poison_seg" bs=1 count=8 conv=notrunc status=none
	chmod 755 "$FB_DEEP_ARCHIVE_DIR" "$FB_DEEP_FAKE_PGWAL_DIR"
	find "$FB_DEEP_ARCHIVE_DIR" "$FB_DEEP_FAKE_PGWAL_DIR" -maxdepth 1 -type f -exec chmod 644 {} +
	fb_deep_log "prepared batch D fixture: poisoned overlap segment $poison_seg"
}

fb_deep_refresh_archive_fixture() {
	local datadir
	local wal_dir
	local seg
	local latest_segments_sql

	if [[ "$FB_DEEP_ARCHIVE_DIR" == "$FB_DEEP_ARCHIVE_CLEAN_DIR" ]]; then
		mkdir -p "$FB_DEEP_ARCHIVE_DIR"
		fb_deep_log "using live archive directory for fixture: $FB_DEEP_ARCHIVE_DIR"
		return 0
	fi

	datadir="$(fb_deep_as_18pg_raw "PGPORT='$FB_DEEP_PGPORT' '$FB_DEEP_PSQL' -d '$FB_DEEP_DBNAME' -Atqc \"select current_setting('data_directory');\"")"
	wal_dir="$datadir/pg_wal"

	rm -rf "$FB_DEEP_ARCHIVE_DIR"
	mkdir -p "$FB_DEEP_ARCHIVE_DIR"

	latest_segments_sql="select name from (select name from pg_ls_waldir() where name ~ '^[0-9A-F]{24}(\\\\.partial)?$' order by name desc limit ${FB_DEEP_FIXTURE_SEGMENTS}) s order by name;"

	while IFS= read -r seg; do
		[[ -n "$seg" ]] || continue
		cp "$wal_dir/$seg" "$FB_DEEP_ARCHIVE_DIR/$seg"
	done < <(fb_deep_as_18pg_raw "PGPORT='$FB_DEEP_PGPORT' '$FB_DEEP_PSQL' -d '$FB_DEEP_DBNAME' -Atqc \"$latest_segments_sql\"")

	chmod 755 "$FB_DEEP_ARCHIVE_DIR"
	find "$FB_DEEP_ARCHIVE_DIR" -maxdepth 1 -type f -exec chmod 644 {} +
	fb_deep_log "refreshed archive fixture with latest $FB_DEEP_FIXTURE_SEGMENTS segments"
}
