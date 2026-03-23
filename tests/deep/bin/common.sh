#!/usr/bin/env bash
set -euo pipefail

FB_DEEP_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
FB_DEEP_SQL_DIR="$FB_DEEP_REPO_ROOT/tests/deep/sql"
FB_DEEP_OUTPUT_ROOT="$FB_DEEP_REPO_ROOT/tests/deep/output"

FB_DEEP_PSQL="${FB_DEEP_PSQL:-/home/18pg/local/bin/psql}"
FB_DEEP_CREATEDB="${FB_DEEP_CREATEDB:-/home/18pg/local/bin/createdb}"
FB_DEEP_DROPDB="${FB_DEEP_DROPDB:-/home/18pg/local/bin/dropdb}"
FB_DEEP_PGPORT="${FB_DEEP_PGPORT:-5832}"
FB_DEEP_DBNAME="${FB_DEEP_DBNAME:-fb_deep_test}"

FB_DEEP_ROOT_DIR="${FB_DEEP_ROOT_DIR:-/tmp/pg_flashback_deep}"
FB_DEEP_ARCHIVE_DIR="${FB_DEEP_ARCHIVE_DIR:-$FB_DEEP_ROOT_DIR/archive}"
FB_DEEP_CKWAL_DIR="${FB_DEEP_CKWAL_DIR:-$FB_DEEP_ROOT_DIR/ckwal}"
FB_DEEP_FAKE_PGWAL_DIR="${FB_DEEP_FAKE_PGWAL_DIR:-$FB_DEEP_ROOT_DIR/fake_pg_wal}"

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

fb_deep_apply_mode() {
	fb_deep_require_mode

	if [[ "$FB_DEEP_MODE" == "full" ]]; then
		FB_DEEP_ROW_COUNT=5000000
		FB_DEEP_BAG_DISTINCT=250000
		FB_DEEP_INSERT_COUNT=200000
		FB_DEEP_STRESS_ROUNDS=12
		FB_DEEP_STRESS_INSERT_COUNT=100000
		FB_DEEP_FIXTURE_SEGMENTS=160
		FB_DEEP_MEMORY_LIMIT_KB=4194304
	else
		FB_DEEP_ROW_COUNT=50000
		FB_DEEP_BAG_DISTINCT=2500
		FB_DEEP_INSERT_COUNT=3000
		FB_DEEP_STRESS_ROUNDS=4
		FB_DEEP_STRESS_INSERT_COUNT=4000
		FB_DEEP_FIXTURE_SEGMENTS=48
		FB_DEEP_MEMORY_LIMIT_KB=1048576
	fi

	export FB_DEEP_ROW_COUNT
	export FB_DEEP_BAG_DISTINCT
	export FB_DEEP_INSERT_COUNT
	export FB_DEEP_STRESS_ROUNDS
	export FB_DEEP_STRESS_INSERT_COUNT
	export FB_DEEP_FIXTURE_SEGMENTS
	export FB_DEEP_MEMORY_LIMIT_KB
}

fb_deep_output_dir() {
	local name="$1"
	printf '%s/%s/%s\n' "$FB_DEEP_OUTPUT_ROOT" "$FB_DEEP_MODE" "$name"
}

fb_deep_as_18pg() {
	local cmd="$1"
	su - 18pg -c "$cmd"
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
	if [[ "$(su - 18pg -c "$cmd")" == "1" ]]; then
		return 0
	fi
	return 1
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
export FB_DEEP_MEMORY_LIMIT_KB='$FB_DEEP_MEMORY_LIMIT_KB'
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

	datadir="$(fb_deep_as_18pg "PGPORT='$FB_DEEP_PGPORT' '$FB_DEEP_PSQL' -d '$FB_DEEP_DBNAME' -Atqc \"select current_setting('data_directory');\"")"
	wal_dir="$datadir/pg_wal"

	rm -rf "$FB_DEEP_ARCHIVE_DIR"
	mkdir -p "$FB_DEEP_ARCHIVE_DIR"

	latest_segments_sql="select name from (select name from pg_ls_waldir() where name ~ '^[0-9A-F]{24}(\\\\.partial)?$' order by name desc limit ${FB_DEEP_FIXTURE_SEGMENTS}) s order by name;"

	while IFS= read -r seg; do
		[[ -n "$seg" ]] || continue
		cp "$wal_dir/$seg" "$FB_DEEP_ARCHIVE_DIR/$seg"
	done < <(fb_deep_as_18pg "PGPORT='$FB_DEEP_PGPORT' '$FB_DEEP_PSQL' -d '$FB_DEEP_DBNAME' -Atqc \"$latest_segments_sql\"")

	chmod 755 "$FB_DEEP_ARCHIVE_DIR"
	find "$FB_DEEP_ARCHIVE_DIR" -maxdepth 1 -type f -exec chmod 644 {} +
	fb_deep_log "refreshed archive fixture with latest $FB_DEEP_FIXTURE_SEGMENTS segments"
}
