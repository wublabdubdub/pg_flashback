#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
COMMON_SH="$REPO_ROOT/tests/release_gate/bin/common.sh"
RUN_SH="$REPO_ROOT/tests/release_gate/bin/run_release_gate.sh"
PREPARE_SH="$REPO_ROOT/tests/release_gate/bin/prepare_empty_instance.sh"
SIM_SH="$REPO_ROOT/tests/release_gate/bin/start_alldbsim.sh"
LOAD_SH="$REPO_ROOT/tests/release_gate/bin/load_alldb_seed.sh"
PRESSURE_SH="$REPO_ROOT/tests/release_gate/bin/run_alldb_dml_pressure.sh"
GROW_SH="$REPO_ROOT/tests/release_gate/bin/grow_flashback_target.sh"
SNAPSHOT_SH="$REPO_ROOT/tests/release_gate/bin/capture_truth_snapshots.sh"
MATRIX_SH="$REPO_ROOT/tests/release_gate/bin/run_flashback_matrix.sh"
EVAL_SH="$REPO_ROOT/tests/release_gate/bin/evaluate_gate.sh"
REPORT_SH="$REPO_ROOT/tests/release_gate/bin/render_report.sh"
CONFIG_DIR="$REPO_ROOT/tests/release_gate/config"
TEMPLATE_DIR="$REPO_ROOT/tests/release_gate/templates"
GOLDEN_DIR="$REPO_ROOT/tests/release_gate/golden"
SQL_DIR="$REPO_ROOT/tests/release_gate/sql"

fail() {
	echo "[release_gate:selftest] $*" >&2
	exit 1
}

assert_file_contains() {
	local file="$1"
	local pattern="$2"

	if ! grep -Fq "$pattern" "$file"; then
		fail "expected pattern '$pattern' in $file"
	fi
}

assert_file_matches() {
	local file="$1"
	local pattern="$2"

	if ! grep -Eq "$pattern" "$file"; then
		fail "expected regex '$pattern' in $file"
	fi
}

[[ -f "$COMMON_SH" ]] || fail "missing common helper: $COMMON_SH"
[[ -f "$RUN_SH" ]] || fail "missing runner: $RUN_SH"
[[ -f "$PREPARE_SH" ]] || fail "missing prepare script: $PREPARE_SH"
[[ -f "$SIM_SH" ]] || fail "missing simulator wrapper: $SIM_SH"
[[ -f "$LOAD_SH" ]] || fail "missing seed loader: $LOAD_SH"
[[ -f "$PRESSURE_SH" ]] || fail "missing pressure runner: $PRESSURE_SH"
[[ -f "$GROW_SH" ]] || fail "missing grow-target runner: $GROW_SH"
[[ -f "$SNAPSHOT_SH" ]] || fail "missing snapshot runner: $SNAPSHOT_SH"
[[ -f "$MATRIX_SH" ]] || fail "missing flashback matrix runner: $MATRIX_SH"
[[ -f "$EVAL_SH" ]] || fail "missing gate evaluator: $EVAL_SH"
[[ -f "$REPORT_SH" ]] || fail "missing report renderer: $REPORT_SH"
[[ -x "$RUN_SH" ]] || fail "runner is not executable: $RUN_SH"
[[ -x "$PREPARE_SH" ]] || fail "prepare script is not executable: $PREPARE_SH"
[[ -x "$SIM_SH" ]] || fail "simulator wrapper is not executable: $SIM_SH"
[[ -x "$LOAD_SH" ]] || fail "seed loader is not executable: $LOAD_SH"
[[ -x "$PRESSURE_SH" ]] || fail "pressure runner is not executable: $PRESSURE_SH"
[[ -x "$GROW_SH" ]] || fail "grow-target runner is not executable: $GROW_SH"
[[ -x "$SNAPSHOT_SH" ]] || fail "snapshot runner is not executable: $SNAPSHOT_SH"
[[ -x "$MATRIX_SH" ]] || fail "flashback matrix runner is not executable: $MATRIX_SH"
[[ -x "$EVAL_SH" ]] || fail "gate evaluator is not executable: $EVAL_SH"
[[ -x "$REPORT_SH" ]] || fail "report renderer is not executable: $REPORT_SH"

bash -n "$COMMON_SH" "$RUN_SH" "$PREPARE_SH" "$SIM_SH" "$LOAD_SH" "$PRESSURE_SH" "$GROW_SH" \
	"$SNAPSHOT_SH" "$MATRIX_SH" "$EVAL_SH" "$REPORT_SH"

# shellcheck disable=SC1090
source "$COMMON_SH"

for major in 14 15 16 17 18; do
	expected="/walstorage/${major}waldata"
	actual="$(fb_release_gate_archive_dir_from_major "$major")"
	[[ "$actual" == "$expected" ]] || fail "archive dir mismatch for PG${major}: $actual"
done

archive_cmd='cp %p /home/18pg/wal_arch/%f'
fb_release_gate_archive_command_matches_dir "$archive_cmd" "/walstorage/18waldata" || \
	fail "symlink-equivalent archive_command should match /walstorage/18waldata"

archive_cmd='cp %p /tmp/not-release-gate/%f'
if fb_release_gate_archive_command_matches_dir "$archive_cmd" "/walstorage/18waldata"; then
	fail "wrong archive target should not match /walstorage/18waldata"
fi

[[ -f "$CONFIG_DIR/release_gate.conf" ]] || fail "missing release_gate.conf"
[[ -f "$CONFIG_DIR/scenario_matrix.json" ]] || fail "missing scenario_matrix.json"
[[ -f "$CONFIG_DIR/thresholds.json" ]] || fail "missing thresholds.json"
[[ -f "$TEMPLATE_DIR/report.md.tpl" ]] || fail "missing report template"
[[ -f "$SQL_DIR/recreate_alldb.sql" ]] || fail "missing recreate_alldb.sql"
[[ -f "$SQL_DIR/table_size_summary.sql" ]] || fail "missing table_size_summary.sql"
[[ -f "$SQL_DIR/export_table_csv.sql" ]] || fail "missing export_table_csv.sql"
[[ -f "$SQL_DIR/export_flashback_csv.sql" ]] || fail "missing export_flashback_csv.sql"
[[ -f "$SQL_DIR/create_flashback_ctas.sql" ]] || fail "missing create_flashback_ctas.sql"
[[ -f "$SQL_DIR/drop_flashback_ctas.sql" ]] || fail "missing drop_flashback_ctas.sql"

jq empty \
	"$CONFIG_DIR/scenario_matrix.json" \
	"$CONFIG_DIR/thresholds.json" \
	"$GOLDEN_DIR/pg14.json" \
	"$GOLDEN_DIR/pg15.json" \
	"$GOLDEN_DIR/pg16.json" \
	"$GOLDEN_DIR/pg17.json" \
	"$GOLDEN_DIR/pg18.json" >/dev/null

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

render_output_dir="$tmp_dir/output"
mkdir -p "$render_output_dir/json" "$render_output_dir/reports"

cat > "$render_output_dir/json/gate_evaluation.json" <<'EOF'
{
  "pg_major": "18",
  "verdict": "INCOMPLETE",
  "golden_file": "/tmp/fake-golden.json",
  "summary": {
    "total": 3,
    "correctness_passed": 2,
    "correctness_failures": 1,
    "correctness_infra_failures": 0,
    "performance_failures": 0,
    "performance_regressions": 0,
    "performance_infra_failures": 0,
    "performance_skipped": 1,
    "performance_missing_baseline": 2
  },
  "truth_summary": {
    "entries": 2,
    "target_snapshot_entries": 1
  },
  "golden_summary": {
    "entries": 0
  },
  "results": [
    {
      "scenario_id": "random_flashback_1",
      "truth_scenario_id": "random_flashback_1",
      "schema_name": "scenario_oa_50t_50000r",
      "table_name": "users",
      "target_ts": "2026-04-08 00:38:25.357868+00",
      "path_kind": "query",
      "baseline_key": "query:random_flashback_1:scenario_oa_50t_50000r.users",
      "correctness_status": "fail",
      "correctness_reason": "row_count or sha256 mismatch",
      "performance_status": "skipped",
      "performance_reason": "correctness failed",
      "diff_path": "/tmp/diff_users.diff",
      "result_sha256": "result_users",
      "truth_sha256": "truth_users",
      "result_row_count": 50015,
      "truth_row_count": 50015,
      "gate_elapsed_ms": 18380,
      "baseline_elapsed_ms": 0,
      "ratio_threshold": 0.2,
      "absolute_threshold_ms": 30000
    },
    {
      "scenario_id": "random_flashback_1",
      "truth_scenario_id": "random_flashback_1",
      "schema_name": "scenario_oa_50t_50000r",
      "table_name": "documents",
      "target_ts": "2026-04-08 00:38:25.357868+00",
      "path_kind": "query",
      "baseline_key": "query:random_flashback_1:scenario_oa_50t_50000r.documents",
      "correctness_status": "pass",
      "correctness_reason": "",
      "performance_status": "missing_baseline",
      "performance_reason": "missing golden baseline",
      "diff_path": "",
      "result_sha256": "result_documents",
      "truth_sha256": "truth_documents",
      "result_row_count": 1949993,
      "truth_row_count": 1949993,
      "gate_elapsed_ms": 461652,
      "baseline_elapsed_ms": 0,
      "ratio_threshold": 0.2,
      "absolute_threshold_ms": 30000
    },
    {
      "scenario_id": "copy_to_flashback",
      "truth_scenario_id": "random_flashback_1",
      "schema_name": "scenario_oa_50t_50000r",
      "table_name": "documents",
      "target_ts": "2026-04-08 00:38:25.357868+00",
      "path_kind": "copy",
      "baseline_key": "copy:copy_to_flashback:scenario_oa_50t_50000r.documents",
      "correctness_status": "pass",
      "correctness_reason": "",
      "performance_status": "missing_baseline",
      "performance_reason": "missing golden baseline",
      "diff_path": "",
      "result_sha256": "result_copy",
      "truth_sha256": "truth_documents",
      "result_row_count": 1949993,
      "truth_row_count": 1949993,
      "gate_elapsed_ms": 329027,
      "baseline_elapsed_ms": 0,
      "ratio_threshold": 0.2,
      "absolute_threshold_ms": 30000
    }
  ]
}
EOF

cat > "$render_output_dir/json/environment.json" <<'EOF'
{
  "archive_dir": "/walstorage/18waldata",
  "archive_mode": "on",
  "archive_command": "cp %p /walstorage/18waldata/%f"
}
EOF

cat > "$render_output_dir/json/truth_manifest.json" <<'EOF'
[
  {
    "scenario_id": "random_flashback_1",
    "target_ts": "2026-04-08 00:38:25.357868+00",
    "target_snapshot": "100:101:",
    "schema_name": "scenario_oa_50t_50000r",
    "table_name": "users",
    "csv_path": "/tmp/truth_users.csv",
    "sha256": "truth_users",
    "row_count": 50015,
    "table_class": "medium",
    "capture_mode": "random"
  },
  {
    "scenario_id": "random_flashback_1",
    "target_ts": "2026-04-08 00:38:25.357868+00",
    "schema_name": "scenario_oa_50t_50000r",
    "table_name": "documents",
    "csv_path": "/tmp/truth_documents.csv",
    "sha256": "truth_documents",
    "row_count": 1949993,
    "table_class": "large_5gb_target",
    "capture_mode": "random"
  }
]
EOF

cat > "$render_output_dir/json/random_snapshot_schedule.json" <<'EOF'
[
  {
    "scenario_id": "random_flashback_1",
    "scheduled_ts": "2026-04-08 00:38:24+00",
    "offset_sec": 419
  }
]
EOF

list_log="$tmp_dir/list_stages.log"
FB_RELEASE_GATE_OUTPUT_DIR="$render_output_dir" \
FB_RELEASE_GATE_PSQL="/bin/true" \
bash "$RUN_SH" --list-stages >"$list_log" 2>&1 || fail "--list-stages should succeed"
assert_file_contains "$list_log" "prepare_instance"
assert_file_contains "$list_log" "render_gate_report"
grow_line="$(grep -n '^grow_target_table' "$list_log" | cut -d: -f1)"
pressure_line="$(grep -n '^start_dml_pressure' "$list_log" | cut -d: -f1)"
[[ -n "$grow_line" && -n "$pressure_line" ]] || fail "missing grow_target_table/start_dml_pressure in stage list"
(( grow_line < pressure_line )) || fail "grow_target_table should appear before start_dml_pressure"

only_log="$tmp_dir/only_render.log"
FB_RELEASE_GATE_OUTPUT_DIR="$render_output_dir" \
FB_RELEASE_GATE_PSQL="/bin/true" \
bash "$RUN_SH" --only render_gate_report >"$only_log" 2>&1 || fail "--only render_gate_report should succeed"
[[ -f "$render_output_dir/reports/release_gate_report.md" ]] || fail "missing rendered report for --only render_gate_report"
assert_file_contains "$render_output_dir/reports/release_gate_report.md" "# Release Gate 最终报告"
assert_file_contains "$render_output_dir/reports/release_gate_report.md" "## 一页结论"
assert_file_contains "$render_output_dir/reports/release_gate_report.md" "## 测试过程"
assert_file_contains "$render_output_dir/reports/release_gate_report.md" "## 正确性失败明细"
assert_file_contains "$render_output_dir/reports/release_gate_report.md" "target_snapshot 覆盖率"
assert_file_contains "$render_output_dir/reports/release_gate_report.md" "缺少 golden baseline"
assert_file_contains "$only_log" "render_gate_report"
assert_file_matches "$only_log" '^\[release_gate\]\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}( [+-][0-9]{4})?\]'

from_to_log="$tmp_dir/from_to_render.log"
rm -f "$render_output_dir/reports/release_gate_report.md"
FB_RELEASE_GATE_OUTPUT_DIR="$render_output_dir" \
FB_RELEASE_GATE_PSQL="/bin/true" \
bash "$RUN_SH" --from render_gate_report --to render_gate_report >"$from_to_log" 2>&1 || fail "--from/--to render_gate_report should succeed"
[[ -f "$render_output_dir/reports/release_gate_report.md" ]] || fail "missing rendered report for --from/--to render_gate_report"
assert_file_contains "$from_to_log" "render_gate_report"

fake_bin_dir="$tmp_dir/fakebin"
fake_archive_root="$tmp_dir/walstorage"
fake_archive_dir="$fake_archive_root/18waldata"
mkdir -p "$fake_bin_dir" "$fake_archive_dir"
cat > "$fake_bin_dir/psql" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' 180000
EOF
chmod +x "$fake_bin_dir/psql"
touch "$fake_archive_dir/keep.seg"

mid_stage_log="$tmp_dir/mid_stage_render.log"
FB_RELEASE_GATE_OUTPUT_DIR="$render_output_dir" \
FB_RELEASE_GATE_ARCHIVE_ROOT="$fake_archive_root" \
FB_RELEASE_GATE_PSQL="$fake_bin_dir/psql" \
bash "$RUN_SH" --only render_gate_report >"$mid_stage_log" 2>&1 || fail "mid-stage render_gate_report should succeed"
[[ -f "$fake_archive_dir/keep.seg" ]] || fail "mid-stage run should not delete existing archive files"

snapshot_output_dir="$tmp_dir/snapshot_output"
mkdir -p "$snapshot_output_dir/logs" "$snapshot_output_dir/json"
printf '%s\n' 'STALE_RANDOM_LOG' > "$snapshot_output_dir/logs/random_snapshot_capture.log"
cat > "$snapshot_output_dir/json/dml_pressure_runtime.json" <<'EOF'
{
  "pressure_start_ts": "2026-04-08 00:31:25+00",
  "duration_sec": 3600,
  "job_id": "dry-run-job"
}
EOF
FB_RELEASE_GATE_OUTPUT_DIR="$snapshot_output_dir" \
bash "$SNAPSHOT_SH" --dry-run --mode random >"$tmp_dir/capture_random_dry.log" 2>&1 || fail "capture_truth_snapshots --dry-run --mode random should succeed"
if grep -Fq 'STALE_RANDOM_LOG' "$snapshot_output_dir/logs/random_snapshot_capture.log"; then
	fail "capture_truth_snapshots should reset random_snapshot_capture.log before each run"
fi

prepare_test_repo="$tmp_dir/repo"
prepare_test_root="$prepare_test_repo/tests/release_gate"
mkdir -p "$prepare_test_root"
cp -R "$REPO_ROOT/tests/release_gate/bin" "$prepare_test_root/bin"
cp -R "$REPO_ROOT/tests/release_gate/sql" "$prepare_test_root/sql"
cp -R "$REPO_ROOT/tests/release_gate/config" "$prepare_test_root/config"
cp -R "$REPO_ROOT/tests/release_gate/templates" "$prepare_test_root/templates"
cat > "$prepare_test_root/config/release_gate.conf" <<EOF
FB_RELEASE_GATE_DBNAME=alldb
FB_RELEASE_GATE_ARCHIVE_ROOT=$tmp_dir/prepare_walstorage
FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB=100
EOF

prepare_test_sh="$prepare_test_root/bin/prepare_empty_instance.sh"
prepare_fake_bin_dir="$tmp_dir/prepare_fakebin"
prepare_log="$tmp_dir/prepare_calls.log"
prepare_archive_root="$tmp_dir/prepare_walstorage"
prepare_archive_dir="$prepare_archive_root/18waldata"
mkdir -p "$prepare_fake_bin_dir" "$prepare_archive_dir"

cat > "$prepare_fake_bin_dir/psql" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf 'psql %s\n' "\$*" >> "$prepare_log"
if [[ "\$*" == *"show server_version_num;"* ]]; then
	printf '%s\n' 180000
	exit 0
fi
if [[ "\$*" == *"check_environment.sql"* ]]; then
	printf '%s\n' "archive_mode=on"
	printf '%s\n' "archive_command=cp %p $prepare_archive_dir/%f"
	exit 0
fi
if [[ "\$*" == *"list_large_databases.sql"* ]]; then
	exit 0
fi
exit 0
EOF
chmod +x "$prepare_fake_bin_dir/psql"

cat > "$prepare_fake_bin_dir/createdb" <<EOF
#!/usr/bin/env bash
printf 'createdb %s\n' "\$*" >> "$prepare_log"
EOF
chmod +x "$prepare_fake_bin_dir/createdb"

cat > "$prepare_fake_bin_dir/dropdb" <<EOF
#!/usr/bin/env bash
printf 'dropdb %s\n' "\$*" >> "$prepare_log"
EOF
chmod +x "$prepare_fake_bin_dir/dropdb"

prepare_output_dir="$tmp_dir/prepare_output"
mkdir -p "$prepare_output_dir"
FB_RELEASE_GATE_OUTPUT_DIR="$prepare_output_dir" \
FB_RELEASE_GATE_PSQL="$prepare_fake_bin_dir/psql" \
FB_RELEASE_GATE_CREATEDB="$prepare_fake_bin_dir/createdb" \
FB_RELEASE_GATE_DROPDB="$prepare_fake_bin_dir/dropdb" \
FB_RELEASE_GATE_OS_USER="$(id -un)" \
bash "$prepare_test_sh" >"$tmp_dir/prepare.log" 2>&1 || fail "prepare_empty_instance should succeed with fake commands"
assert_file_contains "$prepare_log" "createdb -p 5832 -U 18pg alldb"
assert_file_contains "$prepare_log" "psql -X -v ON_ERROR_STOP=1 -p 5832 -U 18pg -d alldb -Atqc CREATE EXTENSION IF NOT EXISTS pg_flashback;"

cleanup_test_repo="$tmp_dir/cleanup_repo"
cleanup_test_root="$cleanup_test_repo/tests/release_gate"
mkdir -p "$cleanup_test_root"
cp -R "$REPO_ROOT/tests/release_gate/bin" "$cleanup_test_root/bin"
cp -R "$REPO_ROOT/tests/release_gate/sql" "$cleanup_test_root/sql"
cp -R "$REPO_ROOT/tests/release_gate/config" "$cleanup_test_root/config"
cp -R "$REPO_ROOT/tests/release_gate/templates" "$cleanup_test_root/templates"
cat > "$cleanup_test_root/config/release_gate.conf" <<EOF
FB_RELEASE_GATE_DBNAME=alldb
FB_RELEASE_GATE_ARCHIVE_ROOT=$tmp_dir/cleanup_walstorage
FB_RELEASE_GATE_LARGE_DB_THRESHOLD_MB=100
EOF

cleanup_run_sh="$cleanup_test_root/bin/run_release_gate.sh"
cleanup_prepare_sh="$cleanup_test_root/bin/prepare_empty_instance.sh"
cleanup_fake_bin_dir="$tmp_dir/cleanup_fakebin"
cleanup_log="$tmp_dir/cleanup_calls.log"
cleanup_archive_root="$tmp_dir/cleanup_walstorage"
cleanup_archive_dir="$cleanup_archive_root/18waldata"
cleanup_marker="$cleanup_archive_dir/post_prepare.seg"
mkdir -p "$cleanup_fake_bin_dir" "$cleanup_archive_dir"

cat > "$cleanup_fake_bin_dir/psql" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf 'psql %s\n' "\$*" >> "$cleanup_log"
if [[ "\$*" == *"show server_version_num;"* ]]; then
	printf '%s\n' 180000
	exit 0
fi
if [[ "\$*" == *"check_environment.sql"* ]]; then
	printf '%s\n' "archive_mode=on"
	printf '%s\n' "archive_command=cp %p $cleanup_archive_dir/%f"
	exit 0
fi
if [[ "\$*" == *"list_large_databases.sql"* ]]; then
	exit 0
fi
exit 0
EOF
chmod +x "$cleanup_fake_bin_dir/psql"

cat > "$cleanup_fake_bin_dir/createdb" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf 'createdb %s\n' "\$*" >> "$cleanup_log"
mkdir -p "$cleanup_archive_dir"
touch "$cleanup_marker"
EOF
chmod +x "$cleanup_fake_bin_dir/createdb"

cat > "$cleanup_fake_bin_dir/dropdb" <<EOF
#!/usr/bin/env bash
printf 'dropdb %s\n' "\$*" >> "$cleanup_log"
EOF
chmod +x "$cleanup_fake_bin_dir/dropdb"

cat > "$cleanup_test_root/bin/start_alldbsim.sh" <<EOF
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$cleanup_test_root/bin/start_alldbsim.sh"

cleanup_output_dir="$tmp_dir/cleanup_output"
mkdir -p "$cleanup_output_dir"
FB_RELEASE_GATE_OUTPUT_DIR="$cleanup_output_dir" \
FB_RELEASE_GATE_PSQL="$cleanup_fake_bin_dir/psql" \
FB_RELEASE_GATE_CREATEDB="$cleanup_fake_bin_dir/createdb" \
FB_RELEASE_GATE_DROPDB="$cleanup_fake_bin_dir/dropdb" \
FB_RELEASE_GATE_OS_USER="$(id -un)" \
bash "$cleanup_run_sh" --only prepare_instance >"$tmp_dir/cleanup_prepare.log" 2>&1 || fail "run_release_gate --only prepare_instance should succeed with fake commands"
[[ -f "$cleanup_marker" ]] || fail "archive files created after prepare_instance should be preserved on exit"

matrix_test_root="$tmp_dir/matrix_repo/tests/release_gate"
mkdir -p "$matrix_test_root"
cp -R "$REPO_ROOT/tests/release_gate/bin" "$matrix_test_root/bin"
cp -R "$REPO_ROOT/tests/release_gate/sql" "$matrix_test_root/sql"
cp -R "$REPO_ROOT/tests/release_gate/config" "$matrix_test_root/config"
cp -R "$REPO_ROOT/tests/release_gate/templates" "$matrix_test_root/templates"

matrix_output_dir="$tmp_dir/matrix_output"
matrix_log="$tmp_dir/matrix_psql.log"
matrix_archive_root="$tmp_dir/matrix_walstorage"
matrix_archive_dir="$matrix_archive_root/18waldata"
matrix_pgwal_dir="$tmp_dir/matrix_pgwal"
mkdir -p "$matrix_output_dir/json" "$matrix_archive_dir" "$matrix_pgwal_dir"
touch \
	"$matrix_archive_dir/000000010000000A00000003" \
	"$matrix_pgwal_dir/000000010000000A00000004"
cat > "$matrix_output_dir/json/truth_manifest.json" <<'EOF'
[
  {
    "scenario_id": "random_flashback_1",
    "target_ts": "2026-04-07 03:17:56.806265+00",
    "schema_name": "scenario_oa_50t_50000r",
    "table_name": "documents",
    "qualified_name": "scenario_oa_50t_50000r.documents",
    "csv_path": "/tmp/fake.csv",
    "sha256": "7cde7fb64fd82bd152710cf238e017b9ab46c0592483edc067ba4f6c75fac108",
    "row_count": 1,
    "table_class": "large_5gb_target",
    "capture_mode": "all"
  }
]
EOF

matrix_fake_bin_dir="$tmp_dir/matrix_fakebin"
mkdir -p "$matrix_fake_bin_dir"
cat > "$matrix_fake_bin_dir/psql" <<EOF
#!/usr/bin/env bash
set -euo pipefail
printf 'PGOPTIONS=%s psql %s\n' "\${PGOPTIONS-}" "\$*" >> "$matrix_log"
if [[ "\$*" == *"show server_version_num;"* ]]; then
	printf '%s\n' 180000
	exit 0
fi
if [[ "\$*" == *"string_agg(format('%I', a.attname), ', ' order by key_ord.ord)"* ]]; then
	printf '%s\n' 'id'
	exit 0
fi
if [[ "\$*" == *"pg_flashback("* || "\$*" == *"create_flashback_ctas.sql"* ]]; then
	if [[ "\${PGOPTIONS-}" != "-c pg_flashback.memory_limit=6GB" ]]; then
		cat >&2 <<'ERR'
ERROR:  estimated flashback working set exceeds pg_flashback.memory_limit
DETAIL:  estimated=1946827608 bytes limit=1073741824 bytes (1 GB) mode=auto phase=preflight
HINT:  Increase pg_flashback.memory_limit, or set pg_flashback.spill_mode = 'disk' to allow spill.
ERR
		exit 1
	fi
fi
printf 'id\n1\n'
EOF
chmod +x "$matrix_fake_bin_dir/psql"

FB_RELEASE_GATE_OUTPUT_DIR="$matrix_output_dir" \
FB_RELEASE_GATE_ARCHIVE_ROOT="$matrix_archive_root" \
FB_RELEASE_GATE_PSQL="$matrix_fake_bin_dir/psql" \
FB_RELEASE_GATE_OS_USER="$(id -un)" \
bash "$matrix_test_root/bin/run_flashback_matrix.sh" >"$tmp_dir/matrix_run.log" 2>&1 || \
	fail "run_flashback_matrix should succeed with fake psql"

if grep -Fq 'set pg_flashback.archive_dest =' "$matrix_log"; then
	fail "run_flashback_matrix should not override archive_dest"
fi
if grep -Fq 'pg_switch_wal()' "$matrix_log"; then
	fail "run_flashback_matrix should not seal WAL tail"
fi
if grep -Fq 'archive_dest=' "$matrix_log"; then
	fail "CTAS path should not pass archive_dest variable"
fi
if ! grep -Fq "PGOPTIONS=-c pg_flashback.memory_limit=6GB" "$matrix_log"; then
	fail "run_flashback_matrix should default every flashback command to 6GB"
fi
if ! grep -Fq "flashback sql [random_flashback_1:documents:query:run1]" "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should log query flashback SQL"
fi
if ! grep -Fq "flashback sql [copy_to_flashback:documents:copy:run1]" "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should log copy flashback SQL"
fi
if ! grep -Fq "flashback sql [ctas_flashback:documents:ctas-create:run1]" "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should log CTAS flashback SQL"
fi
if ! grep -Fq "accuracy [random_flashback_1:documents:query] pass" "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should compare query accuracy immediately after CSV output"
fi
if ! grep -Fq "accuracy [copy_to_flashback:documents:copy] pass" "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should compare copy accuracy immediately after CSV output"
fi
if ! grep -Fq "accuracy [ctas_flashback:documents:ctas] pass" "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should compare CTAS accuracy immediately after CSV output"
fi
if grep -Fq ':run2]' "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should default each flashback case to a single execution"
fi
if grep -Fq ':run3]' "$tmp_dir/matrix_run.log"; then
	fail "run_flashback_matrix should not emit a third execution for the same flashback case"
fi

echo "[release_gate:selftest] PASS"
