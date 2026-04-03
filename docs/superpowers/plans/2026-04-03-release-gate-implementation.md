# Release Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable `PG14-18` release gate that provisions `alldb`, drives `/root/alldbsimulator`, captures truth snapshots, validates `pg_flashback` correctness, compares performance against golden baselines, and emits a Markdown report.

**Architecture:** Add an isolated `tests/release_gate/` harness that follows the existing `tests/deep/bin` shell style but targets release blocking instead of deep pilot experiments. Split the harness into environment/archive guards, `alldbsimulator` service orchestration, snapshot/export SQL helpers, flashback scenario execution, baseline evaluation, and report rendering so each phase can be rerun independently and preserves artifacts.

**Tech Stack:** Bash, `psql`, PostgreSQL catalog SQL, existing `tests/deep/bin` shell helpers, `/root/alldbsimulator` HTTP API, JSON config files, Markdown templates

---

## File Map

### New directories

- `tests/release_gate/`
  - Release-gate harness root
- `tests/release_gate/bin/`
  - Shell entrypoints and orchestrators
- `tests/release_gate/sql/`
  - SQL files used by the shell harness
- `tests/release_gate/config/`
  - Release-gate config, thresholds, scenario definitions
- `tests/release_gate/golden/`
  - Per-version golden baseline JSON files
- `tests/release_gate/output/`
  - Checked-in placeholder and output README only
- `tests/release_gate/templates/`
  - Markdown report template and JSON skeletons

### New files

- `tests/release_gate/README.md`
  - Operator and agent usage guide
- `tests/release_gate/bin/common.sh`
  - Shared shell helpers, logging, PG version detection, archive-dir resolution, artifact paths
- `tests/release_gate/bin/run_release_gate.sh`
  - Main orchestrator
- `tests/release_gate/bin/prepare_empty_instance.sh`
  - Empty-instance checks, oversized DB deletion, `alldb` recreation, archive cleanup
- `tests/release_gate/bin/start_alldbsim.sh`
  - Launch `/root/alldbsimulator/bin/alldbsim`, wait for health, stop it on exit
- `tests/release_gate/bin/load_alldb_seed.sh`
  - Call `alldbsimulator` generate API to create `50 x 100MB`
- `tests/release_gate/bin/run_alldb_dml_pressure.sh`
  - Call `alldbsimulator` DML API for the 1-hour pressure run
- `tests/release_gate/bin/grow_flashback_target.sh`
  - Extend the configured target table to `5GB`
- `tests/release_gate/bin/capture_truth_snapshots.sh`
  - Choose reproducible random timestamps and export truth snapshots
- `tests/release_gate/bin/run_flashback_matrix.sh`
  - Execute query, `COPY TO`, `CTAS`, and DML-specific flashback scenarios
- `tests/release_gate/bin/evaluate_gate.sh`
  - Compare correctness outputs and measured runtimes against golden baselines
- `tests/release_gate/bin/render_report.sh`
  - Render the final Markdown report
- `tests/release_gate/sql/check_environment.sql`
  - Archive settings, DB sizes, current version, current extension info
- `tests/release_gate/sql/list_large_databases.sql`
  - Return DB names larger than the configured threshold
- `tests/release_gate/sql/recreate_alldb.sql`
  - Drop/create `alldb`
- `tests/release_gate/sql/table_size_summary.sql`
  - Validate `50 x 100MB` and target table `5GB`
- `tests/release_gate/sql/export_table_csv.sql`
  - Standardized table export for truth snapshots and CTAS/COPY verification
- `tests/release_gate/sql/export_flashback_csv.sql`
  - Standardized `SELECT * FROM pg_flashback(...)` export
- `tests/release_gate/sql/create_flashback_ctas.sql`
  - Deterministic CTAS wrapper
- `tests/release_gate/sql/drop_flashback_ctas.sql`
  - Cleanup helper for CTAS tables
- `tests/release_gate/config/release_gate.conf`
  - Main harness config: DB names, target table, archive root, simulator listen addr
- `tests/release_gate/config/scenario_matrix.json`
  - Scenario catalog and flashback workload definitions
- `tests/release_gate/config/thresholds.json`
  - Relative and absolute performance thresholds
- `tests/release_gate/golden/pg14.json`
- `tests/release_gate/golden/pg15.json`
- `tests/release_gate/golden/pg16.json`
- `tests/release_gate/golden/pg17.json`
- `tests/release_gate/golden/pg18.json`
  - Golden baselines for each supported release-gate version
- `tests/release_gate/templates/report.md.tpl`
  - Markdown report scaffold
- `tests/release_gate/output/.gitkeep`
- `tests/release_gate/output/README.md`
  - Artifact layout documentation

### Modified files

- `STATUS.md`
  - Track implementation progress as tasks land
- `TODO.md`
  - Track checklist completion
- `docs/architecture/overview.md`
  - Register the new release-gate harness responsibilities

---

### Task 1: Register the release-gate harness in project docs

**Files:**
- Modify: `docs/architecture/overview.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `tests/release_gate/README.md`

- [ ] **Step 1: Add architecture coverage for `tests/release_gate`**

Document the new release-gate harness as a separate validation surface from `tests/deep`, including its responsibility for release blocking, golden baselines, and Markdown reports.

- [ ] **Step 2: Add operator-facing README**

Write `tests/release_gate/README.md` with:
- required environment variables
- per-version archive directory rules
- expected `/root/alldbsimulator` dependency
- artifact layout and cleanup semantics

- [ ] **Step 3: Sync tracking docs**

Mark the release-gate implementation as in progress in `STATUS.md` and keep the `TODO.md` checklist aligned with the first batch of files that will be created.

- [ ] **Step 4: Verify doc references are internally consistent**

Run: `rg -n "release_gate|walstorage/.+waldata|golden baseline" docs tests/release_gate STATUS.md TODO.md`

Expected: only the new release-gate paths and wording appear; no stray `PG12/13` release-gate scope remains.

### Task 2: Scaffold the shared harness structure and config files

**Files:**
- Create: `tests/release_gate/bin/common.sh`
- Create: `tests/release_gate/bin/run_release_gate.sh`
- Create: `tests/release_gate/config/release_gate.conf`
- Create: `tests/release_gate/config/scenario_matrix.json`
- Create: `tests/release_gate/config/thresholds.json`
- Create: `tests/release_gate/templates/report.md.tpl`
- Create: `tests/release_gate/output/.gitkeep`
- Create: `tests/release_gate/output/README.md`

- [ ] **Step 1: Create the directory tree and placeholders**

Add the `tests/release_gate/{bin,sql,config,golden,templates,output}` layout with checked-in placeholders only where needed.

- [ ] **Step 2: Implement `common.sh`**

Follow the `tests/deep/bin/common.sh` style and add:
- strict shell settings
- repo-root discovery
- `psql`/`createdb`/`dropdb` command resolution
- current PG major detection
- archive-dir mapping to `/walstorage/{14,15,16,17,18}waldata`
- per-run output dir helpers
- JSON and CSV artifact path helpers

- [ ] **Step 3: Add config skeletons**

Create initial `release_gate.conf`, `scenario_matrix.json`, and `thresholds.json` with explicit defaults for:
- `alldb`
- archive root
- simulator listen addr
- target table name
- random seed
- scenario IDs
- ratio and absolute thresholds

- [ ] **Step 4: Add the top-level orchestrator skeleton**

Implement `run_release_gate.sh` with phase ordering and a cleanup `trap`, but leave phase bodies as calls into not-yet-written scripts.

- [ ] **Step 5: Run syntax and JSON validation**

Run:

```bash
bash -n tests/release_gate/bin/common.sh tests/release_gate/bin/run_release_gate.sh
jq empty tests/release_gate/config/scenario_matrix.json tests/release_gate/config/thresholds.json
```

Expected: all checks pass with no syntax errors.

### Task 3: Build empty-instance preparation and per-version archive guards

**Files:**
- Create: `tests/release_gate/bin/prepare_empty_instance.sh`
- Create: `tests/release_gate/sql/check_environment.sql`
- Create: `tests/release_gate/sql/list_large_databases.sql`
- Create: `tests/release_gate/sql/recreate_alldb.sql`
- Modify: `tests/release_gate/bin/common.sh`
- Modify: `tests/release_gate/bin/run_release_gate.sh`

- [ ] **Step 1: Add SQL probes for environment and oversized DB discovery**

Create SQL files that:
- verify archive settings
- list databases over `100MB`
- expose the current `data_directory`, current archive settings, and extension version

- [ ] **Step 2: Implement per-version archive validation**

In `prepare_empty_instance.sh`, compute the expected archive path from the detected major version and fail fast unless the instance is configured to archive into the matching `/walstorage/<major>waldata`.

- [ ] **Step 3: Implement pre-run cleanup**

Delete non-template databases larger than `100MB`, clear the current major-version archive dir, and recreate `alldb`.

- [ ] **Step 4: Wire cleanup and environment summary into the orchestrator**

Have `run_release_gate.sh` persist the environment summary JSON before moving to data generation.

- [ ] **Step 5: Run a focused preparation dry run**

Run:

```bash
tests/release_gate/bin/prepare_empty_instance.sh
```

Expected:
- `alldb` exists afterward
- only allowed small databases remain
- the current version archive directory has been emptied
- a readable environment summary artifact is produced

### Task 4: Add `alldbsimulator` service orchestration and workload drivers

**Files:**
- Create: `tests/release_gate/bin/start_alldbsim.sh`
- Create: `tests/release_gate/bin/load_alldb_seed.sh`
- Create: `tests/release_gate/bin/run_alldb_dml_pressure.sh`
- Create: `tests/release_gate/bin/grow_flashback_target.sh`
- Create: `tests/release_gate/sql/table_size_summary.sql`
- Modify: `tests/release_gate/bin/common.sh`
- Modify: `tests/release_gate/config/release_gate.conf`
- Modify: `tests/release_gate/config/scenario_matrix.json`
- Test reference: `/root/alldbsimulator/README.md`

- [ ] **Step 1: Implement simulator lifecycle wrapper**

Add `start_alldbsim.sh` to:
- launch `/root/alldbsimulator/bin/alldbsim` on a configurable listen address
- poll the health endpoint until ready
- capture pid/log paths
- stop the service on normal exit or trap cleanup

- [ ] **Step 2: Implement seed loading via simulator APIs**

Add `load_alldb_seed.sh` to request a generation job that produces `50` tables sized toward `100MB` each, then wait for job completion and persist the returned job payload.

- [ ] **Step 3: Implement the 1-hour DML pressure driver**

Add `run_alldb_dml_pressure.sh` to submit the mixed DML workload, wait for completion, and record `pressure_start_ts`, `pressure_end_ts`, seed, and job metadata.

- [ ] **Step 4: Implement 5GB target-table growth**

Add `grow_flashback_target.sh` to extend the configured table until `pg_total_relation_size` reaches or exceeds `5GB`, then record final size metadata.

- [ ] **Step 5: Add size verification SQL**

Use `table_size_summary.sql` to assert:
- table count is `50`
- representative table sizes are near the intended `100MB`
- target table reached `5GB`

- [ ] **Step 6: Run a controlled data-build verification**

Run:

```bash
tests/release_gate/bin/start_alldbsim.sh
tests/release_gate/bin/load_alldb_seed.sh
tests/release_gate/bin/run_alldb_dml_pressure.sh
tests/release_gate/bin/grow_flashback_target.sh
```

Expected:
- simulator starts cleanly
- each job reaches a terminal success state
- table-size summary confirms the expected dataset shape

### Task 5: Implement truth snapshot export and standardized CSV helpers

**Files:**
- Create: `tests/release_gate/bin/capture_truth_snapshots.sh`
- Create: `tests/release_gate/sql/export_table_csv.sql`
- Create: `tests/release_gate/sql/export_flashback_csv.sql`
- Modify: `tests/release_gate/bin/common.sh`
- Modify: `tests/release_gate/config/scenario_matrix.json`

- [ ] **Step 1: Implement reproducible random timestamp selection**

Add a helper in `capture_truth_snapshots.sh` that derives five timestamps inside the recorded pressure window from the configured seed and saves them to a machine-readable artifact.

- [ ] **Step 2: Add standardized export SQL for live tables**

Create `export_table_csv.sql` that exports tables with:
- stable column order
- stable key ordering
- fixed `NULL` formatting
- CSV suitable for `sha256` and diff comparison

- [ ] **Step 3: Add standardized export SQL for flashback queries**

Create `export_flashback_csv.sql` that wraps `SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text)` and emits the same normalized CSV shape as live exports.

- [ ] **Step 4: Implement truth snapshot capture**

For each selected timestamp and table, export the truth CSV, calculate row count and `sha256`, and save a snapshot manifest JSON.

- [ ] **Step 5: Verify snapshot output determinism**

Run:

```bash
tests/release_gate/bin/capture_truth_snapshots.sh
```

Expected:
- five timestamp entries are created
- each target table produces a CSV plus manifest entry
- rerunning without changing data regenerates the same hashes

### Task 6: Implement the flashback scenario matrix, `COPY TO`, and `CTAS`

**Files:**
- Create: `tests/release_gate/bin/run_flashback_matrix.sh`
- Create: `tests/release_gate/sql/create_flashback_ctas.sql`
- Create: `tests/release_gate/sql/drop_flashback_ctas.sql`
- Modify: `tests/release_gate/sql/export_flashback_csv.sql`
- Modify: `tests/release_gate/config/scenario_matrix.json`
- Modify: `tests/release_gate/bin/common.sh`

- [ ] **Step 1: Encode the scenario matrix**

Populate `scenario_matrix.json` with the approved scenarios:
- five random flashbacks
- single `insert/update/delete`
- `10000` row `insert/update/delete`
- mixed DML
- `COPY TO`
- `CTAS`

- [ ] **Step 2: Implement measured flashback execution**

In `run_flashback_matrix.sh`, execute each scenario, record wall-clock timing, persist raw SQL, and save normalized outputs plus row-count/hash metadata.

- [ ] **Step 3: Add `COPY TO` handling**

Materialize `COPY (SELECT * FROM pg_flashback(...)) TO ...` outputs into the same normalized comparison path used by plain query scenarios.

- [ ] **Step 4: Add deterministic `CTAS` handling**

Create CTAS helper SQL that writes into scenario-specific temp tables, exports them via `export_table_csv.sql`, and cleans them up afterward.

- [ ] **Step 5: Add measured-run policy**

Implement one warm-up run plus two measured runs per performance scenario, and store both raw timings and the selected timing used for gate comparison.

- [ ] **Step 6: Run the focused flashback matrix verification**

Run:

```bash
tests/release_gate/bin/run_flashback_matrix.sh
```

Expected:
- each scenario emits a structured result JSON
- query, `COPY TO`, and `CTAS` all produce comparable CSV outputs
- warm-up timings are excluded from the gate metric

### Task 7: Add correctness comparison, golden baselines, and gate evaluation

**Files:**
- Create: `tests/release_gate/bin/evaluate_gate.sh`
- Create: `tests/release_gate/golden/pg14.json`
- Create: `tests/release_gate/golden/pg15.json`
- Create: `tests/release_gate/golden/pg16.json`
- Create: `tests/release_gate/golden/pg17.json`
- Create: `tests/release_gate/golden/pg18.json`
- Modify: `tests/release_gate/config/thresholds.json`
- Modify: `tests/release_gate/README.md`

- [ ] **Step 1: Create golden baseline schemas**

Add empty-but-valid baseline JSON files for `PG14-18` with the agreed fields:
- metadata
- scenario list
- expected row counts and hashes
- baseline timing
- ratio threshold
- absolute threshold

- [ ] **Step 2: Implement correctness evaluation**

Compare each scenario’s normalized output against the truth snapshot or derived expectation and classify failures as `correctness_fail`.

- [ ] **Step 3: Implement dual-threshold regression logic**

For scenarios with a golden entry, classify `performance_regression_fail` only if both the relative and absolute thresholds are exceeded.

- [ ] **Step 4: Implement infrastructure-failure handling**

Mark missing artifacts, simulator crashes, archive-dir mismatches, and interrupted jobs as `infrastructure_fail` with explicit reasons.

- [ ] **Step 5: Run evaluator validation on fixture data**

Run:

```bash
jq empty tests/release_gate/golden/pg14.json tests/release_gate/golden/pg15.json tests/release_gate/golden/pg16.json tests/release_gate/golden/pg17.json tests/release_gate/golden/pg18.json
tests/release_gate/bin/evaluate_gate.sh
```

Expected:
- baseline JSON parses cleanly
- evaluator produces a single verdict JSON with pass/fail plus failure categories

### Task 8: Render the Markdown report and wire end-to-end cleanup

**Files:**
- Create: `tests/release_gate/bin/render_report.sh`
- Modify: `tests/release_gate/templates/report.md.tpl`
- Modify: `tests/release_gate/bin/run_release_gate.sh`
- Modify: `tests/release_gate/bin/common.sh`
- Modify: `tests/release_gate/output/README.md`

- [ ] **Step 1: Implement report rendering**

Render one Markdown report per run that includes:
- environment summary
- archive-dir details
- dataset summary
- correctness results
- performance results
- failure details
- artifact paths

- [ ] **Step 2: Guarantee archive cleanup on every exit path**

Wire the orchestrator cleanup `trap` so the current major-version archive directory is cleared on both success and failure, and record pre/post cleanup status in artifacts.

- [ ] **Step 3: Wire the full pipeline**

Update `run_release_gate.sh` so it executes:
- prepare
- simulator start
- seed load
- DML pressure
- target growth
- truth capture
- flashback matrix
- evaluation
- report rendering
- cleanup

- [ ] **Step 4: Run a full PG18 end-to-end release-gate validation**

Run:

```bash
tests/release_gate/bin/run_release_gate.sh
```

Expected:
- the harness completes with a single verdict
- a Markdown report is written under `tests/release_gate/output/...`
- `/walstorage/18waldata` is empty after completion

- [ ] **Step 5: Generate and commit initial golden baselines**

After the first trusted full run per version, populate `tests/release_gate/golden/pg14.json` through `pg18.json` with the measured baseline values and commit them with the harness.

- [ ] **Step 6: Perform broad final verification**

Run:

```bash
bash -n tests/release_gate/bin/*.sh
jq empty tests/release_gate/config/*.json tests/release_gate/golden/*.json
tests/release_gate/bin/run_release_gate.sh
```

Expected:
- no shell syntax issues
- all JSON files are valid
- the release gate reaches a stable terminal PASS/FAIL verdict with a report and cleaned archive dir
