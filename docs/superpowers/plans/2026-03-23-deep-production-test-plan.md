# Deep Production Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a staged deep-validation test suite for `pg_flashback` that exercises >50 columns, >5 million rows, >50 WAL segments, multiple DML patterns, transaction boundaries, and dual-source WAL resolution.

**Architecture:** The test work is split into environment bootstrap, dataset generation, truth snapshot capture, five execution batches, and reporting. Each batch is isolated so failures are attributable to one subsystem instead of a single mixed mega-test.

**Tech Stack:** PostgreSQL 18, `psql`, SQL scripts, shell orchestration, local archive WAL directories, `pg_flashback` extension.

---

### Task 1: Create Deep-Test Spec References In Project State

**Files:**
- Modify: `TODO.md`
- Modify: `STATUS.md`
- Modify: `README.md`
- Create: `docs/specs/2026-03-23-deep-production-test-design.md`

- [ ] **Step 1: Verify the new deep-test design file exists**

Run: `test -f docs/specs/2026-03-23-deep-production-test-design.md`
Expected: exit code `0`

- [ ] **Step 2: Update TODO and STATUS so deep validation is a visible project phase**

Add:
- deep validation entry
- current next-step note
- success criteria summary

- [ ] **Step 3: Re-read modified docs for consistency**

Run: `sed -n '1,260p' TODO.md && sed -n '1,260p' STATUS.md`
Expected: deep test phase and current intent are visible

### Task 2: Create Deep-Test Directory Structure

**Files:**
- Create: `tests/deep/README.md`
- Create: `tests/deep/sql/`
- Create: `tests/deep/bin/`
- Create: `tests/deep/output/.gitkeep`

- [ ] **Step 1: Create directory skeleton**

Run: `mkdir -p tests/deep/sql tests/deep/bin tests/deep/output`
Expected: directories created

- [ ] **Step 2: Add a README describing how deep tests differ from regress tests**

Include:
- not part of `make installcheck`
- requires dedicated database
- may run for hours
- produces large WAL and output

- [ ] **Step 3: Add `.gitkeep` to output directory**

Expected: path exists in git

### Task 3: Write Environment Bootstrap Script

**Files:**
- Create: `tests/deep/bin/bootstrap_env.sh`
- Test: `tests/deep/README.md`

- [ ] **Step 1: Write a failing dry-run command expectation in README**

Document:
`tests/deep/bin/bootstrap_env.sh --dry-run`

- [ ] **Step 2: Implement bootstrap script**

Responsibilities:
- check PG18 login method
- create deep-test database
- create archive output directories
- set required GUCs / session variables
- print chosen archive paths

- [ ] **Step 3: Run bootstrap dry-run**

Run: `bash tests/deep/bin/bootstrap_env.sh --dry-run`
Expected: prints actions without modifying database

### Task 4: Create Large Table DDL Generator

**Files:**
- Create: `tests/deep/sql/01_schema_keyed.sql`
- Create: `tests/deep/sql/02_schema_bag.sql`
- Create: `tests/deep/sql/03_schema_truth.sql`

- [ ] **Step 1: Write keyed schema with >50 columns**

Include:
- 64 columns
- stable primary key
- non-TOAST types

- [ ] **Step 2: Write bag schema with >50 columns**

Include:
- 64 columns
- no key
- deliberate duplicate-friendly column patterns

- [ ] **Step 3: Write truth schema helpers**

Include helper tables for:
- target timestamps
- batch metadata
- truth snapshots

### Task 5: Build Baseline Data Loader

**Files:**
- Create: `tests/deep/bin/load_baseline.sh`
- Create: `tests/deep/sql/10_load_keyed_baseline.sql`
- Create: `tests/deep/sql/11_load_bag_baseline.sql`

- [ ] **Step 1: Write loading SQL using deterministic generated data**

Use:
- `generate_series`
- deterministic expressions
- batches sized to finish reliably

- [ ] **Step 2: Implement loader shell wrapper**

Responsibilities:
- run keyed load
- run bag load
- record row counts
- run explicit `CHECKPOINT`

- [ ] **Step 3: Verify rowcount goal**

Run SQL:
- keyed table count
- bag table count

Expected: each `> 5000000`

### Task 6: Build Truth Snapshot Capture

**Files:**
- Create: `tests/deep/sql/20_capture_truth.sql`
- Create: `tests/deep/bin/capture_truth.sh`

- [ ] **Step 1: Write snapshot SQL**

Responsibilities:
- capture `target_ts`
- create truth tables per batch/timepoint
- store metadata rows

- [ ] **Step 2: Implement wrapper**

Run:
- explicit `CHECKPOINT`
- snapshot capture
- output chosen timepoint names

- [ ] **Step 3: Verify one sample truth table exists**

Run: `\dt fb_truth_*`
Expected: at least one truth table exists

### Task 7: Implement Batch A Keyed High-DML Workload

**Files:**
- Create: `tests/deep/sql/30_batch_a_keyed_workload.sql`
- Create: `tests/deep/sql/31_batch_a_keyed_validate.sql`
- Create: `tests/deep/bin/run_batch_a.sh`

- [ ] **Step 1: Write workload SQL**

Include:
- large `UPDATE`
- large `DELETE`
- bounded `INSERT`
- committed and rolled-back transactions

- [ ] **Step 2: Write validation SQL**

Include:
- flashback materialization
- dual `EXCEPT`
- rowcount checks

- [ ] **Step 3: Run batch A and save output**

Expected:
- WAL segments generated > 50 across the window
- validation result is empty in both directions

### Task 8: Implement Batch B Transaction Boundary Workload

**Files:**
- Create: `tests/deep/sql/40_batch_b_txn_workload.sql`
- Create: `tests/deep/sql/41_batch_b_txn_validate.sql`
- Create: `tests/deep/bin/run_batch_b.sh`

- [ ] **Step 1: Write interleaved transaction workload**

Include:
- target-before-commit
- target-after-rollback
- repeated updates on same keys

- [ ] **Step 2: Write validation SQL**

Expected:
- only committed-before-target effects remain visible

- [ ] **Step 3: Run and archive output**

Save logs under `tests/deep/output/batch_b/`

### Task 9: Implement Batch C Bag Workload

**Files:**
- Create: `tests/deep/sql/50_batch_c_bag_workload.sql`
- Create: `tests/deep/sql/51_batch_c_bag_validate.sql`
- Create: `tests/deep/bin/run_batch_c.sh`

- [ ] **Step 1: Write duplicate-heavy workload**

Include:
- many duplicate row values
- `INSERT / DELETE / UPDATE`

- [ ] **Step 2: Write grouped validation SQL**

Use:
- `GROUP BY` all columns
- `count(*)`
- dual `EXCEPT`

- [ ] **Step 3: Run and record result**

Expected:
- grouped diffs are empty

### Task 10: Implement Batch D WAL Source Switching Workload

**Files:**
- Create: `tests/deep/sql/60_batch_d_source_workload.sql`
- Create: `tests/deep/sql/61_batch_d_source_validate.sql`
- Create: `tests/deep/bin/run_batch_d.sh`

- [ ] **Step 1: Write source-splitting setup**

Create conditions where:
- older WAL is only in `archive_dest`
- recent WAL remains in `pg_wal`

- [ ] **Step 2: Add mismatch + ckwal scenario**

Use:
- debug pg_wal override
- ckwal restore directory

- [ ] **Step 3: Validate both normal and recovery paths**

Expected:
- normal dual-source flashback succeeds
- mismatch path fails without ckwal
- mismatch path succeeds with ckwal restore dir

### Task 11: Implement Batch E Long-Window Stress

**Files:**
- Create: `tests/deep/sql/70_batch_e_stress_workload.sql`
- Create: `tests/deep/sql/71_batch_e_stress_validate.sql`
- Create: `tests/deep/bin/run_batch_e.sh`

- [ ] **Step 1: Write long-window workload**

Target:
- `80-120` WAL segments
- wide time window

- [ ] **Step 2: Record runtime and memory observations**

Capture:
- wall-clock runtime
- chosen memory limit
- whether failure occurs

- [ ] **Step 3: Classify result**

Possible statuses:
- pass
- expected limit hit
- correctness failure

### Task 12: Build End-To-End Orchestrator

**Files:**
- Create: `tests/deep/bin/run_all_deep_tests.sh`
- Create: `tests/deep/output/README.md`

- [ ] **Step 1: Implement batch runner**

Order:
- bootstrap
- load baseline
- capture truth
- batch A
- batch B
- batch C
- batch D
- batch E

- [ ] **Step 2: Make each batch resumable**

Requirements:
- independent output dirs
- no silent overwrite
- clear failure exit codes

- [ ] **Step 3: Run orchestrator in dry-run**

Expected: prints batch order and required resources

### Task 13: Write Test Report Template

**Files:**
- Create: `tests/deep/report-template.md`

- [ ] **Step 1: Add environment section**

Include:
- PG version
- archive config
- table sizes
- WAL segment counts

- [ ] **Step 2: Add per-batch result section**

Include:
- duration
- row counts
- validation outcome
- failure notes

- [ ] **Step 3: Add final release gate summary**

Include:
- pass / fail
- known limits
- recommended next engineering tasks

### Task 14: Run Initial Pilot On Reduced Scale

**Files:**
- Modify: `tests/deep/bin/run_all_deep_tests.sh`
- Output: `tests/deep/output/pilot/`

- [ ] **Step 1: Add a pilot mode**

Pilot should:
- run same logic
- reduced row counts
- reduced WAL target

- [ ] **Step 2: Execute pilot mode**

Expected:
- all scripts run end-to-end
- no syntax/runtime issues

- [ ] **Step 3: Fix pilot issues before full deep run**

Only after pilot is stable should the 5M/50-segment run begin.

### Task 15: Execute Full Deep Validation

**Files:**
- Output: `tests/deep/output/full/`
- Create: `tests/deep/output/full/report.md`

- [ ] **Step 1: Run full deep suite**

Run: `bash tests/deep/bin/run_all_deep_tests.sh --full`

- [ ] **Step 2: Verify pass criteria**

Check:
- row thresholds
- WAL thresholds
- dual `EXCEPT`
- grouped `bag` validation

- [ ] **Step 3: Write final report**

Summarize:
- which batches passed
- measured limits
- next implementation priorities
