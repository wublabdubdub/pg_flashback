# Unresolved XID Debug And Replay Error Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose a public structured SQL function that lists unresolved/fallback XIDs for a flashback target and upgrade replay errors so copied logs contain enough stable context for offline diagnosis.

**Architecture:** Reuse the existing WAL metadata and xid-resolution pipeline in `fb_wal.c`, add one new public SQL entry in `fb_entry.c`, and surface per-xid probe results as `TABLE` rows. Keep replay correctness unchanged, but standardize replay error detail fields so user-provided logs can identify the failing record without reconnecting to the source database.

**Tech Stack:** PostgreSQL extension C, PGXS regression SQL/expected files, extension versioned SQL install/upgrade scripts.

---

### Task 1: Lock Failing Surface Tests

**Files:**
- Modify: `sql/fb_user_surface.sql`
- Modify: `sql/fb_extension_upgrade.sql`
- Modify: `sql/fb_wal_error_surface.sql`
- Create: `sql/fb_unresolved_xid_debug.sql`
- Create: `expected/fb_unresolved_xid_debug.out`
- Modify: `expected/fb_user_surface.out`
- Modify: `expected/fb_extension_upgrade.out`
- Modify: `expected/fb_wal_error_surface.out`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing regression for the new public function**

Add checks that `pg_flashback_debug_unresolv_xid(regclass, timestamptz)` is installed and callable.

- [ ] **Step 2: Add a fixture that produces at least one unresolved/fallback xid row**

Prefer a summary-gap or subxid fixture already close to `fb_recordref.sql` patterns so the regression stays stable.

- [ ] **Step 3: Add a failing replay error-surface expectation**

Tighten `fb_wal_error_surface.sql` so failing replay/missing-FPI output must include the new stable keys.

- [ ] **Step 4: Run the targeted regressions and capture the red state**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_user_surface fb_extension_upgrade fb_wal_error_surface fb_unresolved_xid_debug'`

- [ ] **Step 5: Confirm failures are for the expected missing function/detail fields**

Do not start implementation until the failure mode is clearly the new API/detail surface.

### Task 2: Add the Public SQL Surface

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `include/fb_entry.h`
- Modify: `sql/pg_flashback--0.2.2.sql`
- Modify: `sql/pg_flashback--0.2.1--0.2.2.sql`
- Modify: `pg_flashback.control`
- Modify: `VERSION`
- Modify: `Makefile`

- [ ] **Step 1: Declare the new SQL entry point**

Add `PG_FUNCTION_INFO_V1(pg_flashback_debug_unresolv_xid)` and header declaration.

- [ ] **Step 2: Implement a tuple-returning wrapper in `fb_entry.c`**

Use relation gate + target-ts parsing patterns consistent with existing entry points.

- [ ] **Step 3: Install the new function in the versioned SQL scripts**

Keep the function public and documented with `COMMENT ON FUNCTION`.

- [ ] **Step 4: Bump extension version and add upgrade path**

Move default/install version to `0.2.2` because the public SQL surface changes.

- [ ] **Step 5: Re-run the public-surface and upgrade regressions**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_user_surface fb_extension_upgrade'`

### Task 3: Materialize Structured Unresolved-XID Rows

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h` only if a shared struct/prototype is actually needed
- Test: `sql/fb_unresolved_xid_debug.sql`

- [ ] **Step 1: Add a worker function that builds unresolved/fallback xid rows from existing probe logic**

Prefer reusing the existing summary metadata, fallback-set building, and per-xid probe helpers instead of introducing a second scan pipeline.

- [ ] **Step 2: Classify each returned xid**

Populate `xid_role`, `resolved_by`, `fallback_reason`, `top_xid`, `commit_ts`, `summary_missing_segments`, `fallback_windows`, and `diag`.

- [ ] **Step 3: Keep empty-result semantics**

Return zero rows when no unresolved/fallback xid remains after the existing resolution chain.

- [ ] **Step 4: Run the new regression and make it pass**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_unresolved_xid_debug'`

- [ ] **Step 5: Refactor only if needed**

If helper extraction is necessary, keep it small and local to xid debug/probe code paths.

### Task 4: Standardize Replay Error Detail

**Files:**
- Modify: `src/fb_replay.c`
- Test: `sql/fb_wal_error_surface.sql`

- [ ] **Step 1: Add a shared replay-error detail appender/helper**

Centralize stable fields such as `record_kind`, `xid`, `lsn`, `end_lsn`, relation locator, fork/block/offset, toast flag, and page-image/data flags.

- [ ] **Step 2: Route existing replay error sites through the helper**

At minimum cover the current heap insert/update/multi-insert failure sites and missing-FPI path.

- [ ] **Step 3: Preserve existing per-error page-state detail**

Append new stable keys without deleting useful page-specific fields.

- [ ] **Step 4: Run the replay error-surface regression**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_wal_error_surface fb_replay'`

- [ ] **Step 5: Verify the copied error text is sufficient for offline record identification**

Check that a user can see `record_kind`, `xid`, `lsn`, `end_lsn`, and locator/block identity directly in the error text.

### Task 5: Final Verification And Repository Hygiene

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md` only if product boundary wording changes
- Modify: `open_source/pg_flashback/*` via sync script

- [ ] **Step 1: Run the targeted regression set**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_user_surface fb_extension_upgrade fb_unresolved_xid_debug fb_wal_error_surface fb_replay fb_recordref'`

- [ ] **Step 2: If public SQL or headers changed, use clean install path**

Run:
```bash
make clean
make PG_CONFIG=/home/18pg/local/bin/pg_config install
```

- [ ] **Step 3: Update `STATUS.md` and `TODO.md`**

Record the new public interface, current progress, and remaining follow-up.

- [ ] **Step 4: Sync the open-source mirror**

Run: `bash scripts/sync_open_source.sh`

- [ ] **Step 5: Re-check the synced public files**

Confirm `src/`, `include/`, `sql/`, and `expected/` changes are reflected under `open_source/pg_flashback/`.
