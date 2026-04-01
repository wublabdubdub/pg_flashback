# Build-Index Progress And Payload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make stage `3/9` accurately expose its internal hotspot via `NOTICE` subphases and reduce the hidden payload-materialize work in the build-index tail.

**Architecture:** Keep the public 9-stage progress surface unchanged, but remap stage `3/9` onto fixed subphase ranges: `prefilter`, `summary-span`, `metadata`, `xact-status`, and `payload`. Then tighten the payload materialize path inside `fb_wal_build_record_index()` and expose payload counters through existing debug helpers so regressions and live cases can prove the reduction is real.

**Tech Stack:** PostgreSQL extension C, PGXS regression SQL, existing `fb_progress`, `fb_wal`, `fb_entry`, and regression debug helpers.

---

### Task 1: Document The Approved Direction

**Files:**
- Create: `docs/superpowers/specs/2026-04-01-build-index-progress-and-payload-design.md`
- Create: `docs/superpowers/plans/2026-04-01-build-index-progress-and-payload-plan.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Record the approved design**

Write the confirmed design into the spec file, including:
- keep 9 stages unchanged
- split `3/9` into fixed subphases
- do not special-case `count(*)`
- optimize build-index payload rather than replay discover

- [ ] **Step 2: Update project status docs**

Add concise status/TODO entries so the next handoff can see:
- why `4/9` is not the hotspot
- why `3/9` observability changes are part of the fix

### Task 2: Write Failing Progress Regression

**Files:**
- Modify: `sql/fb_progress.sql`
- Modify: `expected/fb_progress.out`

- [ ] **Step 1: Write the failing regression expectation**

Change the regression fixture so the expected `NOTICE` stream for `3/9` requires the new subphase details:
- `prefilter`
- `summary-span`
- `metadata`
- `xact-status`
- `payload`

- [ ] **Step 2: Run the targeted regression and verify it fails**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS=fb_progress`

Expected: `fb_progress` fails because `3/9` still emits the old generic detail stream.

- [ ] **Step 3: Implement the minimal progress mapping change**

Modify:
- `src/fb_wal.c`
- `src/fb_progress.c` only if helper behavior must change

Add explicit `3/9` subphase progress calls using `fb_progress_map_subrange()`.

- [ ] **Step 4: Re-run the targeted regression**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS=fb_progress`

Expected: PASS with the new `3/9` detail stream.

### Task 3: Write Failing Payload-Counter Regression

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`

- [ ] **Step 1: Extend the debug contract in the regression**

Require `fb_recordref_debug()` to expose payload-specific counters, for example:
- payload windows
- payload covered segments
- payload scanned/decoded records
- payload kept/materialized records

- [ ] **Step 2: Run the targeted regression and verify it fails**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS=fb_recordref`

Expected: `fb_recordref` fails because the new payload counters are not yet present.

- [ ] **Step 3: Implement the minimal debug-surface change**

Modify:
- `include/fb_wal.h`
- `src/fb_wal.c`
- `src/fb_entry.c`

Capture the payload counters during build-index and include them in `fb_recordref_debug()`.

- [ ] **Step 4: Re-run the targeted regression**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS=fb_recordref`

Expected: PASS with the new payload counters.

### Task 4: Tighten Build-Index Payload Work

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Test: `sql/fb_recordref.sql`
- Test: `sql/fb_progress.sql`

- [ ] **Step 1: Measure the current payload work boundary in code**

Use the newly added counters to identify whether the current cost is dominated by:
- too many payload windows
- too many records decoded
- too many records serialized into the payload log

- [ ] **Step 2: Implement one minimal payload narrowing change**

Within `fb_wal_build_record_index()`:
- reduce payload work for windows proven irrelevant by the existing summary/span chain
- avoid redundant materialize/decode work where replay/final does not need it

- [ ] **Step 3: Re-run the focused regressions**

Run:
- `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_progress fb_recordref'`

Expected: PASS.

- [ ] **Step 4: Re-run the summary/replay safety bundle**

Run:
- `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_summary_v3 fb_summary_overlap_toast fb_replay'`

Expected: PASS.

### Task 5: Live-Case Verification

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Re-run the user’s live case**

Run in `alldb`:

```sql
select count(*)
from pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 01:40:13');
```

Confirm:
- `3/9` now names the hot subphase directly
- the old fake gap before `4/9` is gone
- if payload tightening works, the total `3/9` elapsed time decreases

- [ ] **Step 2: Update handoff docs with the verified result**

Record:
- old baseline
- new `3/9` subphase breakdown
- whether payload narrowing reduced elapsed time

