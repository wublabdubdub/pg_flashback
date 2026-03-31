# Summary Progress Surface Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current summary progress user surface with a clearer `pg_flashback_summary_progress` view and move internal queue/service counters into a separate debug view.

**Architecture:** Keep the existing summary-service snapshot scan as the single source of truth, but split its output into two SQL surfaces. The user view exposes only stable-window frontiers and first-gap coordinates, while the debug view keeps launcher/worker/queue counters for development and regression use.

**Tech Stack:** PostgreSQL extension SQL install script, C SRF in `src/fb_summary_service.c`, pg_regress

---

### Task 1: Document the breaking user-surface change

**Files:**
- Create: `docs/superpowers/specs/2026-03-31-summary-progress-surface-redesign.md`
- Create: `docs/decisions/ADR-0022-summary-progress-surface-redesign.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Update the docs to state that `fb_summary_progress` is being replaced by `pg_flashback_summary_progress`**
- [ ] **Step 2: Record the split between user view and debug view**
- [ ] **Step 3: Record the new `pg_flashback_` naming rule for future user-visible summary observability**

### Task 2: Write the failing regressions first

**Files:**
- Modify: `sql/fb_summary_service.sql`
- Modify: `expected/fb_summary_service.out`

- [ ] **Step 1: Rewrite the regression to query `pg_flashback_summary_progress` instead of `fb_summary_progress`**
- [ ] **Step 2: Add checks for `near_contiguous_through_ts`, `far_contiguous_until_ts`, and both first-gap columns**
- [ ] **Step 3: Add checks that debug/service counters now come from `pg_flashback_summary_service_debug`**
- [ ] **Step 4: Run `installcheck` for `fb_summary_service` and confirm the new regression fails for the expected missing objects/columns**

### Task 3: Implement the new SQL surfaces

**Files:**
- Modify: `sql/pg_flashback--0.1.0.sql`
- Modify: `src/fb_summary_service.c`

- [ ] **Step 1: Replace the old installed view with `pg_flashback_summary_progress`**
- [ ] **Step 2: Add `pg_flashback_summary_service_debug` for launcher/worker/queue counters**
- [ ] **Step 3: Keep all newly exposed user objects on the `pg_flashback_` prefix**
- [ ] **Step 4: Extend the C progress collector with stable-window timestamps and first-gap coordinates**
- [ ] **Step 5: Keep the existing snapshot-based denominator semantics intact**

### Task 4: Verify and sync outputs

**Files:**
- Modify: `expected/fb_summary_service.out`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Run focused regressions for the summary service surface**
Run: `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_summary_service'`
Expected: `All 1 tests passed.`

- [ ] **Step 2: Run one broader summary-related regression bundle**
Run: `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_summary_service fb_summary_v3 fb_wal_sidecar'`
Expected: `All 3 tests passed.`

- [ ] **Step 3: Update `STATUS.md` / `TODO.md` to reflect the completed user-surface redesign**
