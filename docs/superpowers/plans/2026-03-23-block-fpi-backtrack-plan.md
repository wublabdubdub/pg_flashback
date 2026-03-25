# Block-Level Earlier FPI Backtracking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add shared block-level earlier-FPI backtracking so `missing FPI` can be resolved for both main and TOAST relations without per-block rescans.

**Architecture:** Keep the existing checkpoint-based global anchor, but when replay discovers blocks with no page baseline, collect them into a shared pending set, scan earlier `RecordRef` entries once to find per-block recoverable anchors (`FPI` or `INIT_PAGE`), warm those block states, then rerun normal replay. Do not attempt best-effort recovery when no earlier recoverable anchor exists.

**Tech Stack:** PostgreSQL extension C, `XLogReader`, `RecordRef`, `BlockReplayStore`, PGXS regression + deep SQL scripts

---

### Task 1: Record the chosen repair strategy in project docs

**Files:**
- Modify: `TODO.md`
- Modify: `STATUS.md`
- Modify: `PROJECT.md`
- Modify: `AGENTS.md`

- [ ] **Step 1: Update TODO and STATUS**
- [ ] **Step 2: Update PROJECT and AGENTS with shared block-level backtracking policy**
- [ ] **Step 3: Confirm docs describe one shared backward search, not one search per block**

### Task 2: Reproduce the blocker in a failing test/run

**Files:**
- Test: `tests/deep/bin/run_toast_scale.sh`
- Test: `tests/deep/sql/80_toast_scale.sql`
- Test: existing deep/full execution path

- [ ] **Step 1: Run the existing TOAST full script and confirm current `missing FPI` failure**
- [ ] **Step 2: Preserve the failing symptom in STATUS/report notes before code changes**

### Task 3: Add backtracking metadata to WAL/replay structures

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `include/fb_replay.h`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_replay.c`

- [ ] **Step 1: Introduce block-keyed pending/backtrack metadata**
- [ ] **Step 2: Expose enough index helpers to scan earlier `RecordRef` entries for matching blocks**
- [ ] **Step 3: Keep the design shared across main and TOAST relations**

### Task 4: Implement shared earlier-FPI backtracking

**Files:**
- Modify: `src/fb_replay.c`
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Modify: `include/fb_replay.h`

- [ ] **Step 1: Discovery pass collects all missing-baseline blocks without immediate hard failure**
- [ ] **Step 2: Shared backward scan resolves anchors for the collected block set**
- [ ] **Step 3: Warm `BlockReplayStore` from the discovered per-block anchors**
- [ ] **Step 4: Rerun normal replay using warmed block states**
- [ ] **Step 5: Keep final error strict when no recoverable earlier anchor exists**

### Task 5: Verify and document the outcome

**Files:**
- Modify: `STATUS.md`
- Modify: `README.md`
- Modify: `docs/reports/2026-03-23-toast-full-report.md`

- [ ] **Step 1: Run `make clean && make install`**
- [ ] **Step 2: Run `make installcheck` and confirm regressions still pass**
- [ ] **Step 3: Run `tests/deep/bin/run_toast_scale.sh --full`**
- [ ] **Step 4: Record whether TOAST full now passes or what residual blocker remains**
