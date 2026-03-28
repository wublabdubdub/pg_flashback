# Progress Elapsed Timing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add incremental elapsed timing to each `show_progress` NOTICE and emit one final total-duration NOTICE when progress output is enabled.

**Architecture:** Keep all timing logic inside `fb_progress` so existing stage callers stay unchanged. Add a deterministic regression-only debug entry point to validate elapsed formatting without depending on wall clock variance, and extend the existing `fb_progress` regression to cover the final total NOTICE plus `show_progress=off` silence.

**Tech Stack:** PostgreSQL extension (PGXS), C, SQL install/test scripts, `pg_regress`, Markdown

---

### Task 1: Document the approved behavior

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `docs/superpowers/plans/2026-03-28-progress-elapsed-timing-plan.md`

- [ ] **Step 1: Record the requirement before code changes**
- [ ] **Step 2: Confirm the plan file exists in-repo for handoff history**

### Task 2: Write failing regression coverage

**Files:**
- Modify: `sql/fb_progress.sql`
- Modify: `expected/fb_progress.out`
- Create: `sql/fb_progress_debug.sql`
- Create: `expected/fb_progress_debug.out`
- Modify: `Makefile`

- [ ] **Step 1: Add a regression-only debug SQL entry that can assert elapsed formatting deterministically**
- [ ] **Step 2: Extend `fb_progress` expectations to require a final total-duration NOTICE while preserving `show_progress=off` silence**
- [ ] **Step 3: Run the targeted regressions and confirm they fail before implementation**

### Task 3: Implement minimal elapsed timing support

**Files:**
- Modify: `src/fb_progress.c`
- Modify: `include/fb_progress.h`
- Modify: `src/fb_entry.c`

- [ ] **Step 1: Add query-start and last-emit timestamp tracking to `fb_progress`**
- [ ] **Step 2: Append incremental elapsed text to each emitted progress NOTICE only when progress is enabled**
- [ ] **Step 3: Emit one final total-duration NOTICE from `fb_progress_finish()`**
- [ ] **Step 4: Add the regression-only debug entry point needed by the deterministic test**

### Task 4: Verify and sync status

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Run targeted build and regressions until green**
- [ ] **Step 2: Update `STATUS.md` with completed work and verification evidence**
- [ ] **Step 3: Mark the `TODO.md` item complete**
