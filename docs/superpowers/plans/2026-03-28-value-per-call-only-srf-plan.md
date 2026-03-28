# ValuePerCall-only SRF Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove internal materialized SRF usage from `pg_flashback()` and make the extension rely exclusively on the ValuePerCall SRF path.

**Architecture:** Keep the existing keyed/bag streaming apply pipeline unchanged, but collapse the SQL entry point onto the current `SRF_PERCALL_SETUP()` path. Remove `tuplestore` plumbing from apply, and add a regression that proves low-`work_mem` `count(*)` no longer causes function-owned temp spill.

**Tech Stack:** PostgreSQL C extension, PGXS regressions, SQL regression tests, project ADR/docs

---

### Task 1: Document the decision

**Files:**
- Create: `docs/decisions/ADR-0009-value-per-call-only-srf.md`
- Create: `docs/superpowers/specs/2026-03-28-value-per-call-only-srf-design.md`
- Create: `docs/superpowers/plans/2026-03-28-value-per-call-only-srf-plan.md`

- [ ] **Step 1: Record the architectural decision**
- [ ] **Step 2: Record the implementation design**
- [ ] **Step 3: Save this implementation plan**

### Task 2: Add the failing regression first

**Files:**
- Create: `sql/fb_value_per_call.sql`
- Create: `expected/fb_value_per_call.out`
- Modify: `Makefile`

- [ ] **Step 1: Write a regression that captures `temp_bytes` before and after `count(*) FROM pg_flashback(...)` under low `work_mem`**
- [ ] **Step 2: Add the regression to `REGRESS`**
- [ ] **Step 3: Run the regression and verify it fails against the old materialized path**

### Task 3: Remove materialized SRF code

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `src/fb_apply.c`
- Modify: `include/fb_apply.h`

- [ ] **Step 1: Delete the `prefer_materialize` branch from `pg_flashback()`**
- [ ] **Step 2: Delete `fb_apply_materialize()` and `tuplestore`-specific helpers/includes**
- [ ] **Step 3: Verify the new regression passes**

### Task 4: Update project docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Modify: `README.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/核心入口源码导读.md`
- Modify: `docs/architecture/源码级维护手册.md`
- Modify: `docs/decisions/ADR-0007-toast-heavy-materialized-srf.md`

- [ ] **Step 1: Replace “may internally materialize” statements with ValuePerCall-only wording**
- [ ] **Step 2: Mark ADR-0007 as superseded by ADR-0009**
- [ ] **Step 3: Update status/todo to reflect the removal and new regression**

### Task 5: Verify cleanly

**Files:**
- Modify: none

- [ ] **Step 1: Run `make clean` because headers will change**
- [ ] **Step 2: Run `make install`**
- [ ] **Step 3: Run targeted regressions covering `fb_value_per_call`, `pg_flashback`, `fb_user_surface`, and `fb_progress`**
- [ ] **Step 4: Run any additional regressions touched by entry/apply behavior**
