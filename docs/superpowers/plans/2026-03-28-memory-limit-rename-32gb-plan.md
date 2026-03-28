# Memory Limit Rename And 32GB Cap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the user-facing memory GUC from `pg_flashback.memory_limit_kb` to `pg_flashback.memory_limit` and cap accepted values at `32GB`.

**Architecture:** Keep the current string GUC plus custom unit parser, but move the public parameter name to `pg_flashback.memory_limit` and tighten the range check to `1kB .. 32GB`. Update every SQL surface, hint string, regression, and customer-facing document so the renamed parameter is the only supported spelling.

**Tech Stack:** PostgreSQL PGXS extension C code, SQL regression tests, Markdown docs

---

### Task 1: Record The Interface Change

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `docs/superpowers/plans/2026-03-28-memory-limit-rename-32gb-plan.md`

- [ ] **Step 1: Add the new request to status/todo before touching code**
- [ ] **Step 2: Save this implementation plan under `docs/superpowers/plans/`**

### Task 2: Write The Failing Regression First

**Files:**
- Modify: `sql/fb_guc_defaults.sql`
- Modify: `expected/fb_guc_defaults.out`

- [ ] **Step 1: Replace `memory_limit_kb` with `memory_limit` in the GUC defaults regression**
- [ ] **Step 2: Change the maximum accepted sample from `4tb` to `32gb`**
- [ ] **Step 3: Add one over-limit case, `33gb`, and expect a `1kB .. 32GB` error**
- [ ] **Step 4: Run `installcheck REGRESS='fb_guc_defaults'` and confirm it fails for the renamed GUC / new cap**

### Task 3: Implement The Renamed GUC And New Cap

**Files:**
- Modify: `src/fb_guc.c`
- Modify: `include/fb_memory.h`

- [ ] **Step 1: Rename the public GUC registration from `pg_flashback.memory_limit_kb` to `pg_flashback.memory_limit`**
- [ ] **Step 2: Tighten the maximum accepted kB count from `4TB` to `32GB`**
- [ ] **Step 3: Update parser/detail/hint strings so every user-visible message references `pg_flashback.memory_limit`**
- [ ] **Step 4: Keep unit parsing and canonical display behavior unchanged aside from the new limit**

### Task 4: Update Remaining SQL And Docs

**Files:**
- Modify: `sql/fb_memory_limit.sql`
- Modify: `expected/fb_memory_limit.out`
- Modify: `sql/fb_spill.sql`
- Modify: `expected/fb_spill.out`
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `docs/architecture/overview.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Replace all user-facing `memory_limit_kb` references with `memory_limit` in regressions**
- [ ] **Step 2: Update docs to describe the renamed parameter and `32GB` maximum**
- [ ] **Step 3: Mark the task complete in status/todo once code and tests are green**

### Task 5: Verify End-To-End

**Files:**
- Modify: `src/fb_guc.c`
- Modify: `sql/fb_guc_defaults.sql`
- Modify: `sql/fb_memory_limit.sql`
- Modify: `sql/fb_spill.sql`

- [ ] **Step 1: Run `make PG_CONFIG=/home/18pg/local/bin/pg_config install`**
- [ ] **Step 2: Run `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults fb_memory_limit fb_spill'`**
- [ ] **Step 3: Confirm all three regressions pass and record the exact command/output in `STATUS.md`**
