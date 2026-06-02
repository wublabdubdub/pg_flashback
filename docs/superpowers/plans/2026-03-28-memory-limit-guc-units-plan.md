# Memory Limit GUC Units Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `pg_flashback.memory_limit_kb` accept PostgreSQL memory units such as `kB`, `MB`, and `GB` directly.

**Architecture:** Replace the current custom real-valued GUC with a PostgreSQL native integer memory GUC that uses `GUC_UNIT_KB`, so parsing, normalization, and error handling stay in core behavior. Extend existing regressions first to require unit input, then update the GUC definition, runtime accessor, and user-facing docs.

**Tech Stack:** PostgreSQL extension (PGXS), C, `pg_regress`, Markdown

---

### Task 1: Record the approved requirement

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `docs/superpowers/plans/2026-03-28-memory-limit-guc-units-plan.md`

- [ ] **Step 1: Record the new unit-input requirement before code changes**
- [ ] **Step 2: Save the plan file for handoff history**

### Task 2: Write failing regression coverage

**Files:**
- Modify: `sql/fb_guc_defaults.sql`
- Modify: `expected/fb_guc_defaults.out`
- Modify: `sql/fb_memory_limit.sql`
- Modify: `expected/fb_memory_limit.out`

- [ ] **Step 1: Update `fb_guc_defaults` to require a unit-bearing `current_setting` and a successful `MB` assignment**
- [ ] **Step 2: Update `fb_memory_limit` to use unit-bearing `SET` statements**
- [ ] **Step 3: Run the targeted regressions and confirm they fail before implementation**

### Task 3: Implement the native unit GUC

**Files:**
- Modify: `src/fb_guc.c`
- Modify: `include/fb_guc.h`
- Modify: `README.md`

- [ ] **Step 1: Change `pg_flashback.memory_limit_kb` to a native integer GUC with `GUC_UNIT_KB`**
- [ ] **Step 2: Keep `fb_get_memory_limit_bytes()` behavior stable by converting stored KB to bytes**
- [ ] **Step 3: Update the README examples and wording to show unit-bearing input**

### Task 4: Verify and sync docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Run targeted build and regression commands until green**
- [ ] **Step 2: Update `STATUS.md` with completion and verification evidence**
- [ ] **Step 3: Mark the `TODO.md` item complete**
