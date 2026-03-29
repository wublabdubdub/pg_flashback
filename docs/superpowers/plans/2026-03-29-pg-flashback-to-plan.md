# pg_flashback_to Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `pg_flashback_to(regclass, text)` to create a persistent `<table>_flashback` table populated with flashback rows.

**Architecture:** Reuse the existing flashback query pipeline and add a write-capable export sink that clones the base table shape, bulk-loads result rows, then recreates PK/UNIQUE constraints and non-constraint indexes. Keep the existing `pg_flashback(anyelement, text)` path unchanged.

**Tech Stack:** PostgreSQL extension C, SPI/utility DDL execution, table AM bulk insert, regression SQL tests

---

### Task 1: Record the new user-surface decision

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Create: `docs/decisions/ADR-0013-pg-flashback-to-persistent-table-export.md`
- Create: `docs/superpowers/specs/2026-03-29-pg-flashback-to-design.md`

- [ ] Step 1: Record scope and product boundary changes before code.
- [ ] Step 2: Save the ADR and design note for `pg_flashback_to`.

### Task 2: Add regression coverage first

**Files:**
- Create: `sql/fb_flashback_to.sql`
- Create: `expected/fb_flashback_to.out`
- Modify: `Makefile`
- Modify: `sql/pg_flashback--0.1.0.sql`

- [ ] Step 1: Add a regression test that proves persistent-table creation, data correctness, and structure-copy boundaries.
- [ ] Step 2: Run the new regression and confirm it fails before implementation.

### Task 3: Implement the export sink

**Files:**
- Modify: `src/fb_export.c`
- Create: `include/fb_export.h`
- Modify: `src/fb_entry.c`
- Modify: `include/fb_entry.h`
- Modify: `docs/architecture/overview.md`

- [ ] Step 1: Add helpers to clone the base table shape and name the target table.
- [ ] Step 2: Add bulk row loading from `fb_flashback_query_begin/next_slot`.
- [ ] Step 3: Add post-load PK/UNIQUE and non-constraint index recreation.
- [ ] Step 4: Expose `pg_flashback_to(regclass, text)` in the install script.

### Task 4: Verify the feature end to end

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Step 1: Run focused install/installcheck verification for `fb_flashback_to`.
- [ ] Step 2: Update status docs with evidence and remaining gaps.
