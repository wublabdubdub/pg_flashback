# pg_flashback_to Keyed Parallel Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in keyed parallel export path for `pg_flashback_to(regclass, text)` that uses multiple workers and a finalizer to build the final flashback table.

**Architecture:** Keep the current serial export path as default. When `pg_flashback.export_parallel_workers > 1` and the relation is keyed with a single stable key, launch multiple workers that each rerun the flashback pipeline and retain only their key-hash shard into stage tables; then a finalizer worker merges stages into the final table and rebuilds constraints/indexes.

**Tech Stack:** PostgreSQL extension C, dynamic background workers, DSM task state, SPI DDL, table AM bulk insert, regression SQL tests

---

### Task 1: Record the new keyed-parallel contract

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `docs/decisions/ADR-0015-keyed-parallel-pg-flashback-to.md`
- Create: `docs/superpowers/specs/2026-03-29-pg-flashback-to-keyed-parallel-design.md`

- [ ] Step 1: Record the keyed-only scope and transaction-lifetime change.
- [ ] Step 2: Save ADR and design note before code.

### Task 2: Add failing regression coverage first

**Files:**
- Create: `sql/fb_flashback_to_parallel.sql`
- Create: `expected/fb_flashback_to_parallel.out`
- Modify: `Makefile`

- [ ] Step 1: Add a keyed scenario with `pg_flashback.export_parallel_workers = 2`.
- [ ] Step 2: Assert correctness of final rows and structure.
- [ ] Step 3: Run the new regression and confirm it fails before implementation.

### Task 3: Add the export parallel GUC and routing

**Files:**
- Modify: `src/fb_guc.c`
- Modify: `include/fb_guc.h`
- Modify: `src/fb_export.c`

- [ ] Step 1: Add `pg_flashback.export_parallel_workers`.
- [ ] Step 2: Route keyed single-key relations to serial or parallel export.

### Task 4: Implement worker/finalizer orchestration

**Files:**
- Modify: `src/fb_export.c`
- Modify: `include/fb_export.h`
- Modify: `docs/architecture/overview.md`

- [ ] Step 1: Add DSM task state and bgworker entry points.
- [ ] Step 2: Implement shard worker stage-table export.
- [ ] Step 3: Implement finalizer merge and cleanup.

### Task 5: Verify and document

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`

- [ ] Step 1: Run focused install/installcheck verification.
- [ ] Step 2: Record remaining risks, especially duplicated replay cost.
