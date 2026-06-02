# Streaming Query Flashback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the result-table flashback entrypoint with a direct streaming query interface whose apply memory no longer scales with current table size.

**Architecture:** Keep the WAL/replay/reverse-op core, but replace the public SQL surface with `pg_flashback(anyelement, text) RETURNS SETOF anyelement` and rewrite keyed/bag apply to be change-set driven. Remove obsolete result-table materialization and parallel result-table write code.

**Tech Stack:** PostgreSQL 18 C extension, PGXS, `pg_regress`, polymorphic SRF, dynahash/custom hash buckets.

---

### Task 1: Document and freeze the new product surface

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Modify: `README.md`
- Modify: `docs/architecture/overview.md`
- Create: `docs/decisions/ADR-0005-streaming-query-interface.md`
- Create: `docs/superpowers/specs/2026-03-25-streaming-query-flashback-design.md`

- [ ] Step 1: Update status and TODO to describe the new direct-query, bounded-memory target.
- [ ] Step 2: Add the new ADR and design doc.
- [ ] Step 3: Remove stale product statements that promise result-table creation or parallel result-table write.

### Task 2: Replace regression expectations with the new public interface

**Files:**
- Modify: `Makefile`
- Modify: `sql/pg_flashback.sql`
- Modify: `sql/fb_flashback_keyed.sql`
- Modify: `sql/fb_flashback_bag.sql`
- Modify: `sql/fb_user_surface.sql`
- Modify: `sql/fb_smoke.sql`
- Modify: `sql/fb_runtime_gate.sql`
- Modify: `sql/fb_progress.sql`
- Modify: `sql/fb_memory_limit.sql`
- Modify: `sql/fb_toast_flashback.sql`
- Modify: `expected/*.out` for affected regressions
- Delete: `sql/fb_parallel_apply.sql`
- Delete: `expected/fb_parallel_apply.out`
- Delete: `sql/fb_flashback_materialize.sql`
- Delete: `expected/fb_flashback_materialize.out`

- [ ] Step 1: Rewrite regression SQL files to call `SELECT * FROM pg_flashback(NULL::schema.table, target_ts);`
- [ ] Step 2: Remove tests that only validate result-table creation or parallel result-table write.
- [ ] Step 3: Run targeted regressions and confirm the new tests fail before implementation.

### Task 3: Replace the SQL/C install surface

**Files:**
- Modify: `sql/pg_flashback--0.1.0.sql`
- Modify: `include/fb_entry.h`
- Modify: `src/fb_entry.c`
- Modify: `include/fb_progress.h`
- Modify: `src/fb_progress.c`
- Modify: `include/fb_guc.h`
- Modify: `src/fb_guc.c`
- Delete: `include/fb_parallel.h`
- Delete: `src/fb_parallel.c`

- [ ] Step 1: Change the installed function signature to `pg_flashback(anyelement, text) RETURNS SETOF anyelement`.
- [ ] Step 2: Remove result-table creation and parallel-write code.
- [ ] Step 3: Convert `pg_flashback` to a value-per-call SRF with cleanup callbacks.
- [ ] Step 4: Rename stage 9 progress text from “materializing result table” to residual result emission.

### Task 4: Rewrite apply to stream current-table rows with bounded memory

**Files:**
- Modify: `include/fb_apply.h`
- Modify: `src/fb_apply_keyed.c`
- Modify: `src/fb_apply_bag.c`
- Modify: `src/fb_reverse_ops.c` if ownership or compaction needs change

- [ ] Step 1: Write failing keyed regression that proves unchanged current rows are streamed without a result table.
- [ ] Step 2: Implement keyed change-set state and per-key action lists.
- [ ] Step 3: Write failing bag regression that proves duplicate rows are handled without full-table bag materialization.
- [ ] Step 4: Implement bag delta state and residual emit.
- [ ] Step 5: Keep progress and memory accounting accurate.

### Task 5: Verify build and regression

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Step 1: Run `make clean && make install`.
- [ ] Step 2: Run targeted regressions for smoke, keyed, bag, user surface, progress, memory, toast.
- [ ] Step 3: Run full `make installcheck`.
- [ ] Step 4: Update status/todo with the actual shipped state and residual risks.
