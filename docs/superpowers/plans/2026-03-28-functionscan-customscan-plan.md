# FunctionScan To CustomScan Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace PostgreSQL's default `FunctionScan` execution of `FROM pg_flashback(...)` with an extension-owned `CustomScan` so large flashback queries stop spilling SRF output to `base/pgsql_tmp/*`.

**Architecture:** Add a planner `set_rel_pathlist_hook` that recognizes `RTE_FUNCTION` calls to `pg_flashback(anyelement, text)` and injects a `CustomPath`. Convert that path to a `CustomScan` with its own executor state, and reuse the existing flashback runtime/apply pipeline to stream rows directly into the executor result slot without `ExecMakeTableFunctionResult()` or `tuplestore`.

**Tech Stack:** PostgreSQL extension hooks, `CustomPath` / `CustomScan`, existing `fb_entry` / `fb_apply` / PGXS regression tests.

---

### Task 1: Add design records before code

**Files:**
- Create: `docs/decisions/ADR-0010-custom-scan-for-pg-flashback-from.md`
- Create: `docs/superpowers/specs/2026-03-28-functionscan-customscan-design.md`
- Create: `docs/superpowers/plans/2026-03-28-functionscan-customscan-plan.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Record the decision that `ValuePerCall` alone is insufficient for `FROM pg_flashback(...)` because `FunctionScan` still materializes into `tuplestore`.
- [ ] Record the chosen architecture: planner hook + `CustomScan`.
- [ ] Mark the work as in-progress in `STATUS.md` and `TODO.md`.

### Task 2: Factor reusable flashback query session helpers

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `include/fb_entry.h`
- Test: `sql/fb_value_per_call.sql`

- [ ] Extract the current `pg_flashback()` setup/cleanup path into reusable helpers that are not tied to `PG_FUNCTION_ARGS`.
- [ ] Keep the SQL SRF entry working on top of those helpers.
- [ ] Preserve progress reporting, memory limit, spill mode, and cleanup behavior.

### Task 3: Add planner hook and CustomPath registration

**Files:**
- Create: `src/fb_custom_scan.c`
- Create: `include/fb_custom_scan.h`
- Modify: `src/fb_entry.c`
- Modify: `Makefile`

- [ ] Register a `set_rel_pathlist_hook` during extension init.
- [ ] Detect `RTE_FUNCTION` entries that correspond exactly to `pg_flashback(anyelement, text)`.
- [ ] Inject a `CustomPath` for that baserel.
- [ ] Keep unsupported/functionally different RTEs on the default core path.

### Task 4: Plan CustomScan nodes for pg_flashback

**Files:**
- Modify: `src/fb_custom_scan.c`
- Modify: `include/fb_custom_scan.h`

- [ ] Implement `CustomPathMethods`.
- [ ] Convert the chosen path into a `CustomScan`.
- [ ] Use `custom_scan_tlist` to describe the produced row type.
- [ ] Serialize the minimum executor bootstrap data into `custom_private`.
- [ ] Ensure EXPLAIN can show a distinct custom node name.

### Task 5: Execute pg_flashback via CustomScan without tuplestore

**Files:**
- Modify: `src/fb_custom_scan.c`
- Modify: `src/fb_apply.c`
- Modify: `include/fb_apply.h`
- Modify: `src/fb_entry.c`

- [ ] Implement `CreateCustomScanState`, `BeginCustomScan`, `ExecCustomScan`, `EndCustomScan`, and `ReScanCustomScan`.
- [ ] Reuse the existing flashback build/apply pipeline rather than duplicating WAL/replay logic.
- [ ] Fill the executor result slot directly from each `fb_apply_next(...)` result.
- [ ] Handle executor shutdown cleanly on cancel/error.

### Task 6: Add regression and debug coverage

**Files:**
- Create: `sql/fb_custom_scan.sql`
- Create: `expected/fb_custom_scan.out`
- Modify: `Makefile`
- Modify: existing regression SQL/expected as needed

- [ ] Add a regression-visible debug function or EXPLAIN assertion proving `Function Scan` is no longer used for `pg_flashback(...)`.
- [ ] Add coverage for `count(*) FROM pg_flashback(...)`.
- [ ] Add coverage that the planner still falls back safely when the custom path does not apply.

### Task 7: Verify no large temp-file spill on the target query shape

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/核心入口源码导读.md`

- [ ] Build and install with `make clean && make install` if public headers changed.
- [ ] Run focused regressions for flashback/user-surface/progress/custom-scan behavior.
- [ ] Re-run the live query shape on `scenario_oa_12t_50000r.documents`.
- [ ] Confirm plan node is custom, not `Function Scan`.
- [ ] Confirm `base/pgsql_tmp/*` no longer grows massively during the query.
- [ ] Update architecture docs and completion notes with the new execution model.
