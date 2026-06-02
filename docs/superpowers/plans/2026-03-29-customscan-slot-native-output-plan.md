# CustomScan Slot-Native Output Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate executor-side row accumulation for `FROM pg_flashback(...)` by changing CustomScan output from `Datum` re-materialization to direct slot delivery.

**Architecture:** Keep the existing `CustomScan` path and flashback pipeline, but add a slot-native row API from `fb_apply` through `fb_entry` into `fb_custom_scan`. Preserve the existing datum-returning SRF path so plain `pg_flashback()` behavior does not change.

**Tech Stack:** PostgreSQL executor `CustomScan`, tuple table slots, PGXS regressions, `pg_get_backend_memory_contexts()`.

---

### Task 1: Record the design and current work item

**Files:**
- Create: `docs/superpowers/specs/2026-03-29-customscan-slot-native-output-design.md`
- Create: `docs/superpowers/plans/2026-03-29-customscan-slot-native-output-plan.md`
- Modify: `docs/decisions/ADR-0010-custom-scan-for-pg-flashback-from.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Step 1: Record the design and scope boundary before code changes
- [ ] Step 2: Note the architecture delta in ADR/status/todo

### Task 2: Add a failing memory-growth regression

**Files:**
- Modify: `sql/fb_custom_scan.sql`
- Modify: `expected/fb_custom_scan.out`

- [ ] Step 1: Add a regression case that opens a cursor over `FROM pg_flashback(...)`
- [ ] Step 2: Fetch rows in batches and sample `pg_get_backend_memory_contexts()`
- [ ] Step 3: Assert `ExecutorState` growth stays bounded across additional fetches
- [ ] Step 4: Run the regression first and confirm it fails on current code

### Task 3: Implement slot-native apply delivery

**Files:**
- Modify: `include/fb_apply.h`
- Modify: `src/fb_apply.c`

- [ ] Step 1: Add a helper that writes `FbApplyEmit` into a destination slot
- [ ] Step 2: Add `fb_apply_next_slot()` without changing keyed/bag emit semantics
- [ ] Step 3: Keep `fb_apply_next(..., Datum *)` working for the SRF path

### Task 4: Thread slot delivery through query helpers and CustomScan

**Files:**
- Modify: `include/fb_entry.h`
- Modify: `src/fb_entry.c`
- Modify: `src/fb_custom_scan.c`

- [ ] Step 1: Add `fb_flashback_query_next_slot()`
- [ ] Step 2: Update `fb_custom_scan` to pull directly into `ss_ScanTupleSlot`
- [ ] Step 3: Remove `ExecStoreHeapTupleDatum()` from the CustomScan execution path

### Task 5: Verify and document the new model

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/核心入口源码导读.md`

- [ ] Step 1: Run focused regressions for custom scan/value-per-call behavior
- [ ] Step 2: Re-run the new memory-growth regression and confirm it passes
- [ ] Step 3: Summarize the new slot-native output model in status and architecture docs
