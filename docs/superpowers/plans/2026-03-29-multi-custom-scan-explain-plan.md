# Multi CustomScan Explain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single `FbFlashbackScan` executor black box with a real multi-node `CustomScan` tree so `EXPLAIN ANALYZE` can attribute time across WAL, replay, reverse, and apply stages.

**Architecture:** Build a nested `CustomScan` plan tree rooted at `FbApplyScan`, with one child each for reverse-source finish, replay final, replay warm, replay discover, and WAL index build. Keep SQL SRF execution unchanged; only `FROM pg_flashback(...)` gets the new executor tree.

**Tech Stack:** PostgreSQL planner hooks, `CustomPath` / `CustomScan`, existing `fb_wal` / `fb_replay` / `fb_reverse_ops` / `fb_apply`, PGXS regression tests.

---

### Task 1: Record the architecture change before code

**Files:**
- Create: `docs/decisions/ADR-0014-custom-operator-tree-for-explain-analyze.md`
- Create: `docs/superpowers/specs/2026-03-29-multi-custom-scan-explain-design.md`
- Create: `docs/superpowers/plans/2026-03-29-multi-custom-scan-explain-plan.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Add the accepted decision to replace one `FbFlashbackScan` node with a multi-node `CustomScan` tree.
- [ ] Record the chosen node boundaries and scope.
- [ ] Mark the work as in-progress in `STATUS.md` and `TODO.md`.

### Task 2: Write the failing EXPLAIN regression first

**Files:**
- Modify: `sql/fb_custom_scan.sql`
- Modify: `expected/fb_custom_scan.out`

- [ ] Update the regression expectation from one `Custom Scan (FbFlashbackScan)` to the target multi-node plan shape.
- [ ] Keep the existing temp-spill and executor-growth assertions.
- [ ] Run `installcheck` for `fb_custom_scan` and confirm the old implementation fails against the new expectation.

### Task 3: Split CustomScan planning from one node into a nested path tree

**Files:**
- Modify: `src/fb_custom_scan.c`
- Modify: `include/fb_custom_scan.h`

- [ ] Introduce distinct custom node kinds and names for apply, reverse, replay-final, replay-warm, replay-discover, and wal-index.
- [ ] Build nested `custom_paths` instead of one top-level `FbFlashbackScan` path.
- [ ] Keep fast-path detection attached only to the root apply node.

### Task 4: Expose replay phase boundaries as reusable APIs

**Files:**
- Modify: `include/fb_replay.h`
- Modify: `src/fb_replay.c`

- [ ] Extract public helpers for replay discover, warm, and final phases.
- [ ] Keep `fb_replay_build_reverse_source()` working for the SQL SRF path.
- [ ] Provide cleanup helpers for new replay intermediate states.

### Task 5: Execute the multi-node tree in executor

**Files:**
- Modify: `src/fb_custom_scan.c`
- Modify: `include/fb_custom_scan.h`

- [ ] Implement child-first execution for each barrier node so stage work happens inside `ExecProcNode()`.
- [ ] Make `FbApplyScan` the only row-producing node.
- [ ] Preserve error cleanup, rescan safety, and fast-path behavior.

### Task 6: Re-verify existing memory fixes under the new tree

**Files:**
- Modify: `sql/fb_custom_scan.sql`
- Modify: `expected/fb_custom_scan.out`
- Modify: other regression files if plan names changed

- [ ] Re-run `fb_custom_scan` until the new plan shape passes.
- [ ] Re-run `fb_keyed_fast_path`, `fb_value_per_call`, and `pg_flashback` to verify planner / executor behavior still works.
- [ ] Confirm low `work_mem` queries still avoid temp spill and cursor fetches still keep `ExecutorState` bounded.

### Task 7: Update completion docs after verification

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`

- [ ] Update status, next steps, and architecture overview to reflect the new multi-node executor tree.
- [ ] Record the focused regression commands and outcomes.
