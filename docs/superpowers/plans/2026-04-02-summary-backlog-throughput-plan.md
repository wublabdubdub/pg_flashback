# Summary Backlog Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Increase summary prebuild throughput for historical backlog and expose an estimated completion timestamp in `pg_flashback_summary_progress`.

**Architecture:** Rebalance the summary service so only one worker primarily follows the hot frontier while the remaining workers prioritize cold backlog, reduce queue churn with batch cold claims, and cut per-segment builder CPU by deduplicating during scan instead of sorting at flush time. Extend the progress view with an ETA derived from recent build throughput.

**Tech Stack:** PostgreSQL extension C, PGXS SQL regression, background workers, shared memory queue state

---

### Task 1: Update docs and decisions before code

**Files:**
- Create: `docs/superpowers/specs/2026-04-02-summary-backlog-throughput-design.md`
- Create: `docs/decisions/ADR-0025-summary-backlog-throughput-and-eta.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Record the approved design**

Write the design, ADR, and task tracking updates before any production code changes.

- [ ] **Step 2: Verify docs mention both throughput and ETA**

Check that:
- backlog throughput is explicitly prioritized over recent latency
- `estimated_completion_at` semantics are documented as best-effort

### Task 2: Add failing regression coverage for the new progress field

**Files:**
- Modify: `sql/fb_summary_service.sql`
- Modify: `expected/fb_summary_service.out`

- [ ] **Step 1: Add a regression assertion for the new column**

Update the summary service SQL regression to expect one more user-facing column and assert that `estimated_completion_at` is present.

- [ ] **Step 2: Add a regression assertion for nullable ETA behavior**

Add a check that when no usable recent build rate exists, the field can be `NULL`.

- [ ] **Step 3: Run the targeted regression to verify RED**

Run: `make installcheck REGRESS=fb_summary_service`

Expected: FAIL because the installed SQL/C tuple descriptor still exposes only the old progress shape.

### Task 3: Rebalance summary service scheduling and batch claiming

**Files:**
- Modify: `src/fb_summary_service.c`

- [ ] **Step 1: Encode worker queue preference**

Implement one hot-first worker and the remaining workers as cold-first, while allowing fallback to the other queue when the preferred queue is empty.

- [ ] **Step 2: Add batch cold claims**

Allow cold-first workers to claim a small batch of contiguous cold tasks in one shared-lock pass.

- [ ] **Step 3: Track recent build throughput samples**

Persist enough shared stats to compute a recent build rate for ETA reporting.

- [ ] **Step 4: Run targeted regression**

Run: `make installcheck REGRESS=fb_summary_service`

Expected: progress view shape is now correct; existing queue invariants still pass.

### Task 4: Remove builder hot-path duplicate work

**Files:**
- Modify: `src/fb_summary.c`

- [ ] **Step 1: Replace append-then-sort touched xid handling**

Deduplicate touched xids while scanning instead of pushing all duplicates into a growable array.

- [ ] **Step 2: Replace append-then-sort block anchor handling**

Keep only the latest anchor per `(relation, block)` while scanning the segment.

- [ ] **Step 3: Deduplicate unsafe facts during scan**

Reduce end-of-segment sort/dedup work by folding duplicate unsafe facts into a hash set.

- [ ] **Step 4: Run targeted regression**

Run: `make installcheck REGRESS=fb_summary_service fb_summary_v3`

Expected: PASS with no behavioral regressions in progress or summary content.

### Task 5: Wire SQL surface and verify end-to-end

**Files:**
- Modify: `sql/pg_flashback--0.1.0.sql`
- Modify: `src/fb_summary_service.c`
- Modify: `expected/fb_summary_service.out`

- [ ] **Step 1: Expose `estimated_completion_at`**

Extend the C SRF tuple materialization and extension SQL definition for the new column.

- [ ] **Step 2: Rebuild and run focused verification**

Run: `make installcheck REGRESS=fb_summary_service fb_summary_v3`

Expected: PASS

- [ ] **Step 3: Run broader safety verification**

Run: `make installcheck REGRESS=fb_progress fb_wal_parallel_payload`

Expected: PASS
