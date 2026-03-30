# Summary Prebuild Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a low-write-impact background summary prebuild service so flashback prefilter can consume compact WAL segment summaries before user queries.

**Architecture:** Introduce a segment-summary sidecar plus a shared-memory launcher/worker service that scans archive sources, builds summaries in private worker memory, and keeps `meta/summary` under a size cap. Query-side prefilter switches to summary-first with mmap fallback.

**Tech Stack:** PostgreSQL extension C, static background workers, AddinShmem/LWLock/Latch, WAL reader APIs, PGXS regression tests

---

### Task 1: Document And Lock Decisions

**Files:**
- Create: `docs/decisions/ADR-0019-summary-prebuild-service.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/wal-source-resolution.md`

- [ ] Step 1: Update docs to record the new summary service, query-side summary prefilter, and summary-only cleanup policy
- [ ] Step 2: Verify docs match the approved design and current repo terminology

### Task 2: Add Summary Format And Runtime Paths

**Files:**
- Create: `include/fb_summary.h`
- Create: `src/fb_summary.c`
- Modify: `include/fb_runtime.h`
- Modify: `src/fb_runtime.c`
- Modify: `Makefile`

- [ ] Step 1: Write failing regression coverage for summary debug/probe helpers
- [ ] Step 2: Add runtime path helpers for `meta/summary`
- [ ] Step 3: Define compact summary file format with locator/relid bloom filters
- [ ] Step 4: Add summary load/store/probe APIs
- [ ] Step 5: Run focused regression to confirm file format/debug helpers

### Task 3: Switch Query Prefilter To Summary-First

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Test: `sql/fb_summary_prefilter.sql`
- Test: `expected/fb_summary_prefilter.out`

- [ ] Step 1: Write failing regression proving summary miss falls back and summary hit avoids relation-pattern sidecar growth
- [ ] Step 2: Add query-side summary probe path
- [ ] Step 3: Stop writing new `prefilter-*.meta` sidecars once summary path is active
- [ ] Step 4: Run focused regression

### Task 4: Add Shared Queue And Background Service

**Files:**
- Create: `include/fb_summary_service.h`
- Create: `src/fb_summary_service.c`
- Modify: `src/fb_guc.c`
- Modify: `include/fb_guc.h`
- Modify: `Makefile`

- [ ] Step 1: Add failing regression/debug coverage for queue/service status surfaces
- [ ] Step 2: Define GUCs for service enablement, worker count, scan interval, and summary size limit
- [ ] Step 3: Add AddinShmem state, launcher registration, and worker registration
- [ ] Step 4: Implement fixed-slot queue, task leasing, and worker wake/sleep
- [ ] Step 5: Run focused regression or debug helper validation

### Task 5: Implement Directory Scan And Task Scheduling

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `src/fb_wal.c` or shared helper extraction if needed
- Test: `sql/fb_summary_service.sql`
- Test: `expected/fb_summary_service.out`

- [ ] Step 1: Write failing coverage for “scan archive + pg_wal, queue only missing summaries, archive wins over pg_wal”
- [ ] Step 2: Implement hot-frontier + cold-backfill scan policy
- [ ] Step 3: Implement mismatch skipping / recovered_wal inclusion rules
- [ ] Step 4: Implement private-memory summary build and atomic file publish
- [ ] Step 5: Run focused regression and manual launcher smoke test

### Task 6: Add Summary Cleanup

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `src/fb_runtime.c`
- Test: `sql/fb_summary_cleanup.sql`
- Test: `expected/fb_summary_cleanup.out`

- [ ] Step 1: Write failing coverage for high/low watermark cleanup behavior
- [ ] Step 2: Implement summary-only size accounting and candidate ordering
- [ ] Step 3: Protect recent/active summaries from deletion
- [ ] Step 4: Run focused regression

### Task 7: Verification

**Files:**
- Test: `sql/fb_guc_defaults.sql`
- Test: `expected/fb_guc_defaults.out`
- Test: `sql/fb_recordref.sql`
- Test: `expected/fb_recordref.out`
- Test: `sql/fb_wal_sidecar.sql`
- Test: `expected/fb_wal_sidecar.out`

- [ ] Step 1: Run focused installcheck for all new/updated regressions
- [ ] Step 2: Run related existing regressions affected by GUC/runtime/WAL changes
- [ ] Step 3: Perform manual PG18 validation with `shared_preload_libraries`
- [ ] Step 4: Update docs with final status and verification evidence
