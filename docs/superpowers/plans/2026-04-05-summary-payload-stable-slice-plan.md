# Summary Payload Stable Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert summary payload locators from query-time repeated sort/lookup work into build/cache-time stable slices, and deduplicate payload-plan segment lookup so the heavy `documents` live case no longer stalls in `3/9`.

**Architecture:** Keep the one-segment-one-summary model, but strengthen payload locator semantics from raw append-only section data into relation-scoped stable slices. Query cache materializes public locator slices once per relation entry, and payload planning reads each covered segment at most once before falling back only for uncovered segments.

**Tech Stack:** PostgreSQL C extension, PG10-18 WAL reader compatibility, versioned summary sidecars, pg_regress SQL tests, live `alldb` verification

---

### Task 1: Record the architecture change before code

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Create: `docs/decisions/ADR-0028-summary-payload-locator-stable-slice.md`
- Create: `docs/superpowers/specs/2026-04-05-summary-payload-stable-slice-design.md`
- Create: `docs/superpowers/plans/2026-04-05-summary-payload-stable-slice-plan.md`

- [ ] Step 1: Update status/todo with the new root cause and live case target
- [ ] Step 2: Add ADR-0028 for stable slice + segment dedup semantics
- [ ] Step 3: Update architecture overview to mention build/cache/query responsibilities

### Task 2: Add RED observability for the new contract

**Files:**
- Modify: `sql/fb_payload_scan_mode.sql`
- Modify: `expected/fb_payload_scan_mode.out`
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`

- [ ] Step 1: Add assertions that debug output exposes `summary_payload_locator_segments_read`
- [ ] Step 2: Add assertions that debug output exposes `summary_payload_locator_public_builds`
- [ ] Step 3: Run the affected regressions and confirm they fail before implementation

### Task 3: Build stable locator slices into summary files

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `include/fb_summary.h`

- [ ] Step 1: Sort and deduplicate each relation entry’s payload locators during summary build
- [ ] Step 2: Keep the on-disk slice layout stable under `first_payload_locator/payload_locator_count`
- [ ] Step 3: Bump summary version so stale sidecars are rebuilt

### Task 4: Extend query cache with reusable public locator slices

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `include/fb_summary.h`

- [ ] Step 1: Add relation-scoped public locator slice cache to the summary query cache entry
- [ ] Step 2: Materialize each public slice at most once per relation entry
- [ ] Step 3: Change payload locator lookup to merge matched slices instead of flatten+qsort

### Task 5: Deduplicate payload-plan segment lookup

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`

- [ ] Step 1: Add per-query tracking for unique locator segments read
- [ ] Step 2: Rewrite payload locator plan construction to collect unique covered segments and clipped ranges
- [ ] Step 3: Lookup locators once per segment, keep fallback windows only for uncovered segments
- [ ] Step 4: Keep payload locator output globally ordered and deduplicated

### Task 6: Expose debug counters and keep old diagnostics stable

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `src/fb_entry.c`
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`

- [ ] Step 1: Add `summary_payload_locator_segments_read`
- [ ] Step 2: Add `summary_payload_locator_public_builds`
- [ ] Step 3: Keep existing payload/debug fields stable for current tooling

### Task 7: Verify on regressions and the provided live case

**Files:**
- Modify: `open_source/pg_flashback/src/*`
- Modify: `open_source/pg_flashback/include/*`
- Modify: `open_source/pg_flashback/sql/*`
- Modify: `open_source/pg_flashback/expected/*`

- [ ] Step 1: Run targeted regressions for payload locator and summary paths
- [ ] Step 2: Re-run `fb_recordref_debug()` on `scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
- [ ] Step 3: Re-run `select count(*) from pg_flashback(...)` for the same live case and confirm `3/9` no longer stalls on payload locator planning
- [ ] Step 4: Sync the open source mirror and refresh handoff status
