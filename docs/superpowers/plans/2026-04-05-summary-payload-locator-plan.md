# Summary Payload Locator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a relation-scoped payload locator section to segment summaries and consume it during payload materialization so `3/9 payload` no longer spends most of its time decoding unrelated WAL records.

**Architecture:** Keep the existing one-segment-one-summary model and add a new compact summary section that stores payload record entry points per relation slot. Query-side payload first consumes locators to do exact record reads, then falls back to the existing span/window path only for segments not covered by locators.

**Tech Stack:** PostgreSQL C extension, PG10-18 WAL reader compatibility, versioned summary sidecar sections, pg_regress SQL tests

---

### Task 1: Record the architecture change in project docs

**Files:**
- Create: `docs/decisions/ADR-0027-summary-payload-locator-section.md`
- Create: `docs/superpowers/specs/2026-04-05-summary-payload-locator-design.md`
- Create: `docs/superpowers/plans/2026-04-05-summary-payload-locator-plan.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Step 1: Update `STATUS.md` with the new in-progress optimization direction and why span-only summary has reached its limit
- [ ] Step 2: Update `TODO.md` with the payload locator work items and verification target
- [ ] Step 3: Add ADR-0027 to fix the architectural contract before implementation
- [ ] Step 4: Add the design and implementation plan documents

### Task 2: Write RED regressions for summary payload locator coverage

**Files:**
- Modify: `sql/fb_summary_v3.sql`
- Modify: `expected/fb_summary_v3.out`
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`

- [ ] Step 1: Extend `fb_recordref_debug()` assertions so summary-driven payload stats must expose locator counters
- [ ] Step 2: Extend `fb_summary_v3.sql` with a case that proves summary payload locator data is present and used
- [ ] Step 3: Run the affected regressions and confirm they fail before implementation

### Task 3: Extend summary format with payload locator section

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `include/fb_summary.h`

- [ ] Step 1: Bump summary version and add a new section kind/count for payload locators
- [ ] Step 2: Add in-memory and on-disk payload locator structs keyed by relation slot
- [ ] Step 3: Capture locator entries during summary build for every payload-materializable relation record
- [ ] Step 4: Encode and decode the locator section through the existing query cache

### Task 4: Add query-side summary payload locator APIs

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `include/fb_summary.h`

- [ ] Step 1: Add `fb_summary_segment_lookup_payload_locators_cached(...)`
- [ ] Step 2: Return target-relation locator entries in sorted record order
- [ ] Step 3: Keep missing section / old version behavior on safe fallback semantics

### Task 5: Add exact-record payload read path

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`

- [ ] Step 1: Add query-side data structures for summary payload locator candidate streams
- [ ] Step 2: Add a payload materialize path that reads exact WAL records from locator start positions instead of decoding full windows
- [ ] Step 3: Integrate it into `fb_wal_build_record_index()` ahead of the existing sparse/windowed path
- [ ] Step 4: Fall back segment-by-segment to the current window path when locator coverage is missing

### Task 6: Expose observability and keep payload stats correct

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `src/fb_wal.c`
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`

- [ ] Step 1: Add summary payload locator counters to `fb_recordref_debug()`
- [ ] Step 2: Ensure `payload_scanned_records` reflects exact decode work after locator-first execution
- [ ] Step 3: Keep old debug fields stable so existing tests and live diagnostics still compare cleanly

### Task 7: Verify and sync mirror

**Files:**
- Modify: `open_source/pg_flashback/src/*`
- Modify: `open_source/pg_flashback/include/*`
- Modify: `open_source/pg_flashback/sql/*`
- Modify: `open_source/pg_flashback/expected/*`

- [ ] Step 1: Run targeted regressions for summary and payload paths
- [ ] Step 2: Run a live `fb_recordref_debug()` / `pg_flashback()` spot check on the provided `approval_comments` case
- [ ] Step 3: Sync `open_source/pg_flashback/` with `scripts/sync_open_source.sh`
- [ ] Step 4: Update `STATUS.md` / `TODO.md` with verification results and next risks
