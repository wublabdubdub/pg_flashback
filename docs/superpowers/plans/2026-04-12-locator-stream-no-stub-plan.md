# Locator Stream No-Stub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the full locator-cover `locator-only payload stub` spool path so `3/9 build record index` can consume summary payload locators directly without writing and re-reading millions of stub / stats sidecar records.

**Architecture:** Keep `summary payload locator-first` as the selection model, but replace “write locator stubs to record spool + worker stats sidecars + leader merge” with a query-local locator stream owned by `FbWalRecordIndex` and consumed directly by `FbWalRecordCursor`. Parallel stats-only workers must return only aggregate counters / missing-block facts, not tempfile logs. Existing fallback window, deferred payload, replay, and apply semantics stay unchanged.

**Tech Stack:** PostgreSQL C extension, PGXS regression tests, `pg_flashback` WAL/replay pipeline

---

### Task 1: Document The Change

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `docs/decisions/ADR-0038-locator-stream-no-stub-sidecars.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Update status/todo/ADR before code**

Record the root cause from the 14pg live case and the approved direction: direct locator stream, no stub spool, no worker stats sidecars in the full locator-cover path.

- [ ] **Step 2: Update architecture overview**

Describe the new `fb_wal` responsibility boundary so future work does not reintroduce query-hot-path tempfiles for locator-only cases.

### Task 2: Add RED Contract Coverage

**Files:**
- Modify: `src/fb_wal.c`
- Create: `sql/fb_wal_locator_stream.sql`
- Create: `expected/fb_wal_locator_stream.out`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing contract test**

Add a regression-only C helper that exercises the full locator-cover path and reports whether:
- record head spool item count stays `0`
- locator stream count matches the synthetic locator count
- stats-only worker path does not require sidecar spool files

- [ ] **Step 2: Run test to verify it fails**

Run: `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS=fb_wal_locator_stream PGUSER=14pg`

Expected: FAIL because the current implementation still appends locator stubs and uses stats/missing sidecar tempfiles.

### Task 3: Replace Stub Spool With Locator Stream

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_entry.c`
- Modify: `src/fb_replay.c`

- [ ] **Step 1: Add index/cursor fields for direct locator streaming**

Store the stable locator slice and counts on `FbWalRecordIndex`, and teach `FbWalRecordCursor` to iterate locator entries as a virtual head stream before `record_tail_log`.

- [ ] **Step 2: Remove stub append from full locator-cover build path**

In `fb_wal_build_record_index()`, when locator coverage is complete and fallback windows are empty, stop calling `fb_index_append_locator_stub()` and instead publish the locator slice onto the index.

- [ ] **Step 3: Rewrite stats-only parallel path to return memory-only results**

Change `fb_wal_capture_locator_stub_stats_parallel()` and worker plumbing so workers update counters / missing-block data in DSM-owned task state instead of writing `wal-payload-stats-worker-*.bin` and `wal-payload-missing-worker-*.bin`.

- [ ] **Step 4: Keep replay/apply behavior stable**

Ensure `fb_wal_record_cursor_seek()`, forward/backward scans, and materializer reuse still work when the head stream is virtual locators rather than spool records.

### Task 4: Verify

**Files:**
- Modify: `open_source/pg_flashback/src/fb_wal.c`
- Modify: `open_source/pg_flashback/include/fb_wal.h`
- Modify: `open_source/pg_flashback/sql/fb_wal_locator_stream.sql`
- Modify: `open_source/pg_flashback/expected/fb_wal_locator_stream.out`

- [ ] **Step 1: Run focused regressions**

Run:
- `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS=fb_wal_locator_stream PGUSER=14pg`
- `make PG_CONFIG=/home/14pg/local/bin/pg_config installcheck REGRESS=fb_replay_final_progress PGUSER=14pg`

Expected: PASS

- [ ] **Step 2: Re-run the target 14pg SQL**

Run the original SQL on `alldb` and capture stage timings plus `perf/gdb` evidence if it still exceeds the target.

Expected: `3/9 payload` no longer dominated by tempfile `pwrite/read`, and total runtime moves toward `< 60s`.

- [ ] **Step 3: Sync open-source mirror**

Run: `./scripts/sync_open_source.sh`

- [ ] **Step 4: Final verification**

Re-run the focused regressions after sync and record the final timing evidence in `STATUS.md`.
