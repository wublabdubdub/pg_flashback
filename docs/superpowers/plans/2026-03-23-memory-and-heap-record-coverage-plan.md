# Memory And Heap Record Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the current flashback peak memory footprint with low-risk changes and close the remaining PG18 heap WAL coverage gaps without changing user-visible semantics.

**Architecture:** Keep replay semantics unchanged while replacing hot-path allocation patterns: move `RecordRef` payload bytes into an append-only arena, spill cold TOAST retired chunks out of memory, and remove redundant replay tuple copies. At the same time, extend heap WAL handling so speculative abort delete is replayed correctly, `HEAP_LOCK/PRUNE_*` move from conservative no-op to page-safe minimal mutation, and `NEW_CID` is documented and classified as PG18 redo no-op rather than an unhandled page mutation.

**Tech Stack:** PostgreSQL 18 server headers, PGXS extension C, regression tests via `make installcheck`

---

### Task 1: Register Scope In Docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Add the chosen memory optimization order `1 -> 3 -> 4`
- [ ] Record that `XLH_DELETE_IS_SUPER` will be supported for page replay but excluded from user-visible forward ops
- [ ] Record that `XLOG_HEAP2_NEW_CID` is a PG18-recognized redo no-op, not a remaining page replay gap

### Task 2: Write Regression Coverage First

**Files:**
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`
- Modify: `sql/fb_replay.sql`
- Modify: `expected/fb_replay.out`

- [ ] Add a speculative insert abort workload that generates `XLH_INSERT_IS_SPECULATIVE` + `XLH_DELETE_IS_SUPER`
- [ ] Run `make installcheck REGRESS=fb_recordref fb_replay` and verify the new expectations fail before code changes

### Task 3: Reduce WAL Payload Allocation Overhead

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`

- [ ] Add an append-only byte arena owned by `FbWalRecordIndex`
- [ ] Route `main_data`, block data, and FPI image copies through the arena instead of one `palloc` per payload
- [ ] Preserve existing memory-limit enforcement semantics while reducing allocator fragmentation/overhead

### Task 4: Spill Cold TOAST Retired Chunks

**Files:**
- Modify: `include/fb_toast.h`
- Modify: `src/fb_toast.c`
- Modify: `src/fb_replay.c`

- [ ] Add file-backed storage for retired TOAST chunks under `DataDir/pg_flashback/runtime`
- [ ] Keep live TOAST chunks in memory, but move retired chunks out of RAM on remove
- [ ] Make reconstruction read spilled retired chunks transparently without changing visible row reconstruction semantics

### Task 5: Remove Redundant Replay Tuple Copies

**Files:**
- Modify: `src/fb_replay.c`

- [ ] Change `fb_append_forward_*` to take ownership of already-copied tuples instead of copying them again
- [ ] Keep finalize-time TOAST rewrite and row identity generation behavior unchanged
- [ ] Re-run targeted replay tests to prove no user-visible diff

### Task 6: Close Remaining Heap WAL Gaps

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_replay.c`
- Modify: `TODO.md`
- Modify: `STATUS.md`

- [ ] Replay `XLH_DELETE_IS_SUPER` records for page-state correctness
- [ ] Ensure speculative insert/delete records do not become user-visible forward ops
- [ ] Upgrade `XLOG_HEAP2_PRUNE_*` replay from pure no-op to minimal page mutation using PG18 prune helpers
- [ ] Keep `XLOG_HEAP2_NEW_CID` classified as recognized redo no-op and remove it from the “unhandled replay” bucket

### Task 7: Verify Fresh

**Files:**
- None

- [ ] Run `make install` or `make clean && make install` if headers changed materially
- [ ] Run `make installcheck REGRESS=fb_recordref fb_replay fb_toast_flashback`
- [ ] Run full `make installcheck`
- [ ] Update `STATUS.md` with commands and observed results
