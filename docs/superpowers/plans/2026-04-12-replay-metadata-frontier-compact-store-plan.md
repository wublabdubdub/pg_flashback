# Replay Metadata Frontier And Compact Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move replay lifecycle and prune metadata into `3/9`, replace `BlockReplayStore` with a compact query-local store, remove the `6/9` backward prune prepass, and narrow discover to frontier-only apply while keeping `3/9` overhead within `5s`.

**Architecture:** Extend `fb_wal` so the existing payload/index materialization path emits compact replay metadata as a query-local sidecar attached to `FbWalRecordIndex`. Make `fb_replay` consume that metadata to retire block states early, use a compact block store instead of dynahash, and read dense future-block guards directly in final without any extra backward cursor sweep.

**Tech Stack:** PostgreSQL extension C, PG14-18 WAL reader compatibility, PGXS regression SQL, 14pg live `alldb` verification

---

### Task 1: Add replay-metadata contract coverage before implementation

**Files:**
- Modify: `Makefile`
- Create: `sql/fb_replay_metadata_contract.sql`
- Create: `expected/fb_replay_metadata_contract.out`
- Modify: `src/fb_entry.c`
- Modify: `src/fb_replay.c`
- Test: `sql/fb_replay_metadata_contract.sql`

- [ ] Step 1: Add a regression-only SQL entry point that reports whether replay metadata exists, whether final still built a backward prune prepass, and whether block retire counters moved during replay.
- [ ] Step 2: Run the new regression and confirm it fails because the metadata surface is not implemented yet.
- [ ] Step 3: Add minimal debug plumbing only, without changing replay behavior.
- [ ] Step 4: Run the regression again and keep it failing on behavior, not missing symbols.

### Task 2: Emit compact replay metadata from `3/9`

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Modify: `docs/architecture/overview.md`
- Test: `sql/fb_replay_metadata_contract.sql`

- [ ] Step 1: Add query-local replay metadata structures to `FbWalRecordIndex`.
- [ ] Step 2: Emit per-block `last_replay_use_record_index` and `last_prune_guard_index` during the existing payload/index materialization path only.
- [ ] Step 3: Store compact future-block delta in a dense record-index-addressable vector.
- [ ] Step 4: Run the new regression and confirm metadata is now populated.

### Task 3: Replace dynahash `BlockReplayStore` with compact store

**Files:**
- Modify: `include/fb_replay.h`
- Modify: `src/fb_replay.c`
- Test: `sql/fb_replay_block_state_init.sql`
- Test: `sql/fb_replay_metadata_contract.sql`

- [ ] Step 1: Write a failing regression assertion for compact-store retire/lookup behavior.
- [ ] Step 2: Implement compact block store allocation, keying, lookup, and retire paths.
- [ ] Step 3: Switch discover/warm/final to the compact store.
- [ ] Step 4: Re-run regressions until green.

### Task 4: Remove `6/9` backward prune prepass

**Files:**
- Modify: `src/fb_replay.c`
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Test: `sql/fb_replay_prune_lookahead_payload.sql`
- Test: `sql/fb_replay_metadata_contract.sql`

- [ ] Step 1: Write a failing assertion that final no longer builds prune lookahead by sweeping the cursor backward.
- [ ] Step 2: Make final read dense future-block metadata from `FbWalRecordIndex`.
- [ ] Step 3: Delete `fb_replay_build_prune_lookahead()` from the production final path.
- [ ] Step 4: Re-run regressions and verify final still preserves prune correctness contracts.

### Task 5: Narrow discover to frontier apply

**Files:**
- Modify: `src/fb_replay.c`
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Test: `sql/fb_replay_discover_skip_payload.sql`
- Test: `sql/fb_replay_discover_materialize_reuse.sql`
- Test: `sql/fb_replay_metadata_contract.sql`

- [ ] Step 1: Write a failing assertion that discover only materializes/applies records inside the current missing-block frontier closure.
- [ ] Step 2: Implement frontier filtering on top of replay metadata emitted by `3/9`.
- [ ] Step 3: Re-run focused discover regressions until green.

### Task 6: Verify the budget and live target

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Step 1: Run focused PG14 regressions:
  - `fb_replay_block_state_init`
  - `fb_replay_discover_toast`
  - `fb_replay_discover_skip_payload`
  - `fb_replay_discover_materialize_reuse`
  - `fb_replay_prune_lookahead_payload`
  - `fb_replay_metadata_contract`
- [ ] Step 2: Run the 14pg live `alldb` SQL and capture stage timings.
- [ ] Step 3: Confirm `3/9` incremental overhead stays within `<= 5s`.
- [ ] Step 4: Confirm `4/9` and `6/9` both improve materially toward the `60%` goal.
- [ ] Step 5: Update `STATUS.md` / `TODO.md` with measured results and remaining gaps.
