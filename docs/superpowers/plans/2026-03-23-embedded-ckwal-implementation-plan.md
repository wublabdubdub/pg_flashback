# Embedded ckwal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove user-facing `ckwal` configuration and embed WAL recovery into `pg_flashback`, with automatic runtime directory initialization under `DataDir/pg_flashback/`.

**Architecture:** Add a runtime directory module that creates and validates `runtime/`, `recovered_wal/`, and `meta/` during extension creation. Replace the current GUC-driven `ckwal` hook with an internal `fb_ckwal` module that restores required WAL segments into `recovered_wal/`, then let the existing `archive_dest + pg_wal` resolver consume those segments transparently.

**Tech Stack:** PostgreSQL extension C (`PGXS`, PG18 baseline), SQL install script, regression tests.

---

## File Structure

- Create: `include/fb_runtime.h`
- Create: `src/fb_runtime.c`
- Create: `include/fb_ckwal.h`
- Create: `src/fb_ckwal.c`
- Modify: `include/fb_guc.h`
- Modify: `src/fb_guc.c`
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Modify: `sql/pg_flashback--0.1.0.sql`
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Test: `sql/fb_runtime_dir.sql`
- Test: `expected/fb_runtime_dir.out`
- Test: `sql/fb_wal_source_policy.sql`
- Test: `expected/fb_wal_source_policy.out`

### Task 1: Runtime Directory Contract

**Files:**
- Create: `include/fb_runtime.h`
- Create: `src/fb_runtime.c`
- Modify: `sql/pg_flashback--0.1.0.sql`
- Test: `sql/fb_runtime_dir.sql`
- Test: `expected/fb_runtime_dir.out`

- [ ] **Step 1: Write the failing test**

Add a regression entry point that exposes runtime directory status, for example:

```sql
SELECT fb_runtime_dir_debug();
```

Expected shape:

```text
base=.../pg_flashback runtime=true recovered=true meta=true
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
make clean && make install
su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 make installcheck'
```

Expected: test fails because `fb_runtime_dir_debug()` does not exist.

- [ ] **Step 3: Write minimal implementation**

Add `fb_runtime.c` / `fb_runtime.h` with:

- `fb_runtime_base_dir()`
- `fb_runtime_runtime_dir()`
- `fb_runtime_recovered_wal_dir()`
- `fb_runtime_meta_dir()`
- `fb_runtime_ensure_initialized()`

Implementation requirements:

- base dir is `DataDir/pg_flashback`
- use `stat(2)` / `mkdir(2)` via PostgreSQL-safe wrappers
- create `runtime/`, `recovered_wal/`, `meta/`
- error if any directory cannot be created or is not writable

Expose a temporary debug SQL function from `fb_entry.c` or SQL install script.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
make clean && make install
su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 make installcheck'
```

Expected: runtime-dir regression passes.

- [ ] **Step 5: Commit**

```bash
git add include/fb_runtime.h src/fb_runtime.c sql/pg_flashback--0.1.0.sql sql/fb_runtime_dir.sql expected/fb_runtime_dir.out
git commit -m "feat: add pg_flashback runtime directory initialization"
```

### Task 2: Remove User-Facing ckwal GUC

**Files:**
- Modify: `include/fb_guc.h`
- Modify: `src/fb_guc.c`
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Write the failing test**

Extend a regression to assert old user-facing `ckwal` configuration is no longer part of the supported interface. For example, documentation/status checks plus SQL smoke verifying normal runtime still works without those GUCs.

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
make clean && make install
su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 make installcheck'
```

Expected: docs/tests still reference old `ckwal` GUCs.

- [ ] **Step 3: Write minimal implementation**

Remove:

- `pg_flashback.ckwal_restore_dir`
- `pg_flashback.ckwal_command`

Keep:

- `pg_flashback.archive_dest`
- `pg_flashback.archive_dir`
- `pg_flashback.debug_pg_wal_dir`
- `pg_flashback.memory_limit_kb`

Update docs to say:

- `ckwal` is internal
- user no longer configures recovery directory/command

- [ ] **Step 4: Run test to verify it passes**

Run the same installcheck command.

Expected: all existing tests still pass after GUC removal.

- [ ] **Step 5: Commit**

```bash
git add include/fb_guc.h src/fb_guc.c README.md PROJECT.md STATUS.md TODO.md
git commit -m "refactor: remove user-facing ckwal gucs"
```

### Task 3: Internal fb_ckwal Skeleton

**Files:**
- Create: `include/fb_ckwal.h`
- Create: `src/fb_ckwal.c`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing test**

Add a narrow unit-style regression/debug path that calls internal recovery through a SQL wrapper or existing source-debug path for a known mismatched segment fixture.

- [ ] **Step 2: Run test to verify it fails**

Run installcheck and confirm the new path fails because `fb_ckwal` does not exist.

- [ ] **Step 3: Write minimal implementation**

Provide:

- `bool fb_ckwal_restore_segment(TimeLineID tli, XLogSegNo segno, int wal_seg_size, char *out_path, Size out_path_len);`

Initial skeleton behavior:

- ensure runtime dirs exist
- compute destination path under `recovered_wal/`
- return false if not recoverable yet

Do not implement full recovery algorithm in this task; only stabilize interface and file contract.

- [ ] **Step 4: Run test to verify it passes**

Run installcheck and confirm the skeleton path now executes cleanly.

- [ ] **Step 5: Commit**

```bash
git add include/fb_ckwal.h src/fb_ckwal.c Makefile
git commit -m "feat: add internal fb_ckwal interface skeleton"
```

### Task 4: Wire WAL Resolver to Internal Recovery

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Modify: `sql/fb_wal_source_policy.sql`
- Modify: `expected/fb_wal_source_policy.out`

- [ ] **Step 1: Write the failing test**

Use the existing source-policy fixture to assert:

- overlap still prefers `archive_dest`
- mismatched `pg_wal` attempts internal recovery
- error text no longer tells the user to configure `ckwal`

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
make clean && make install
su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 make installcheck'
```

Expected: old error model still mentions missing user-configured `ckwal`.

- [ ] **Step 3: Write minimal implementation**

In `src/fb_wal.c`:

- replace `fb_try_ckwal_segment()` implementation so it calls `fb_ckwal_restore_segment()`
- read recovered files from `DataDir/pg_flashback/recovered_wal/`
- keep `archive_dest` first and `pg_wal` second
- on failure, emit an internal-recovery-specific error, not a user-configuration error

- [ ] **Step 4: Run test to verify it passes**

Run installcheck again.

Expected: source-policy test passes with the new wording and behavior.

- [ ] **Step 5: Commit**

```bash
git add include/fb_wal.h src/fb_wal.c sql/fb_wal_source_policy.sql expected/fb_wal_source_policy.out
git commit -m "refactor: wire wal resolver to internal ckwal recovery"
```

### Task 5: Move Initialization to Extension Lifecycle

**Files:**
- Modify: `sql/pg_flashback--0.1.0.sql`
- Modify: `src/fb_entry.c`
- Modify: `src/fb_runtime.c`
- Test: `sql/fb_smoke.sql`
- Test: `expected/fb_smoke.out`

- [ ] **Step 1: Write the failing test**

Add a smoke expectation that runtime directory initialization happens automatically before flashback use.

- [ ] **Step 2: Run test to verify it fails**

Run installcheck and confirm the extension does not yet eagerly initialize runtime state.

- [ ] **Step 3: Write minimal implementation**

Choose one clear contract and document it:

- either call `fb_runtime_ensure_initialized()` from an extension-invoked SQL function that runs immediately after install
- or call it on first extension function use, but still provide a `CREATE EXTENSION`-time validation helper

Recommended: add a tiny install-time validation helper in SQL script and keep a defensive runtime check in entrypoints.

- [ ] **Step 4: Run test to verify it passes**

Run installcheck again.

Expected: smoke still passes and runtime dir is present.

- [ ] **Step 5: Commit**

```bash
git add sql/pg_flashback--0.1.0.sql src/fb_entry.c src/fb_runtime.c sql/fb_smoke.sql expected/fb_smoke.out
git commit -m "feat: initialize runtime storage during extension lifecycle"
```

### Task 6: Full fb_ckwal Recovery Logic

**Files:**
- Modify: `src/fb_ckwal.c`
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Test: `tests/deep/bin/run_batch_d.sh`
- Test: `docs/reports/2026-03-23-deep-pilot-report.md`

- [ ] **Step 1: Write the failing test**

Use the existing deep batch D mismatch scenario as the correctness target:

- mismatched `pg_wal`
- archive missing segment
- internal recovery produces recovered segment
- flashback proceeds

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
bash tests/deep/bin/run_batch_d.sh --pilot
```

Expected: internal recovery path is still stubbed or incomplete.

- [ ] **Step 3: Write minimal implementation**

Port the smallest viable subset of `/root/xman` `ckwal` logic into `src/fb_ckwal.c`:

- identify target segment
- reconstruct valid segment bytes into `recovered_wal/`
- validate recovered segment before returning success

Keep the write scope focused:

- no extra product features
- no user-visible GUCs
- no unrelated refactors

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
make clean && make install
su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 make installcheck'
bash tests/deep/bin/run_batch_d.sh --pilot
```

Expected:

- regressions pass
- batch D passes using internal recovery

- [ ] **Step 5: Commit**

```bash
git add src/fb_ckwal.c README.md PROJECT.md STATUS.md TODO.md tests/deep/bin/run_batch_d.sh docs/reports/2026-03-23-deep-pilot-report.md
git commit -m "feat: embed ckwal recovery into pg_flashback"
```
