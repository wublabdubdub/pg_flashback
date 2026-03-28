# Memory Vs Disk Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add preflight working-set estimation so `pg_flashback` can stop early when the estimated workload exceeds `pg_flashback.memory_limit`, unless the user explicitly allows disk spill via `pg_flashback.spill_mode`.

**Architecture:** Keep `pg_flashback.memory_limit` as the only memory budget parameter. Add a separate execution-policy GUC, `pg_flashback.spill_mode`, and evaluate a preflight estimate between WAL scan and replay. Reuse existing spool/spill behavior when `spill_mode='disk'`; otherwise fail early with a clear error and hint.

**Tech Stack:** PostgreSQL extension C, PGXS regressions, Markdown docs

---

### Task 1: Register The New User Surface

**Files:**
- Modify: `src/fb_guc.c`
- Modify: `include/fb_guc.h`
- Test: `sql/fb_guc_defaults.sql`
- Test: `expected/fb_guc_defaults.out`

- [ ] **Step 1: Write the failing regression**

Add `pg_flashback.spill_mode` coverage to `sql/fb_guc_defaults.sql` for default value, valid values, and one invalid value.

- [ ] **Step 2: Run the regression to verify it fails**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults'`

Expected: FAIL because `pg_flashback.spill_mode` does not exist yet.

- [ ] **Step 3: Implement the minimal GUC support**

Add enum-style parsing/getter support in `src/fb_guc.c` and declarations in `include/fb_guc.h`.

- [ ] **Step 4: Re-run the regression**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults'`

Expected: PASS for the new GUC surface.

### Task 2: Add A Preflight Estimate Model

**Files:**
- Modify: `include/fb_wal.h`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_entry.c`
- Test: new targeted SQL regression for preflight behavior

- [ ] **Step 1: Write the failing regression**

Create a regression that sets a very small `pg_flashback.memory_limit`, keeps `spill_mode='auto'`, and expects an early preflight error before replay/apply.

- [ ] **Step 2: Run it to verify it fails for the intended reason**

Run the new targeted regression only.

Expected: FAIL because no preflight estimate exists yet.

- [ ] **Step 3: Add estimate fields to the WAL/index context**

Expose enough stats from WAL scan/index build to support a conservative working-set estimate.

- [ ] **Step 4: Implement preflight decision logic in `src/fb_entry.c`**

Evaluate:
- estimate
- current `pg_flashback.memory_limit`
- current `pg_flashback.spill_mode`

Choose continue vs. fail-early there.

- [ ] **Step 5: Re-run the targeted regression**

Expected: PASS with the early error.

### Task 3: Wire Disk-Allowed Mode To Existing Spill

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `src/fb_reverse_ops.c`
- Test: regression SQL/expected for `spill_mode='disk'`

- [ ] **Step 1: Write the failing regression**

Reuse the same high-estimate workload but set `pg_flashback.spill_mode = 'disk'`.

- [ ] **Step 2: Run it to verify failure first**

Expected: FAIL because preflight currently does not distinguish allow-disk mode.

- [ ] **Step 3: Implement the minimal allow-disk path**

When estimate exceeds `memory_limit` but `spill_mode='disk'`, continue and rely on existing reverse-op spill behavior.

- [ ] **Step 4: Add a user-visible notice/debug message**

Emit a clear message that the estimate exceeded the memory budget and the query is continuing with disk spill allowed.

- [ ] **Step 5: Re-run the targeted regression**

Expected: PASS and return the expected rows.

### Task 4: Keep Memory-Only Mode Strict

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `expected/...` for the new preflight regression

- [ ] **Step 1: Add `spill_mode='memory'` coverage to the preflight regression**

Expected behavior: fail early with a memory-only specific error.

- [ ] **Step 2: Run it and verify it fails first**

- [ ] **Step 3: Implement the memory-only error variant**

- [ ] **Step 4: Re-run the regression**

Expected: PASS.

### Task 5: Update Existing Diagnostics And Docs

**Files:**
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/reverse-op-stream.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Rename the user explanation from “memory only” to “memory budget + spill mode”**

- [ ] **Step 2: Document that stage 8 is not the first spill decision point**

- [ ] **Step 3: Document the new `spill_mode` semantics and preflight estimate limitations**

- [ ] **Step 4: Mark the task complete in `STATUS.md` and `TODO.md` after tests pass**

### Task 6: Verify End-To-End

**Files:**
- Modify: any of the above as needed

- [ ] **Step 1: Run full rebuild because headers/GUC surfaces changed**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config clean`

- [ ] **Step 2: Install fresh**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config install`

- [ ] **Step 3: Run focused regressions**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_guc_defaults fb_memory_limit fb_spill <new_preflight_regress>'`

Expected: all pass.

- [ ] **Step 4: Run one manual psql validation**

Verify:
- old `memory_limit` semantics still work
- `spill_mode=auto` blocks when estimate exceeds budget
- `spill_mode=disk` allows the same query to continue
