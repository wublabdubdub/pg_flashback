# Summary Xid Gap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the residual `summary_xid_fallback` gap that still forces `run_flashback_checks` to fall back to WAL for a small unresolved xid tail.

**Architecture:** First restore observability by fixing the regression-only xid resolution debug path and aligning touched-xid collection semantics between summary build and query-time WAL scanning. Then use that observability to identify the exact xid outcome omission class and patch summary build/query resolution so unresolved xid tails no longer force wide WAL fallback.

**Tech Stack:** PostgreSQL extension C code, PGXS regression SQL, release_gate manual verification

---

### Task 1: Restore Xid Gap Observability

**Files:**
- Modify: `src/fb_wal.c`
- Test: `sql/fb_recordref.sql`
- Test: `expected/fb_recordref.out`

- [ ] **Step 1: Write the failing test**

Add a regression assertion that the xid-resolution debug path returns text successfully on a wider xid-resolution case, instead of crashing the backend.

- [ ] **Step 2: Run test to verify it fails**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: FAIL or backend crash on the widened xid-resolution debug case.

- [ ] **Step 3: Write minimal implementation**

Fix the regression-only xid-resolution debug path so it can safely print unresolved touched/unsafe xid samples on large cases.

- [ ] **Step 4: Run test to verify it passes**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: PASS

### Task 2: Align Touched Xid Semantics

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `src/fb_wal.c`
- Test: `sql/fb_recordref.sql`
- Test: `expected/fb_recordref.out`

- [ ] **Step 1: Write the failing test**

Add assertions that summary-seeded and WAL-scanned xid tracking use the same raw/top xid semantics for subtransaction-heavy cases.

- [ ] **Step 2: Run test to verify it fails**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: FAIL on mismatched xid tracking assertions.

- [ ] **Step 3: Write minimal implementation**

Unify touched-xid collection semantics between summary build and query-time WAL metadata collection.

- [ ] **Step 4: Run test to verify it passes**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: PASS

### Task 3: Fix Residual Summary Xid Omissions

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `src/fb_wal.c`
- Test: `sql/fb_recordref.sql`
- Test: `expected/fb_recordref.out`

- [ ] **Step 1: Write the failing test**

Add a regression that reproduces the xid outcome class currently left unresolved after summary exact lookup.

- [ ] **Step 2: Run test to verify it fails**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: FAIL with nonzero unresolved xid / fallback counters.

- [ ] **Step 3: Write minimal implementation**

Patch summary build and/or query resolution so the reproduced xid class is resolved from summary without WAL fallback.

- [ ] **Step 4: Run test to verify it passes**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: PASS with zero residual summary xid fallback for the targeted case.

### Task 4: Verify Release Gate Impact

**Files:**
- Modify: `open_source/pg_flashback/src/*` if source changed
- Modify: `open_source/pg_flashback/include/*` if headers changed
- Modify: `open_source/pg_flashback/sql/*` if SQL regressions changed
- Modify: `open_source/pg_flashback/expected/*` if expected output changed

- [ ] **Step 1: Build and install**

Run: `make clean && make install`

- [ ] **Step 2: Run focused regression**

Run: `PGHOST=/tmp PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_recordref'`
Expected: PASS

- [ ] **Step 3: Re-run release gate focal stage**

Run: `cd tests/release_gate/bin && ./run_release_gate.sh --from run_flashback_checks`
Expected: `3/9 55% xact-status` no longer spends tens of seconds on a tiny unresolved xid tail.

- [ ] **Step 4: Sync open source mirror**

Run: `./scripts/sync_open_source.sh`
Expected: `open_source/pg_flashback/` stays in sync with `src/`, `include/`, `sql/`, and `expected/`.
