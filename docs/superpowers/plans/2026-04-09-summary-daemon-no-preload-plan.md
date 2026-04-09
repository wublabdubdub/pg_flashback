# Summary Daemon Without Preload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move summary background service off `shared_preload_libraries` into a standalone `pg_flashback-summaryd` executable without breaking query-side summary usage or SQL progress/debug surfaces.

**Architecture:** Keep query-time summary readers inside the extension, but replace preload bgworker state with daemon-published state files under `DataDir/pg_flashback/meta/summaryd`. Build a standalone executable from the top-level `Makefile` so open-source users get both the extension and the daemon from the same install flow.

**Tech Stack:** PostgreSQL C extension, PGXS, standalone C executable, JSON state files, shell verification

---

### Task 1: Record the architecture change and target file layout

**Files:**
- Create: `docs/specs/2026-04-09-summary-daemon-without-preload-design.md`
- Create: `docs/decisions/ADR-0035-external-summary-daemon-without-preload.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Record the accepted design and ADR**
- [ ] **Step 2: Mark the work as in progress in `STATUS.md` and `TODO.md`**
- [ ] **Step 3: Register the new daemon/module responsibilities in architecture docs**

### Task 2: Add a minimal standalone daemon target

**Files:**
- Modify: `Makefile`
- Create: `summaryd/pg_flashback_summaryd.c`
- Create: `summaryd/README.md`

- [ ] **Step 1: Add a failing shell verification that expects a built `pg_flashback-summaryd --help`**
- [ ] **Step 2: Add a minimal standalone C executable with `--help` / `--version` / argument parsing**
- [ ] **Step 3: Teach the top-level `Makefile` to build and install the daemon into `$(bindir)`**
- [ ] **Step 4: Run the shell verification and confirm it passes**

### Task 3: Introduce a daemon runtime/state directory contract

**Files:**
- Create: `include/fb_summary_state.h`
- Create: `src/fb_summary_state.c`
- Modify: `src/fb_runtime.c`
- Modify: `include/fb_runtime.h`
- Modify: `summaryd/pg_flashback_summaryd.c`

- [ ] **Step 1: Add tests or debug checks for the expected `meta/summaryd` and runtime hint paths**
- [ ] **Step 2: Implement shared helpers for lock/state/hint path construction**
- [ ] **Step 3: Make the daemon create and validate its required directories**
- [ ] **Step 4: Verify paths and lock files behave as expected**

### Task 4: Replace shared-memory SQL surfaces with state-file readers

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `include/fb_summary_service.h`
- Modify: `sql/pg_flashback--0.2.0.sql`
- Modify: `sql/fb_summary_service.sql`
- Modify: `expected/fb_summary_service.out`

- [ ] **Step 1: Write failing regression coverage for “no preload, state file present” SQL surfaces**
- [ ] **Step 2: Implement state-file readers for progress/debug SQL functions**
- [ ] **Step 3: Keep existing view names and null-safe fallback behavior**
- [ ] **Step 4: Run focused regressions and confirm green**

### Task 5: Move recent-query summary observations to runtime hint files

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `src/fb_entry.c`
- Modify: `include/fb_summary_service.h`

- [ ] **Step 1: Write failing coverage for query observation persistence without shared memory**
- [ ] **Step 2: Replace shared-memory report calls with atomic hint-file writes**
- [ ] **Step 3: Verify the daemon can consume the hint contract**

### Task 6: Port launcher/worker scheduling into the daemon

**Files:**
- Create: `summaryd/scheduler.c`
- Create: `summaryd/scheduler.h`
- Create: `summaryd/state_io.c`
- Create: `summaryd/state_io.h`
- Modify: `src/fb_summary.c`
- Modify: `include/fb_summary.h`

- [ ] **Step 1: Extract backend-independent summary build candidate and cleanup helpers**
- [ ] **Step 2: Implement daemon hot/cold scheduling and build loop**
- [ ] **Step 3: Implement state publication and summary cleanup in the daemon**
- [ ] **Step 4: Verify the daemon publishes stable `state.json/debug.json` snapshots**

### Task 7: Update open-source install surface

**Files:**
- Modify: `README.md`
- Modify: `open_source/pg_flashback/README.md`
- Modify: `scripts/sync_open_source.sh`
- Create: `summaryd/pg_flashback-summaryd.service`
- Create: `summaryd/pg_flashback-summaryd.conf.sample`

- [ ] **Step 1: Document that `make` / `make install` also produce/install `pg_flashback-summaryd`**
- [ ] **Step 2: Add a systemd unit template and sample config**
- [ ] **Step 3: Ensure open-source mirror export includes the daemon sources/assets**

### Task 8: Verification

**Files:**
- Modify as needed from earlier tasks

- [ ] **Step 1: Run focused build verification for the daemon executable**
Run: `make PG_CONFIG=/path/to/pg_config`
Expected: `pg_flashback.so` and `pg_flashback-summaryd` both build successfully

- [ ] **Step 2: Run focused SQL regressions**
Run: `make PG_CONFIG=/path/to/pg_config installcheck REGRESS='fb_summary_service fb_summary_v3 fb_summary_prefilter'`
Expected: all selected regressions pass

- [ ] **Step 3: Run a manual smoke test**
Run: `pg_flashback-summaryd --help`
Expected: usage output with `--pgdata` and `--archive-dest`

