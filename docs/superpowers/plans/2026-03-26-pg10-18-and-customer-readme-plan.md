# PG10-18 Compatibility And Customer README Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `pg_flashback` build against PostgreSQL 10-18, rewrite `README.md` into a customer-facing usage manual, and update the four core development documents to the new version/support baseline.

**Architecture:** Keep the flashback execution path unchanged and isolate cross-version differences inside `fb_compat`. Update documentation first to record the new workstream, then drive compatibility work with a build matrix using the local `PG12-18` installations as the executable verification set while documenting `PG10/11` as source-compatible but not locally verified.

**Tech Stack:** PostgreSQL C extension (`PGXS`), PostgreSQL server headers 10-18, SQL regression tests, markdown project docs.

---

### Task 1: Record The New Workstream In Core Docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Update the documents before code changes**

Record that the current workstream is:
- customer-facing README rewrite
- `PG10-18` compatibility layer extraction
- local verification matrix `PG12-18`
- `PG10/11` pending environment verification

- [ ] **Step 2: Verify the updated docs match the requested scope**

Run:
```bash
rg -n "PG10-18|PG12-18|README|客户|compat" STATUS.md TODO.md PROJECT.md docs/architecture/overview.md
```

Expected:
- the four documents mention the new support matrix and documentation refresh

### Task 2: Reproduce The Compatibility Failures First

**Files:**
- No file edits in this task

- [ ] **Step 1: Run the failing PG12 build**

Run:
```bash
make clean && make PG_CONFIG=/home/12pg/local/bin/pg_config -j2
```

Expected:
- FAIL with missing `storage/relfilelocator.h` / `RelFileLocator` baseline mismatch

- [ ] **Step 2: Run the failing PG16 build**

Run:
```bash
make clean && make PG_CONFIG=/home/16pg/local/bin/pg_config -j2
```

Expected:
- FAIL with missing `INT_MAX` declaration in `src/fb_guc.c`

### Task 3: Implement The Cross-Version Compatibility Layer

**Files:**
- Modify: `include/fb_compat.h`
- Modify: `include/fb_common.h`
- Modify: `src/fb_catalog.c`
- Modify: `src/fb_guc.c`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_replay.c`
- Modify: `Makefile`
- Rename/replace: `src/fb_compat_pg18.c` -> `src/fb_compat.c`

- [ ] **Step 1: Add compatibility typedefs and wrappers**

Implement:
- relation locator typedef/macro shims for `RelFileNode` vs `RelFileLocator`
- relation locator field accessor for `rd_node` vs `rd_locator`
- WAL block tag wrapper for pre-15 vs 15+
- `pg_index` key attribute count wrapper for `PG10`
- any header compatibility includes required by these shims

- [ ] **Step 2: Convert call sites to use `fb_compat` wrappers**

Update source files so version-sensitive code only depends on `fb_compat` helpers.

- [ ] **Step 3: Remove the PG18-only build assumption**

Set `Makefile` to default `PG_CONFIG` to the active environment instead of `/home/18pg/local/bin/pg_config`.

### Task 4: Verify The Build Matrix

**Files:**
- No file edits in this task unless a new root cause is discovered

- [ ] **Step 1: Run the local build matrix**

Run:
```bash
for v in 12 13 14 15 16 17 18; do
  make clean >/dev/null 2>&1
  make PG_CONFIG=/home/${v}pg/local/bin/pg_config -j2 || exit 1
done
```

Expected:
- all local builds pass

- [ ] **Step 2: Record the verification result in docs**

Update the core docs and README with:
- supported source range `PG10-18`
- locally verified range `PG12-18`

### Task 5: Rewrite The Customer README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace developer handoff content with customer guidance**

Cover:
- product positioning
- supported PostgreSQL versions and verification statement
- prerequisites
- installation
- required runtime configuration
- supported query pattern
- result semantics
- unsupported scenarios
- operational checks and troubleshooting

- [ ] **Step 2: Remove internal-only content**

Delete:
- `/root/pduforwm` references
- local developer handoff instructions
- deep test campaign narration
- PG18-only workstation assumptions

### Task 6: Final Verification

**Files:**
- No file edits unless verification reveals a defect

- [ ] **Step 1: Run the final local verification commands**

Run:
```bash
make clean && make PG_CONFIG=/home/18pg/local/bin/pg_config -j2
make clean && make PG_CONFIG=/home/12pg/local/bin/pg_config -j2
for v in 13 14 15 16 17 18; do
  make clean >/dev/null 2>&1
  make PG_CONFIG=/home/${v}pg/local/bin/pg_config -j2 || exit 1
done
```

Expected:
- all local builds pass

- [ ] **Step 2: Summarize exact coverage honestly**

Report:
- code/build adapted for `PG10-18`
- local compilation verified on `PG12-18`
- `PG10/11` not verified because no local toolchain is available in this environment
