# Summary v3 Segment Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `summary-*.meta` from bloom-only segment summaries into compact segment indexes that store relation spans and xid outcomes, then consume them in query execution to avoid whole-segment decoding and most xid-status refill scans.

**Architecture:** Keep one sidecar per WAL segment. Extend `fb_summary` with a versioned multi-section binary format and readers for relation spans and xid outcomes. Extend `fb_wal` to probe blooms first, then drive window scans from relation spans and satisfy touched xid status from summary outcomes before falling back to WAL scans.

**Tech Stack:** PostgreSQL C extension, PGXS, WAL reader APIs, binary sidecar files, regression tests

---

## File Map

- Modify: `include/fb_summary.h`
  - Add summary v3 reader/writer APIs for relation spans and xid outcomes.
- Modify: `src/fb_summary.c`
  - Define v3 file format, compact encoding, builders, probes, and debug helpers.
- Modify: `src/fb_wal.c`
  - Consume relation spans during relation scan/index build and xid outcomes during status fill.
- Modify: `src/fb_summary_service.c`
  - Preserve service compatibility with v3 generation and stats.
- Modify: `docs/architecture/overview.md`
  - Update module responsibilities and file format description.
- Modify: `STATUS.md`
  - Record accepted design, in-progress implementation, and verification.
- Modify: `TODO.md`
  - Add summary v3 implementation checklist.
- Create: `docs/decisions/ADR-0021-summary-v3-segment-index.md`
  - Record the architecture decision.
- Create: `docs/superpowers/specs/2026-03-31-summary-v3-segment-index-design.md`
  - Formal design spec.
- Create: `sql/fb_summary_v3.sql`
  - Regression for v3 build/consume path.
- Create: `expected/fb_summary_v3.out`
  - Expected output for the regression.

### Task 1: Lock Down Docs And Test Targets

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Create: `docs/decisions/ADR-0021-summary-v3-segment-index.md`
- Create: `docs/superpowers/specs/2026-03-31-summary-v3-segment-index-design.md`

- [ ] **Step 1: Record the accepted design before code changes**

Document:
- why bloom-only summary is insufficient
- why segment-local indexes are chosen
- fallback semantics and correctness constraints

- [ ] **Step 2: Add TODO items for v3 implementation**

Checklist:
- new format
- builder
- relation spans consumption
- xid outcome consumption
- regressions

- [ ] **Step 3: Update architecture overview**

Describe:
- summary v3 sections
- query-side usage
- fallback path

### Task 2: Add Failing Regression Coverage For Summary v3

**Files:**
- Create: `sql/fb_summary_v3.sql`
- Create: `expected/fb_summary_v3.out`
- Modify: `Makefile`

- [ ] **Step 1: Write a regression that proves relation spans reduce scan scope**

Test shape:
- build WAL for two relations in nearby time windows
- generate summary v3
- assert target relation returns correct rows
- assert debug stats show span hits instead of full-segment fallback

- [ ] **Step 2: Write a regression that proves xid outcomes avoid refill scans**

Test shape:
- create touched xid set with commit/abort outcomes
- assert query result correctness
- assert debug stats show xid outcome hits and zero refill fallback when v3 is present

- [ ] **Step 3: Register the regression in `Makefile`**

Run:
`make PG_CONFIG=/home/18pg/local/bin/pg_config install`

Expected:
- build succeeds
- test fails before implementation

### Task 3: Define Summary v3 Binary Format And Readers

**Files:**
- Modify: `include/fb_summary.h`
- Modify: `src/fb_summary.c`

- [ ] **Step 1: Add v3 structs and section directory constants**

Include:
- header
- section descriptor
- relation dictionary entry
- relation spans section metadata
- xid outcome section metadata

- [ ] **Step 2: Add compact encoding helpers**

Implement:
- varint encode/decode
- delta encode/decode
- span merge helper
- compact xid outcome encoder

- [ ] **Step 3: Add sidecar readers**

Implement APIs to:
- load bloom gate
- load relation spans for one target relation
- lookup xid outcomes for a touched xid set

- [ ] **Step 4: Preserve safe fallback**

If any parse or identity check fails:
- surface “summary unavailable”
- do not ERROR for recoverable cache issues

### Task 4: Build Relation Spans And Xid Outcomes During Summary Generation

**Files:**
- Modify: `src/fb_summary.c`

- [ ] **Step 1: Extend build state with relation dictionary and pending spans**

Track:
- relation key to `rel_slot`
- current open span per relation
- xid outcome map

- [ ] **Step 2: Capture relation spans while visiting WAL records**

For every matching WAL record:
- determine relation key
- add/extend span using `ReadRecPtr .. EndRecPtr`

- [ ] **Step 3: Capture xid outcomes from `RM_XACT_ID` records**

Store:
- xid
- status
- commit_ts
- commit_lsn

- [ ] **Step 4: Finalize and write sections**

Serialize:
- dictionary
- merged spans
- xid outcomes
- stats

- [ ] **Step 5: Re-run the new regression to confirm the file format is readable**

Run:
`su - 18pg -c "cd /tmp && /home/18pg/local/lib/postgresql/pgxs/src/test/regress/pg_regress --inputdir=/root/pg_flashback --bindir=/home/18pg/local/bin --dbname=contrib_regression --user=18pg fb_summary_v3"`

Expected:
- still failing on query-path consumption, not on sidecar build/read

### Task 5: Consume Relation Spans In Query-Side WAL Scans

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_summary.h`

- [ ] **Step 1: Add summary APIs that return target relation spans for a segment**

Inputs:
- segment identity
- `FbRelationInfo`

Outputs:
- zero or more `start_lsn/end_lsn` spans

- [ ] **Step 2: Replace full-segment hit handling with span-driven windows**

When v3 spans exist:
- build visit windows from spans
- include minimal neighbor safety when record boundaries need it

- [ ] **Step 3: Preserve old path for missing spans**

If spans unavailable:
- keep current window/segment behavior

- [ ] **Step 4: Run focused regressions**

Run:
- `fb_summary_v3`
- `fb_summary_prefilter`
- `fb_recordref`

Expected:
- pass or fail only on xid refill behavior not yet switched

### Task 6: Consume Xid Outcomes Before Refill WAL Scan

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_summary.h`

- [ ] **Step 1: Add summary outcome probe API for touched xid sets**

The API should:
- accept a touched xid hash/table
- return resolved statuses for any xid found in sidecars covering the query window
- report unresolved count

- [ ] **Step 2: Change xid refill flow to use outcomes first**

Flow:
- probe summary outcomes
- only if unresolved xid remain, run fallback WAL refill

- [ ] **Step 3: Add debug counters**

Expose:
- xid outcomes resolved from summary
- fallback xid count
- spans used count

- [ ] **Step 4: Re-run regressions**

Run:
- `fb_summary_v3`
- `fb_wal_parallel_payload`
- `fb_spill`

Expected:
- all pass

### Task 7: Service Compatibility And Documentation Finish

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `docs/architecture/overview.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Update service stats if needed for v3 file size/coverage**

- [ ] **Step 2: Document the final implemented semantics**

Include:
- file format version
- fallback rules
- observed performance target areas

- [ ] **Step 3: Mark completed TODO items**

### Task 8: Final Verification

**Files:**
- Test only

- [ ] **Step 1: Build and install cleanly**

Run:
`make clean && make PG_CONFIG=/home/18pg/local/bin/pg_config install`

Expected:
- success

- [ ] **Step 2: Restart PG18**

Run:
`su - 18pg -c "/home/18pg/local/bin/pg_ctl -D /isoTest/18pgdata restart -m fast -w"`

- [ ] **Step 3: Run focused regression suite**

Run:
`su - 18pg -c "cd /tmp && /home/18pg/local/lib/postgresql/pgxs/src/test/regress/pg_regress --inputdir=/root/pg_flashback --bindir=/home/18pg/local/bin --dbname=contrib_regression --user=18pg fb_summary_v3 fb_summary_prefilter fb_recordref fb_spill fb_runtime_gate"`

- [ ] **Step 4: Run one live flashback case**

Use:
- `scenario_oa_12t_50000r.documents`

Verify:
- correctness unchanged
- summary v3 stats show spans and xid outcome hits
