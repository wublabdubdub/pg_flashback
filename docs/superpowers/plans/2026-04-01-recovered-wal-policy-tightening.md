# Recovered WAL Policy Tightening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the archive exact-hit materialization path so `recovered_wal` is used only for mismatched/overwritten `pg_wal` recovery when archive cannot provide the real segment.

**Architecture:** Keep archive-first resolution unchanged in `fb_wal.c`, but narrow `fb_ckwal_restore_segment()` so it no longer copies exact archive hits into `recovered_wal`. `recovered_wal` remains a normalized sink only for segments recovered from mismatched `pg_wal`, plus reuse of already materialized files.

**Tech Stack:** PostgreSQL extension C, PGXS regression SQL, project architecture docs

---

### Task 1: Record the policy change

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/wal-source-resolution.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Update docs with the new recovered_wal boundary**

State that archive exact-hit files must stay in archive, and `recovered_wal` is reserved for mismatched/overwritten `pg_wal` recovery when archive cannot satisfy the segment.

### Task 2: Add regression coverage first

**Files:**
- Modify: `sql/fb_recovered_wal_policy.sql`
- Modify: `expected/fb_recovered_wal_policy.out`

- [ ] **Step 1: Write a failing regression for archive-hit no-materialize**

Assert that when archive already contains the real segment and `pg_wal` only offers a wrong-name copy, `recovered_wal` remains empty after prepare.

- [ ] **Step 2: Write a failing regression for archive-missing mismatch materialize**

Assert that when archive is absent and only `pg_wal` contains a mismatched file whose header points to the requested segment, prepare causes one recovered segment to appear under the real name.

- [ ] **Step 3: Run the targeted regression and verify it fails for the new branch**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS=fb_recovered_wal_policy`

Expected: the new archive-missing mismatch assertion fails before implementation.

### Task 3: Tighten ckwal behavior

**Files:**
- Modify: `src/fb_ckwal.c`

- [ ] **Step 1: Restrict restore input sources**

Keep reuse of an already materialized `recovered_wal/<real-segname>` file, but remove archive exact-hit copying from `fb_ckwal_restore_segment()`.

- [ ] **Step 2: Permit materialization only from mismatched/overwritten `pg_wal`**

Scan `pg_wal` for a file whose header resolves to the requested `timeline + segno`; materialize it into `recovered_wal` only in that case.

- [ ] **Step 3: Preserve existing behavior for explicit mismatch conversion**

Leave `fb_ckwal_convert_mismatched_segment()` as the normalized writer used when resolver has already identified an invalid `pg_wal` candidate.

### Task 4: Verify and close out

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Run targeted regression**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS=fb_recovered_wal_policy`

Expected: PASS

- [ ] **Step 2: Run the closely related policy regressions**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS="fb_wal_source_policy fb_wal_prefix_suffix fb_wal_error_surface"`

Expected: PASS

- [ ] **Step 3: Update docs to completed state**

Mark the policy tightening done in `STATUS.md` and `TODO.md`, with the verification commands and outcomes.
