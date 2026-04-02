# Remove pg_flashback_to Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `pg_flashback_to(regclass, text)` completely so the product surface only exposes query-style `pg_flashback(anyelement, text)`.

**Architecture:** Treat `pg_flashback_to` as a deleted feature, not a deprecated one. Remove its install entry, delete the dedicated export/rewind implementation module, delete its regress coverage, and rewrite product docs so they no longer describe inplace rewind as current behavior.

**Tech Stack:** PostgreSQL extension SQL, C/PGXS, pg_regress, Chinese maintenance docs.

---

### Task 1: Write the failing user-surface regression

**Files:**
- Modify: `sql/fb_user_surface.sql`
- Modify: `expected/fb_user_surface.out`
- Test: `fb_user_surface`

- [ ] Step 1: Change the regression to assert `pg_flashback_to(regclass,text)` is absent.
- [ ] Step 2: Run `make ... installcheck REGRESS='fb_user_surface'` and confirm it fails before implementation.
- [ ] Step 3: Keep the query-style `pg_flashback(anyelement,text)` assertion intact.

### Task 2: Remove install surface and implementation

**Files:**
- Modify: `sql/pg_flashback--0.1.0.sql`
- Modify: `Makefile`
- Delete: `src/fb_export.c`
- Delete: `include/fb_export.h`

- [ ] Step 1: Remove `pg_flashback_to(regclass, text)` from the extension install script.
- [ ] Step 2: Remove `fb_export.o` from PGXS build inputs.
- [ ] Step 3: Delete the dedicated export/rewind implementation module and header.
- [ ] Step 4: Rebuild and make sure the extension still links successfully.

### Task 3: Remove regressions and user-surface references

**Files:**
- Modify: `Makefile`
- Delete: `sql/fb_flashback_to.sql`
- Delete: `expected/fb_flashback_to.out`
- Modify: `sql/fb_user_surface.sql`
- Modify: `expected/fb_user_surface.out`

- [ ] Step 1: Remove `fb_flashback_to` from the registered regression list.
- [ ] Step 2: Delete the dedicated `fb_flashback_to` regression files.
- [ ] Step 3: Update `fb_user_surface` expected output to match the removed entry.

### Task 4: Rewrite product and architecture docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Modify: `README.md`
- Modify: `docs/architecture/overview.md`
- Delete or rewrite obsolete `pg_flashback_to`-specific ADR/spec/plan docs

- [ ] Step 1: Update current product-boundary docs to say only `pg_flashback(anyelement, text)` remains.
- [ ] Step 2: Remove or delete documents whose sole subject is `pg_flashback_to`.
- [ ] Step 3: Scrub remaining architecture references that still treat inplace rewind as current behavior.

### Task 5: Verify the deletion

**Files:**
- Test: `fb_user_surface`
- Test: one or more query-style regressions such as `pg_flashback`, `fb_flashback_keyed`

- [ ] Step 1: Run `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`.
- [ ] Step 2: Run `make PG_CONFIG=/home/18pg/local/bin/pg_config install`.
- [ ] Step 3: Run `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_user_surface pg_flashback fb_flashback_keyed'`.
- [ ] Step 4: Confirm `to_regprocedure('pg_flashback_to(regclass,text)')` returns `NULL`.
