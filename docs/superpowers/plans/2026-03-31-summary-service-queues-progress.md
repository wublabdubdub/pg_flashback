# Summary Service Hot/Cold Queue And Progress View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make summary prebuild explicitly prioritize recent WAL via hot/cold queues and expose current build progress through a SQL view.

**Architecture:** Keep the existing preload launcher/worker model, but make scheduling explicit: launcher classifies missing stable candidates into hot and cold tasks, workers always claim hot tasks first, and a SQL-visible progress function computes cluster-wide stable-candidate completion plus queue/runtime counters. Regression coverage should validate the scheduling policy via debug helpers and verify the public view shape.

**Tech Stack:** PostgreSQL extension C, AddinShmem/LWLock, static background workers, PGXS regress

---

### Task 1: Lock The New Scheduling Semantics

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/decisions/ADR-0019-summary-prebuild-service.md`

- [ ] Step 1: Record that summary prebuild now defaults to recent-first via hot/cold queues
- [ ] Step 2: Record that a SQL progress view becomes part of the service observability surface

### Task 2: Add Regression Coverage First

**Files:**
- Create: `sql/fb_summary_service.sql`
- Create: `expected/fb_summary_service.out`
- Modify: `Makefile`

- [ ] Step 1: Add a failing regression that creates debug helpers from the shared library
- [ ] Step 2: Assert the scheduling classifier marks recent candidates as hot and older backlog as cold
- [ ] Step 3: Assert the installed progress view exists and returns sane counters

### Task 3: Implement Hot/Cold Scheduling And Progress Surfaces

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `include/fb_summary_service.h`
- Modify: `sql/pg_flashback--0.1.0.sql`

- [ ] Step 1: Add explicit task temperature/priority metadata to the shared queue
- [ ] Step 2: Make workers claim hot tasks before cold tasks, newest segno first within each class
- [ ] Step 3: Add reusable scheduling/progress helpers and debug entry points
- [ ] Step 4: Install a public SQL view over a C function that reports progress and queue state

### Task 4: Verify

**Files:**
- Test: `sql/fb_summary_service.sql`
- Test: `expected/fb_summary_service.out`
- Test: `sql/fb_summary_prefilter.sql`
- Test: `expected/fb_summary_prefilter.out`

- [ ] Step 1: Run `make clean && make install`
- [ ] Step 2: Run focused `installcheck` for summary/service regressions
- [ ] Step 3: Query the new view on the PG18 preload instance and verify recent-first counters move as expected
