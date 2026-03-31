# Summary-First Record Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `3/9 build record index` use summary as the primary index path so live queries stop paying full-segment summary scans and most serial metadata WAL work.

**Architecture:** Keep the existing segment summary model and checkpoint sidecar. Add compact relation-scoped `touched xids` and `unsafe facts` sections plus a backend-local section cache, then change record-index build to consume summary windows first and only fall back to the original WAL path for uncovered windows.

**Tech Stack:** PostgreSQL C extension, PGXS regress tests, summary sidecar binary format, WAL reader helpers

---

### Task 1: Lock in failing observability/tests

**Files:**
- Modify: `sql/fb_summary_v3.sql`
- Modify: `expected/fb_summary_v3.out`
- Modify: `sql/fb_recordref.sql`
- Modify: `expected/fb_recordref.out`

- [ ] **Step 1: Extend debug assertions for summary-driven reduction**
- [ ] **Step 2: Run the targeted regressions and confirm the new expectations fail on the old implementation**

### Task 2: Add low-storage summary facts and query-local cache

**Files:**
- Modify: `src/fb_summary.c`
- Modify: `include/fb_summary.h`

- [ ] **Step 1: Add compact summary sections for relation-scoped touched xids and unsafe facts**
- [ ] **Step 2: Add backend-local summary section caching for repeated reads in one query**
- [ ] **Step 3: Add reader APIs that expose cached spans/xid outcomes/touched xids/unsafe facts**

### Task 3: Convert 3/9 to summary-first reduction

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`

- [ ] **Step 1: Restrict xid fill to window-covered summary segments**
- [ ] **Step 2: Use summary touched-xid/unsafe facts before entering serial metadata WAL scan**
- [ ] **Step 3: Keep the original uncovered-window WAL fallback for correctness and meta-lag cases**

### Task 4: Rebuild and verify from clean meta

**Files:**
- Modify if needed: `STATUS.md`
- Modify if needed: `TODO.md`

- [ ] **Step 1: Rebuild/install extension**
- [ ] **Step 2: Delete existing `DataDir/pg_flashback/meta/summary` and regenerate summaries**
- [ ] **Step 3: Run targeted regressions**
- [ ] **Step 4: Run live debug verification and record the outcome in docs**
