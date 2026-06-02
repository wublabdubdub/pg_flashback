# Bootstrap Cleanup Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `b_pg_flashback.sh` preserve `PGDATA/pg_flashback` on remove, perform only bounded safe cleanup on setup, and print clearer stage-separated terminal output.

**Architecture:** Extend the bootstrap script with explicit stage logging, a guarded `PGDATA/pg_flashback` inspection path, and bounded cleanup that only touches known temporary files. Cover the new behavior with shell smoke tests and document the lifecycle in repo docs.

**Tech Stack:** Bash, shell smoke tests, repo docs

---

### Task 1: Lock remove/setup safety behavior with failing shell tests

**Files:**
- Modify: `tests/summaryd/bootstrap_help_smoke.sh`
- Create: `tests/summaryd/bootstrap_data_dir_safety_smoke.sh`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing tests**
- [ ] **Step 2: Run the new smoke tests to verify failure on old behavior**
- [ ] **Step 3: Add the new smoke test to `check-summaryd`**
- [ ] **Step 4: Re-run syntax checks for touched shell files**

### Task 2: Implement guarded data-dir handling and staged output in bootstrap

**Files:**
- Modify: `scripts/b_pg_flashback.sh`

- [ ] **Step 1: Add stage/summary logging helpers**
- [ ] **Step 2: Add guarded `PGDATA/pg_flashback` path resolution and safe-cleanup helpers**
- [ ] **Step 3: Change `--remove` to preserve the data directory and print manual-delete guidance**
- [ ] **Step 4: Change setup to detect existing `PGDATA/pg_flashback` and perform bounded cleanup only**
- [ ] **Step 5: Re-run targeted smoke tests until green**

### Task 3: Update docs and handoff records

**Files:**
- Modify: `README.md`
- Modify: `summaryd/README.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Document preserved data-dir behavior on remove**
- [ ] **Step 2: Document setup-side safe cleanup and staged output**
- [ ] **Step 3: Update status/todo records for the new bootstrap behavior**
- [ ] **Step 4: Sync the open-source mirror and rerun mirror smoke**
