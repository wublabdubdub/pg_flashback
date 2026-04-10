# Bootstrap Non-Root Enforcement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `scripts/b_pg_flashback.sh` reject root execution immediately and keep bootstrap docs/tests aligned with that rule.

**Architecture:** Add a single early non-root gate before any prompts or systemd/database work. Lock the behavior with a dedicated root-reject smoke, then update existing bootstrap smoke tests to execute the script under a non-root OS user.

**Tech Stack:** Bash, systemd bootstrap smoke tests, repository status/README docs

---

### Task 1: Record The New Requirement

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Update STATUS.md with the new bootstrap non-root requirement**
- [ ] **Step 2: Update TODO.md to mark the work item in progress/completed**

### Task 2: Add The Failing Root-Reject Test

**Files:**
- Create: `tests/summaryd/bootstrap_nonroot_smoke.sh`
- Modify: `Makefile`

- [ ] **Step 1: Write a smoke test that invokes `scripts/b_pg_flashback.sh` as root and expects an immediate failure**
- [ ] **Step 2: Run `bash tests/summaryd/bootstrap_nonroot_smoke.sh` and confirm it fails against the current script**

### Task 3: Enforce The Rule In The Bootstrap Script

**Files:**
- Modify: `scripts/b_pg_flashback.sh`

- [ ] **Step 1: Add an early root-user guard before any interactive prompts**
- [ ] **Step 2: Use a direct error message that tells operators to rerun as a non-root PostgreSQL OS user**
- [ ] **Step 3: Re-run `bash tests/summaryd/bootstrap_nonroot_smoke.sh` and confirm it passes**

### Task 4: Adjust Existing Bootstrap Smoke Tests To Run As Non-Root

**Files:**
- Modify: `tests/summaryd/bootstrap_help_smoke.sh`
- Modify: `tests/summaryd/bootstrap_prompt_validation_smoke.sh`
- Modify: `tests/summaryd/bootstrap_data_dir_safety_smoke.sh`
- Modify: `tests/summaryd/bootstrap_service_scope_smoke.sh`

- [ ] **Step 1: Update positive bootstrap smoke tests to execute the script via a non-root OS user**
- [ ] **Step 2: Keep the existing assertions intact where behavior is unchanged**
- [ ] **Step 3: Run each bootstrap smoke test and confirm they pass under the new rule**

### Task 5: Update User-Facing Docs And Verify

**Files:**
- Modify: `README.md`
- Modify: `summaryd/README.md`

- [ ] **Step 1: Document that `scripts/b_pg_flashback.sh` must be run as a non-root OS user**
- [ ] **Step 2: Run bootstrap/README smoke coverage and confirm all green**
