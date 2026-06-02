# Bootstrap Env-Default Prompt Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `scripts/b_pg_flashback.sh` present environment-derived defaults in interactive prompts, with `dbuser` immediately followed by `db-password`.

**Architecture:** Keep CLI arguments authoritative, compute prompt defaults from `PATH`/environment for interactive mode, and preserve secure password input while allowing an environment-backed default. Lock behavior with a dry-run smoke that exercises prompt order and default adoption.

**Tech Stack:** Bash, bootstrap smoke tests, README/status docs

---

### Task 1: Record The Requirement

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Add the new interactive-default requirement to `STATUS.md`**
- [ ] **Step 2: Add the corresponding checklist item to `TODO.md`**

### Task 2: Add The Failing Smoke Test

**Files:**
- Create: `tests/summaryd/bootstrap_prompt_defaults_smoke.sh`
- Modify: `Makefile`

- [ ] **Step 1: Write a smoke test that uses env defaults in `--dry-run` mode**
- [ ] **Step 2: Verify the current script fails the new smoke because it skips/uses the wrong prompt flow**

### Task 3: Implement Interactive Defaults

**Files:**
- Modify: `scripts/b_pg_flashback.sh`

- [ ] **Step 1: Add prompt-default resolution for `pg_config`, `PGDATA`, `dbname`, `dbuser`, `db-password`, and `db-port`**
- [ ] **Step 2: Keep CLI arguments highest priority while allowing env-backed defaults to be overridden interactively**
- [ ] **Step 3: Reorder prompts so `dbuser` is immediately followed by `db-password`, then `db-port`**
- [ ] **Step 4: Run the new smoke and confirm it passes**

### Task 4: Update Docs And Related Smokes

**Files:**
- Modify: `README.md`
- Modify: `summaryd/README.md`
- Modify: `tests/summaryd/bootstrap_help_smoke.sh`

- [ ] **Step 1: Document the env-default prompt behavior**
- [ ] **Step 2: Keep existing bootstrap smoke coverage aligned with the new prompt order/defaults**
- [ ] **Step 3: Run the affected summaryd smoke tests and confirm all green**
