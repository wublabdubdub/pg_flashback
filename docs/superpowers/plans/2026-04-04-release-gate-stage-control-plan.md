# Release Gate Stage Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add readable stage-based execution control to the release-gate orchestrator, raise the default DML pressure limit to `2000 ops/s`, and publish a complete operator README.

**Architecture:** Keep the existing `tests/release_gate/bin/*.sh` phase scripts unchanged where possible, and concentrate the new behavior in `run_release_gate.sh` plus shared constants in `common.sh`. The README becomes the single operator-facing contract for stage names, phase dependencies, DML pressure semantics, and rerun patterns.

**Tech Stack:** Bash, existing release-gate shell helpers, `jq`, Markdown

---

### Task 1: Register the new operator contract in project docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: Record the new stage-control scope in `STATUS.md`**
- [ ] **Step 2: Add TODO items for stage control and README rewrite**
- [ ] **Step 3: Extend architecture docs with stage orchestration responsibilities**

### Task 2: Add failing selftest coverage first

**Files:**
- Modify: `tests/release_gate/bin/selftest.sh`
- Test: `tests/release_gate/bin/selftest.sh`

- [ ] **Step 1: Add selftest assertions for `--list-stages`**
- [ ] **Step 2: Add selftest assertions for `--only` and `--from/--to` dry-run selection**
- [ ] **Step 3: Run `bash tests/release_gate/bin/selftest.sh` and verify it fails before implementation**

### Task 3: Implement stage-based orchestrator control

**Files:**
- Modify: `tests/release_gate/bin/run_release_gate.sh`
- Modify: `tests/release_gate/bin/common.sh`
- Modify: `tests/release_gate/config/release_gate.conf`

- [ ] **Step 1: Define readable ordered stage names and descriptions**
- [ ] **Step 2: Parse `--list-stages`, `--from`, `--to`, `--only`, and `--dry-run`**
- [ ] **Step 3: Execute only the selected closed interval of stages**
- [ ] **Step 4: Raise the default DML rate limit to `2000 ops/s`**
- [ ] **Step 5: Re-run selftest and dry-run commands to confirm the new CLI works**

### Task 4: Publish the full release-gate operator README

**Files:**
- Modify: `tests/release_gate/README.md`

- [ ] **Step 1: Document prerequisites and environment assumptions**
- [ ] **Step 2: Document every stage with purpose, prerequisites, and artifacts**
- [ ] **Step 3: Document the exact `1h` DML pressure semantics**
- [ ] **Step 4: Add end-to-end and partial-rerun command examples**
- [ ] **Step 5: Verify the README wording matches the implemented CLI and defaults**
