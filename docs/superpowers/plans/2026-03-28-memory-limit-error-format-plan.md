# Memory Limit Error Format Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Improve the shared memory-limit error so users see a tuning hint for `pg_flashback.memory_limit_kb` plus byte counts rendered with human-readable binary units.

**Architecture:** Keep the change in the shared inline helper `fb_memory_charge_bytes()` so every memory-limit error gets the same wording. Extend the existing `fb_memory_limit` regression first, then add a tiny formatter in `include/fb_memory.h` that emits `bytes` plus an exact 1024-based unit when the value is evenly divisible.

**Tech Stack:** PostgreSQL extension (PGXS), C, `pg_regress`, Markdown

---

### Task 1: Record the approved requirement

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Create: `docs/superpowers/plans/2026-03-28-memory-limit-error-format-plan.md`

- [ ] **Step 1: Record the new shared error-format requirement before code changes**
- [ ] **Step 2: Save the plan file for handoff history**

### Task 2: Write failing regression coverage

**Files:**
- Modify: `expected/fb_memory_limit.out`

- [ ] **Step 1: Update the expected memory-limit error to require human-readable units**
- [ ] **Step 2: Update the expected memory-limit error to require a tuning hint mentioning `pg_flashback.memory_limit_kb`**
- [ ] **Step 3: Run `fb_memory_limit` and confirm it fails for the intended reason**

### Task 3: Implement the shared formatter

**Files:**
- Modify: `include/fb_memory.h`

- [ ] **Step 1: Add a tiny helper that formats exact 1024-divisible values as `KB/MB/GB/...`**
- [ ] **Step 2: Reuse the helper in `fb_memory_charge_bytes()` for tracked, limit, and requested**
- [ ] **Step 3: Add a single `errhint` telling users to consider increasing `pg_flashback.memory_limit_kb`**

### Task 4: Verify and sync docs

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: Run targeted build and `fb_memory_limit` regression until green**
- [ ] **Step 2: Update `STATUS.md` with completion and verification evidence**
- [ ] **Step 3: Mark the `TODO.md` item complete**
