# Summary Runner And Recent-First Scheduling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace service/systemd lifecycle management with a script-managed summary runner and switch summary backlog scheduling to recent-first across standalone and in-core paths.

**Architecture:** Keep `pg_flashback-summaryd` as the build/scan binary, but move lifecycle management to a repository script that owns `start/stop/status/run-once`. Update both standalone and in-core scheduling so merged `archive_dest + pg_wal` candidates prioritize the newest backlog first while preserving current progress/statistics surfaces.

**Tech Stack:** Bash, PGXS, PostgreSQL extension C code, standalone summary core C code, SQL regression, shell smoke tests.

---

### Task 1: Lock Recent-First Scheduling Behavior

**Files:**
- Modify: `sql/fb_summary_service.sql`
- Modify: `expected/fb_summary_service.out`

- [ ] Write the failing regression expectation for cold backlog ordering.
- [ ] Run `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS=fb_summary_service` and confirm it fails for newest-first.
- [ ] Update extension scheduling code only after the failure is confirmed.

### Task 2: Implement Recent-First Scheduling

**Files:**
- Modify: `src/fb_summary_service.c`
- Modify: `summaryd/fb_summaryd_core.c`

- [ ] Change in-core cold backlog enqueue/claim priority to newest-first.
- [ ] Change standalone iteration order to newest-first so script-managed daemon matches in-core semantics.
- [ ] Re-run `fb_summary_service` regression and confirm it passes.

### Task 3: Replace Service/Systemd Lifecycle With Script Runner

**Files:**
- Modify: `scripts/b_pg_flashback.sh`
- Create: `scripts/pg_flashback_summary.sh`
- Modify: `tests/summaryd/*.sh` as needed

- [ ] Write failing smoke coverage for `start/stop/status/run-once` script entrypoints and for removal of systemd-specific behavior.
- [ ] Implement the script runner with pid/log/config management.
- [ ] Remove bootstrap systemd/service registration logic and switch setup/remove to script-managed lifecycle.
- [ ] Re-run affected summaryd smoke tests until green.

### Task 4: Update Docs And Mirror Surface

**Files:**
- Modify: `README.md`
- Modify: `summaryd/README.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/decisions/ADR-0035-external-summary-daemon-without-preload.md`

- [ ] Replace service/systemd instructions with script-runner instructions.
- [ ] Document `run-once` semantics.
- [ ] Record final status and remaining follow-up.

### Task 5: Verification

**Files:**
- Modify: `Makefile` if test list changes

- [ ] Run targeted regressions/smokes for scheduling and runner changes.
- [ ] Run `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`.
- [ ] Run one real `18pg` script-managed start/status/run-once/stop verification.
- [ ] Refresh open-source mirror with `bash scripts/sync_open_source.sh`.
