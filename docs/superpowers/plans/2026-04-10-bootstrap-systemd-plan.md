# Bootstrap Systemd Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a one-shot bootstrap script that a database OS user can run after cloning the repo to build/install `pg_flashback`, create or upgrade the extension, and register/start `pg_flashback-summaryd` through systemd with CentOS 7/8/9 defaults.

**Architecture:** Add a shell entrypoint under `scripts/` that owns build/install, SQL bootstrap, OS detection, systemd unit rendering, and service activation. Keep the existing daemon binary unchanged and expose the new path through README plus shell smoke tests.

**Tech Stack:** Bash, PGXS `make`, `psql`, `pg_config`, `systemd`

---

### Task 1: Add failing smoke coverage for bootstrap surface

**Files:**
- Create: `tests/summaryd/bootstrap_help_smoke.sh`
- Modify: `Makefile`

- [ ] Add a smoke test that expects a `scripts/bootstrap_pg_flashback.sh` executable and verifies help output contains `--db-password`, `--systemd-scope`, `--archive-dest`, `--dry-run`, and a remove action.
- [ ] Add a dry-run contract for CentOS adaptation, checking `centos7 -> system` and `centos8/9 -> user`.
- [ ] Wire the new smoke test into `make check-summaryd`.
- [ ] Run the new smoke test and verify it fails before implementation.

### Task 2: Implement bootstrap script

**Files:**
- Create: `scripts/bootstrap_pg_flashback.sh`

- [ ] Parse required inputs: `--pg-config`, `--pgdata`, `--dbname`, `--dbuser`.
- [ ] Add a second action for remove/teardown that stops the service, removes generated files, and drops the extension.
- [ ] Support password input via `--db-password` or secure prompt fallback.
- [ ] Detect CentOS major version from `/etc/os-release` with a test override file.
- [ ] Default `systemd` scope by OS version and allow `--systemd-scope` override.
- [ ] Run `make` and `make install` with the requested `PG_CONFIG`.
- [ ] Use `psql` to `CREATE EXTENSION IF NOT EXISTS` and `ALTER EXTENSION ... UPDATE TO <VERSION>`.
- [ ] Render a config file plus systemd unit file and start it via `systemctl` or `systemctl --user`.
- [ ] Add a `--dry-run` mode that prints chosen scope and generated paths without mutating the host.

### Task 3: Update docs and open-source mirror surface

**Files:**
- Modify: `README.md`
- Modify: `summaryd/README.md`

- [ ] Document the one-command bootstrap path for CentOS 7/8/9.
- [ ] Explain where the generated config file and unit file are written for system/user scope.
- [ ] Explain password handling and the default service naming rule.

### Task 4: Verify and sync

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] Run `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`.
- [ ] Run the bootstrap smoke tests in `--dry-run` mode.
- [ ] Refresh the open-source mirror with `bash scripts/sync_open_source.sh`.
- [ ] Mark status/todo entries complete with the exact commands run.
