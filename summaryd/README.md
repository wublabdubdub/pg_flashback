# pg_flashback-summaryd

`pg_flashback-summaryd` is the standalone summary prebuild daemon for
`pg_flashback`.

Current status:

- built from the top-level `Makefile`
- installed together with the extension
- publishes `state.json` / `debug.json` under `PGDATA/pg_flashback/meta/summaryd`
- bootstrap-managed by a fixed shell runner plus cron keepalive

Artifact paths:

- after `make PG_CONFIG=/path/to/pg_config`:
  - local build artifact:
    - `summaryd/pg_flashback-summaryd`
- after `make PG_CONFIG=/path/to/pg_config install`:
  - installed binary:
    - `$(pg_config --bindir)/pg_flashback-summaryd`

One-shot bootstrap:

```bash
scripts/b_pg_flashback.sh
```

The bootstrap script must be run as a non-root OS user. Root execution fails immediately.

The script prompts interactively for the required paths and database
credentials, using env-backed defaults where available:

- `pg_config`: defaults to `which pg_config`
- `PGDATA`: defaults to environment `PGDATA`
- `dbname`: defaults to environment `PGDATABASE` or `postgres`
- `dbuser`: defaults to environment `PGUSER`
- `db-password`: defaults to environment `PGPASSWORD` (hidden in the prompt)
- `db-port`: defaults to environment `PGPORT` or `5432`

The prompt order is fixed as `pg_config -> PGDATA -> dbname -> dbuser -> db-password -> db-port`.
The script then builds, installs, configures, and starts
the summary watchdog for `pg_flashback-summaryd`.
If `--archive-dest` is omitted, bootstrap first tries to autodetect the local
archive directory from PostgreSQL `archive_command`, and only falls back to
`PGDATA/pg_wal` when autodiscovery is unavailable.

If `PGDATA/pg_flashback` already exists, setup first performs bounded safe
cleanup for known temporary content only:

- clear `runtime/`
- clear stale `meta/summaryd/state.json`, `debug.json`, and `*.lock`
- clear temporary `meta/summary/.tmp.*`
- keep `recovered_wal/` and committed summary/meta files intact

Current bootstrap rules:

- setup also runs
  `ALTER DATABASE ... SET pg_flashback.archive_dest = ...`
  so fresh sessions can query summary views without a manual `SET`
- setup installs a per-user cron keepalive that runs
  `scripts/pg_flashback_summary.sh start`
- root must not run the bootstrap script; switch to the target PostgreSQL OS user first
- the current OS user must own `PGDATA`
- if the same environment is already fully initialized, rerunning setup
  returns `already_initialized` and exits early
- remove preserves `PGDATA/pg_flashback` and prints the retained path so the
  operator can delete it manually if desired
- remove also cleans legacy `pg_flashback-summaryd*.service/.conf`
  entries for the same `PGDATA` and removes the installed daemon binary
- terminal output is grouped into explicit stage headers for setup/remove

Manual runner entrypoints:

```bash
scripts/pg_flashback_summary.sh start
scripts/pg_flashback_summary.sh stop
scripts/pg_flashback_summary.sh status
```

Current behavior:

- run outside PostgreSQL
- not require `shared_preload_libraries`
- use a shell watchdog plus cron keepalive as the default service carrier
- build and clean `DataDir/pg_flashback/meta/summary` directly from
  `PGDATA`, `archive_dest`, and `pg_wal`
- publish daemon status for `pg_flashback_summary_progress`
- leave query-local summary hint files to the extension-side readers

Recommended startup:

```bash
pg_flashback-summaryd \
  --pgdata /path/to/pgdata \
  --archive-dest /path/to/archive \
  --foreground
```

Config-file startup is also supported:

```bash
pg_flashback-summaryd \
  --config /path/to/pg_flashback-summaryd.conf \
  --foreground
```

The config file should include at least:

```ini
pgdata=/path/to/pgdata
archive_dest=/path/to/archive
interval_ms=1000
```

Quick verification:

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

Watch these columns first when checking the external daemon:

- `state_source`
- `daemon_state_present`
- `daemon_state_stale`
- `daemon_state_published_at`

If the daemon runs successfully, you should see status files under:

```text
PGDATA/pg_flashback/meta/summaryd/
```
