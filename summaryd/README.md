# pg_flashback-summaryd

`pg_flashback-summaryd` is the standalone summary prebuild daemon for
`pg_flashback`.

Current status:

- built from the top-level `Makefile`
- installed together with the extension
- publishes `state.json` / `debug.json` under `PGDATA/pg_flashback/meta/summaryd`
- supports `--config`, lock file ownership, and `--once` smoke runs

Current behavior:

- run outside PostgreSQL
- not require `shared_preload_libraries`
- build and clean `DataDir/pg_flashback/meta/summary` through extension-exposed
  debug helpers
- currently require an explicit libpq `conninfo` so the daemon connects to the
  intended PostgreSQL instance and database
- publish daemon status for `pg_flashback_summary_progress` and
  `pg_flashback_summary_service_debug`
- leave query-local summary hint files to the extension-side readers

Recommended startup:

```bash
pg_flashback-summaryd \
  --pgdata /path/to/pgdata \
  --archive-dest /path/to/archive \
  --conninfo "host=/tmp port=5432 dbname=postgres user=postgres connect_timeout=2" \
  --foreground
```

Config-file startup is also supported:

```bash
pg_flashback-summaryd \
  --config /path/to/pg_flashback-summaryd.conf \
  --foreground
```

In the current implementation, the config file should include at least:

```ini
pgdata=/path/to/pgdata
archive_dest=/path/to/archive
conninfo=host=/tmp port=5432 dbname=postgres user=postgres connect_timeout=2
interval_ms=1000
```

Quick verification:

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

If the daemon runs successfully, you should see status files under:

```text
PGDATA/pg_flashback/meta/summaryd/
```
