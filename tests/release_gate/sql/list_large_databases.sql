\pset tuples_only on
\pset format unaligned

SELECT datname
FROM pg_database
WHERE NOT datistemplate
  AND datname <> current_database()
  AND pg_database_size(datname) > (:'threshold_mb')::bigint * 1024 * 1024
ORDER BY datname;
