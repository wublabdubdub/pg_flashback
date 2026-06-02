# 2026-03-27 users WAL tail inline 计划

1. 识别当前 recent tail 进入主链前的重复装配点。
2. 将 `pg_wal` tail 合并进统一 resolver。
3. 回归 recent tail 与 archive overlap 场景。
