# 2026-03-27 users WAL tail inline 设计

## 目标

把 recent tail 的 WAL 直接并入主 resolver，不再让 `archive_dest` 与 `pg_wal` 分裂成两条额外装配路径。

## 设计点

- 仍维持 `archive_dest` 优先
- `pg_wal` 只承接 archive 尚未覆盖的 recent tail
- 错配段继续先转入 `recovered_wal/<actual-segname>`
- resolver 在一次扫描里同时处理 archive 命中与 recent tail
