# 2026-03-27 users WAL segment sidecar 计划

1. 确定 sidecar 要承载的最小段级元数据。
2. 让 resolver / wal scan 能消费该 sidecar。
3. 保持 archive / pg_wal / recovered_wal 三来源统一语义。
