# ADR-0019 Summary 预建服务与 summary 专属自动清理

## 状态

已接受

## 背景

当前 `pg_flashback` 的 WAL prefilter 冷态路径依赖按查询对原始 WAL segment 做 `mmap + memmem` 扫描。relation-pattern 级 `prefilter-*.meta` sidecar 只能复用“同一张表的同一组 pattern”，不能让后台提前为未来查询准备通用摘要。

与此同时，ADR-0018 已取消 `runtime/recovered_wal/meta` 的通用自动 cleanup。随着 summary 预建服务加入，`meta` 中会新增长期积累的通用 summary 文件，若完全不清理，目录容量会持续增长。

## 决策

新增一套“summary 预建服务”，并恢复“summary 专属自动清理”，但不恢复旧的通用 runtime cleanup 语义。

固定口径如下：

- 预建服务要求 `shared_preload_libraries` 支持
- 注册常驻 launcher 与多 worker pool
- launcher 周期扫描 `archive_dest`、`pg_wal`、`recovered_wal`
- worker 在进程私有内存中读取 WAL segment，构建极小通用 summary
- summary 直接落盘到 `DataDir/pg_flashback/meta/summary`
- 查询 prefilter 优先使用 summary；缺失时继续回退旧查询路径
- relation-pattern 级 `prefilter-*.meta` sidecar 停用，不再继续扩张
- 自动清理只针对 `meta/summary`，不影响 `runtime`、`recovered_wal`、checkpoint sidecar 或其它 query-owned 产物

## 后果

优点：

- 后台可在用户查询前预建通用 summary
- 查询冷态不再主要依赖按 relation 扫描原始 WAL 正文
- `meta` 文件扩张从“按 relation-pattern 爆炸”收敛为“按 segment 一份小 summary”
- 对 PostgreSQL WAL 写入热路径无侵入

代价：

- 引入 AddinShmem、launcher、worker 生命周期复杂度
- 需要 `shared_preload_libraries`
- `meta/summary` 需要独立容量管理与清理策略

## 语义要求

- 不在 `XLogInsert`、事务提交、walwriter、checkpointer、archiver 热路径同步构建 summary
- worker 不连接数据库，不访问 catalog，不把任务状态落库
- 预建服务失效时，flashback 查询仍可通过旧 prefilter 路径工作
- summary 清理只删可再生的 summary 文件，不恢复旧的全目录 cleanup
