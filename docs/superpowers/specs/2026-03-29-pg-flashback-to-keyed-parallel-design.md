# keyed `pg_flashback_to` 并行导出设计稿

## 目标

为 `pg_flashback_to(regclass, text)` 增加一个 keyed-only 的 opt-in 并行导出路径，并避免重复执行最贵的 `WAL/replay` 主链。

## 范围

- 仅 `pg_flashback_to`
- 仅 keyed relation
- 仅单列稳定主键/唯一键
- `pg_flashback.export_parallel_workers > 1` 时启用
- 不支持 bag relation 并行导出

## 方案

并行路径改为 leader 统一构建一次 reverse-source，然后让 worker 只并行 apply/export：

```text
leader
  -> build reverse source once
  -> materialize shareable reverse runs
  -> initialize shared parallel table scan
  -> launch prepare worker
  -> launch shard workers
  -> wait workers
  -> launch post worker

worker_i
  -> load shard_i changed-key state from shared reverse runs
  -> parallel scan current relation
  -> write final table
  -> emit shard_i residual rows

finalizer
  -> create final table
  -> merge stage_0..stage_n
  -> add constraints/indexes
  -> drop stages
```

## 代价

- 需要新增 shared reverse-source 与 `ParallelTableScanDesc` 协调代码
- 并行路径仍然只覆盖 keyed + 单列稳定键
- 并行路径结果表不再跟随调用方事务回滚
