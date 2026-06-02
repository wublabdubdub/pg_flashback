# ADR-0015: keyed `pg_flashback_to` 并行导出

## 状态

Superseded by ADR-0016

## 决策

为 `pg_flashback_to(regclass, text)` 新增 opt-in 的 keyed 并行导出路径。

入口开关：

- `pg_flashback.export_parallel_workers`

语义：

- `<= 1`：继续走当前串行 `pg_flashback_to`
- `> 1`：仅在 keyed relation 且单列稳定主键/唯一键时启用并行导出

并行路径固定为：

1. leader 只执行一次 `WAL scan -> replay -> ReverseOpSource`
2. leader 将 reverse-source 序列化为可跨 backend 顺序读取的 shared run 描述，并初始化共享 `ParallelTableScanDesc`
3. prepare worker 先创建最终 `table_flashback`
4. shard worker 只加载自己负责的 changed-key shard
5. shard worker 基于共享并行表扫描读取当前表，并直接写入最终 `table_flashback`
6. post worker 再补 `PRIMARY KEY` / `UNIQUE` / 普通索引

## 原因

- 真实大表用例表明热点主要在 `FbWalIndexScan` 与 `FbApplyScan`，而不是最终写表
- “每个 worker 独立重跑主链”的旧方案重复了最贵的 `WAL/replay` 成本，难以得到稳定正收益
- keyed + 单列稳定键场景可以把并行点前移到“共享一次 reverse-source 之后”，以更小代价把 CPU 用在 `apply/current-table scan`

## 影响

- 并行路径下最终结果表与 staging tables 由独立 worker 事务创建/提交
- 因此并行路径结果表不再跟随调用方事务回滚
- bag relation 不进入并行路径
- 非单列稳定键 relation 不进入并行路径
- 当前并行 worker 只用于 `pg_flashback_to()` 导出，不用于 `pg_flashback()` 纯查询
- 并行导出内部允许为 worker 协调而临时序列化 reverse-source，这不改变 `pg_flashback()` 纯查询路径的 spill 语义
