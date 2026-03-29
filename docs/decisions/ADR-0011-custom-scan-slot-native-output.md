# ADR-0011: CustomScan 直接以 slot 交付 pg_flashback 行结果

## 状态

已接受

## 背景

`ADR-0010` 已用 `CustomScan` 取代 `FunctionScan`，消除了 PostgreSQL 默认 table SRF 对 `tuplestore` 和 `pgsql_tmp` 的强制物化。

但新的 live 调试表明，`FROM pg_flashback(...) ORDER BY ... LIMIT ...` 仍会出现大量 backend RSS 增长。内存快照显示：

- `TupleSort` 占用很小
- `ExecutorState` 会随着已输出历史行数量持续增长

根因是当前 `CustomScan` 输出链路仍然经过：

- `fb_apply` 将输出行构造成复合 `Datum`
- `fb_custom_scan` 再用 `ExecStoreHeapTupleDatum()` 将该 `Datum` 反解回 scan slot

这会把 scan 节点本应“只保留当前行”的结果流变成“每输出一行就向 executor 总上下文追加分配”。

## 决策

保留 `CustomScan` 规划与 keyed/bag apply 主链不变，但把 `FROM pg_flashback(...)` 的结果交付改为 slot-native：

- `fb_apply` 新增面向 `TupleTableSlot` 的输出 API
- `fb_entry` 暴露 `fb_flashback_query_next_slot()`
- `fb_custom_scan` 直接把下一条 flashback 结果写入 scan slot
- 不再通过 `Datum -> ExecStoreHeapTupleDatum()` 中转

SQL SRF 路径仍保留 datum-returning API，不与本次修复耦合。

## 结果

预期效果：

- `FROM pg_flashback(...)` 已输出行不再线性堆积到 `ExecutorState`
- 查询内存主要反映 flashback 算法工作集，而不是结果发射副本
- 为后续 `ORDER BY/LIMIT` 下推优化保留清晰边界

## 代价

- 需要同时维护 datum-returning SRF API 与 slot-returning CustomScan API
- `fb_apply` 与 `fb_entry` 需要新增一层共享输出适配

## 不做的事

- 不在本 ADR 中解决 `ORDER BY/LIMIT` 的索引化下推
- 不改变 replay/apply 的核心算法与 bounded spill 范围
