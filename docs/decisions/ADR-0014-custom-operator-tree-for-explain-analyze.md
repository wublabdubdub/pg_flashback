# ADR-0014: 将 pg_flashback CustomScan 拆成可观测的多节点算子树

## 状态

已接受

## 背景

`ADR-0010` 已让 `FROM pg_flashback(...)` 不再走 PostgreSQL 默认 `FunctionScan -> tuplestore`。

`ADR-0011` 又进一步把结果交付改成 slot-native，解决了 `Datum -> ExecStoreHeapTupleDatum()` 导致的 `ExecutorState` 线性膨胀。

但当前 `EXPLAIN` / `EXPLAIN ANALYZE` 仍只有一个：

- `Custom Scan (FbFlashbackScan)`

真实执行阶段虽然已经分成：

- `validate`
- `prepare wal`
- `build record index`
- `replay discover`
- `replay warm`
- `replay final`
- `build reverse ops`
- `apply`
- `emit residual rows`

这些阶段的耗时目前只能通过 `NOTICE progress` 观察，进不到计划树，也无法让用户在 `EXPLAIN ANALYZE` 下直接定位“时间主要花在哪一段”。

## 决策

保留当前用户 SQL 入口不变，但将单个 `FbFlashbackScan` 拆成一棵真实的 `CustomScan` 算子树。

第一阶段采用以下节点边界：

- `FbApplyScan`
- `FbReverseSourceScan`
- `FbReplayFinalScan`
- `FbReplayWarmScan`
- `FbReplayDiscoverScan`
- `FbWalIndexScan`

对应职责如下：

- `FbWalIndexScan`
  - 负责 target relation/runtime gate
  - 负责 `prepare wal + build record index`
- `FbReplayDiscoverScan`
  - 负责共享 backtrack / missing FPI discover
- `FbReplayWarmScan`
  - 负责 warm store 基线构建
- `FbReplayFinalScan`
  - 负责 final redo 与 `ForwardOp -> ReverseOp append`
- `FbReverseSourceScan`
  - 负责 `ReverseOpSource` 排序 / spill 收尾
- `FbApplyScan`
  - 负责 keyed/bag apply、fast path、residual 行发射
  - 作为真正向上层返回历史行的根节点

## 结果

预期效果：

- `EXPLAIN` 计划树中不再只有一个黑盒 `FbFlashbackScan`
- `EXPLAIN ANALYZE` 可以直接看到 WAL、replay、reverse、apply 各自节点的执行耗时
- 后续 bounded spill / replay 收敛可以继续沿这些节点边界推进，而不是继续堆在一个入口函数里

## 代价

- 需要把当前 `fb_flashback_query_begin()` 内部的大串行 startup 逻辑拆到 executor 节点中
- replay 内部 discover / warm / final 之间的中间状态需要正式建模并跨节点传递
- `CustomScan` 的 rescan / end / error cleanup 链路会更复杂

## 不做的事

- 不改变 `pg_flashback(anyelement, text)` 用户接口
- 不把 progress stage 机械地 1:1 映射成 plan node
- 不在本 ADR 中解决 bounded spill 全链路完成问题
