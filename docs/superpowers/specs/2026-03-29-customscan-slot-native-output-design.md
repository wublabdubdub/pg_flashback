# CustomScan Slot-Native Output Design

## 背景

`FROM pg_flashback(...)` 已经不再走 PostgreSQL 默认 `FunctionScan -> tuplestore`，但 live case `SELECT * FROM pg_flashback(...) ORDER BY id DESC LIMIT 10` 仍会出现数 GB 到 10GB+ 的 backend RSS 膨胀。

当前已定位到：

- `TupleSort` 自身内存极小，不是主因
- `ExecutorState` 会随着已输出历史行数量持续膨胀
- `fb_apply` 当前先把每一行结果做成复合 `Datum`
- `fb_custom_scan` 再用 `ExecStoreHeapTupleDatum()` 反解回 scan slot

这条 `slot -> Datum -> slot` 路径会让已输出结果在 executor 总上下文里持续累积，不符合 scan 节点应有的“当前行覆盖当前行”模型。

## 目标

- 保持现有 SQL 接口与 `CustomScan` 规划路径不变
- 修复 `CustomScan` 输出链路导致的 `ExecutorState` 线性膨胀
- 将 `FROM pg_flashback(...)` 的结果发射改成 slot-native 流式交付
- 不在本轮实现 `ORDER BY/LIMIT` 下推，也不改变 replay/apply 算法工作集

## 方案

### 1. 在 apply 层保留 `FbApplyEmit`

当前 keyed/bag apply 已经能区分：

- `FB_APPLY_EMIT_SLOT`
- `FB_APPLY_EMIT_TUPLE`

因此不改 keyed/bag 核心判定，只新增一条“把 emit 直接写进目标 `TupleTableSlot`”的公共路径。

### 2. 新增 slot-native apply API

新增 `fb_apply_next_slot(FbApplyContext *ctx, TupleTableSlot *slot)`：

- 对 `FB_APPLY_EMIT_SLOT` 使用 `ExecCopySlot()`
- 对 `FB_APPLY_EMIT_TUPLE` 使用 `ExecForceStoreHeapTuple(..., false)`
- 继续复用现有进度、phase 切换、scan/residual 逻辑

现有 `fb_apply_next(..., Datum *)` 继续保留，供 SQL SRF 路径使用。

### 3. 在 flashback query helper 暴露 slot API

新增 `fb_flashback_query_next_slot()`，供 `fb_custom_scan` 直接拉取 scan slot。

### 4. CustomScan 改成直接填充 scan slot

`fb_custom_scan` 不再：

- 调 `fb_flashback_query_next_datum()`
- 调 `ExecStoreHeapTupleDatum()`

改为：

- 直接调用 `fb_flashback_query_next_slot()`
- 让 scan slot 成为唯一对上层暴露的当前行容器

## 预期效果

- 已输出的历史行不再持续堆积进 `ExecutorState`
- `FROM pg_flashback(...)` 的内存曲线应主要反映 flashback 算法工作集，而不是结果发射副本
- `ORDER BY/LIMIT` 仍可能慢，但应不再因为输出路径本身而无限涨 RSS

## 不做的事

- 不修改用户 SQL 接口
- 不恢复 materialized SRF
- 不在本轮做“按主键顺序下推 `ORDER BY ... LIMIT N`”
- 不承诺 replay/apply 工作集本身立刻大幅下降
