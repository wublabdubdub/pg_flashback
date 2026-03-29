# pg_flashback 多 CustomScan Explain 设计

## 目标

让用户在：

```sql
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF)
SELECT *
FROM pg_flashback(NULL::schema.table, target_ts_text);
```

下，直接看到 `pg_flashback` 各主要阶段的独立 plan node 与耗时，而不是只有一个黑盒 `Custom Scan (FbFlashbackScan)`。

## 当前问题

当前 `FROM pg_flashback(...)` 虽然已经通过 `CustomScan` 避开 `FunctionScan` 和 `tuplestore`，但执行期仍是：

```text
FbFlashbackScan
  -> fb_flashback_query_begin()
     -> validate
     -> wal prepare
     -> record index
     -> replay discover/warm/final
     -> reverse source finish
     -> apply begin
```

也就是说：

- 计划树只有一个节点
- 绝大多数工作都发生在单节点 startup 中
- 用户只能看见总耗时，看不见阶段归因

## 方案对比

### 方案 A：保留单节点，只在 Explain 文本里追加阶段耗时

优点：

- 改动最小

缺点：

- 不是 plan tree 上的真实节点
- 不能和 PostgreSQL 原生 `EXPLAIN ANALYZE` 语义对齐
- 后续 replay / spill 继续拆层时仍然是黑盒

### 方案 B：粗粒度三层节点

```text
FbApplyScan
  -> FbReplayBuildScan
    -> FbWalIndexScan
```

优点：

- 可以较快摆脱“单黑盒”

缺点：

- replay 仍然太粗
- 不能回答“discover / warm / final 哪一段慢”

### 方案 C：推荐，按稳定中间产物拆成六层节点

```text
FbApplyScan
  -> FbReverseSourceScan
    -> FbReplayFinalScan
      -> FbReplayWarmScan
        -> FbReplayDiscoverScan
          -> FbWalIndexScan
```

优点：

- 与当前真实数据边界一致
- 每层都对应一个可复用中间产物
- 最适合后续 bounded spill / replay 收敛继续演进

缺点：

- 需要把 replay 中间状态正式抬出单函数内部

## 设计

### 1. 节点语义

每个中间节点都采用“一次执行完成本阶段，然后返回 EOF”的执行模型：

- 第一次 `ExecProcNode()`：
  - 先驱动子节点完成
  - 再执行本节点对应阶段
  - 缓存阶段结果
  - 返回空 slot
- 后续 `ExecProcNode()`：
  - 直接返回空 slot

只有根节点 `FbApplyScan` 真正持续返回历史结果行。

这样有两个好处：

- 阶段耗时发生在节点自己的 `ExecProcNode()` 中，能被 `EXPLAIN ANALYZE` 记录
- 中间节点不需要伪造真正的行流协议，只作为 executor 可观测 barrier

### 2. 节点边界与中间产物

#### `FbWalIndexScan`

输入：

- `source_relid`
- `target_ts_expr`

输出状态：

- `FbRelationInfo`
- `TupleDesc`
- `TimestampTz target_ts`
- `FbSpoolSession`
- `FbWalScanContext`
- `FbWalRecordIndex`

#### `FbReplayDiscoverScan`

输入：

- `FbWalIndexScan` 输出

输出状态：

- discover 轮次最终收敛出的共享 `backtrack_blocks`
- replay discover 统计信息

#### `FbReplayWarmScan`

输入：

- `FbWalIndexScan` 输出
- `FbReplayDiscoverScan` 输出

输出状态：

- warm 后的 replay store
- warm 阶段 TOAST store
- warm 阶段 tracked bytes 高水位

#### `FbReplayFinalScan`

输入：

- `FbWalIndexScan` 输出
- `FbReplayWarmScan` 输出

输出状态：

- `FbReplayResult`
- 尚未 finish 的 `FbReverseOpSource`

#### `FbReverseSourceScan`

输入：

- `FbReplayFinalScan` 输出

输出状态：

- 已完成排序 / spill 收尾的 `FbReverseOpSource`

#### `FbApplyScan`

输入：

- `FbWalIndexScan` 输出中的 `FbRelationInfo` / `TupleDesc`
- `FbReverseSourceScan` 输出
- planner 下推的 `FbFastPathSpec`

输出：

- 历史结果行

### 3. 规划期结构

planner 仍从 `RTE_FUNCTION pg_flashback(anyelement, text)` 识别入口，但不再只注入一个 `CustomPath`。

改为构造一棵嵌套 `custom_paths`：

- 顶层 `FbApplyScanPath`
- 子路径 `FbReverseSourceScanPath`
- 子路径 `FbReplayFinalScanPath`
- 子路径 `FbReplayWarmScanPath`
- 子路径 `FbReplayDiscoverScanPath`
- 叶子 `FbWalIndexScanPath`

只有顶层 apply 节点承接原来的 scan targetlist、qual 与 fast path。

### 4. 进度与 Explain 的关系

现有 `fb_progress` 继续保留：

- `NOTICE progress` 仍适合在线人工排查
- 多节点 `EXPLAIN ANALYZE` 适合查询级归因与 plan 可视化

两者并存，不互相替代。

### 5. 回归策略

第一批回归先覆盖结构与行为：

- `EXPLAIN (VERBOSE, COSTS OFF)` 下能看到多层 `Custom Scan`
- 低 `work_mem` 下 `count(*) FROM pg_flashback(...)` 仍保持 `temp_bytes = 0`
- 光标持续 `FETCH` 时 `ExecutorState` 仍不线性增长

时间数值本身不进入回归基线，避免不稳定。

## 风险

- replay 中间状态跨节点后，cleanup 顺序必须严格
- `ReScanCustomScan` 需要保证各层中间态正确销毁并重建
- 计划树加深后，`EXPLAIN` 输出与现有回归会有较大基线变动
