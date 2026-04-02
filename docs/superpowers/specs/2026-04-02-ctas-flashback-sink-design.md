# `pg_flashback(...)` full-output 快路径设计稿

## 背景

当前项目已拍板删除原表闪回入口 `pg_flashback_to(regclass, text)`。

删除后，客户侧对“全表闪回后落地到新表”的正式承接面，需要收口为 PostgreSQL 内核原生语法，而不是再保留扩展自定义建表入口。

当前已经确认：

- `SELECT * FROM pg_flashback(...)` 由扩展自带 `CustomScan` 接管，不走 PostgreSQL 默认 `FunctionScan -> tuplestore`
- `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)` 当前也会继续进入同一条 `CustomScan` 链
- PostgreSQL `COPY` 原生只支持：
  - `COPY table FROM ...`
  - `COPY (query) TO ...`
- PostgreSQL 不存在“`COPY FROM SELECT` 直接创建新表”的内核语法

因此，全表闪回“另立新表”的正式产品路径必须是：

```sql
CREATE TABLE new_table AS
SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);
```

而：

```sql
COPY (
  SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text)
) TO ...
```

只作为导出路径存在。

## 目标

在不新增新的公开 helper 的前提下，把全表闪回结果的三条 full-output 路径做成共享快路径：

- `SELECT * FROM pg_flashback(...)`
- `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)`
- `COPY (SELECT * FROM pg_flashback(...)) TO ...`

主目标不是替换 PostgreSQL 原生 `CTAS` receiver，而是把这些 full-output 场景上游仍然偏高的 tuple 产出、搬运和通用 apply 成本降掉。

## 非目标

- 不改变普通 `SELECT * FROM pg_flashback(...)` 的语义
- 不改变 `COPY (SELECT * FROM pg_flashback(...)) TO ...` 的导出语义
- 不新增新的公开 SQL helper
- 不把 `COPY` 改造成“建表”路径
- 不在本轮引入后台预物化历史表或全量快照 sidecar 新体系

## 现状问题

当前全表落地新表的执行链路仍然是：

`FbWalIndexScan -> FbReplayDiscover -> FbReplayWarm -> FbReplayFinal -> FbReverseSource -> FbApplyScan -> 通用 executor slot -> CTAS receiver -> 新表`

需要先纠正一个前提：

- PostgreSQL `CTAS` 内核末端不是“通用慢 receiver”
- `createas.c` 的 `IntoRelDestReceiver` 已直接使用：
  - `BulkInsertState`
  - `table_tuple_insert()`

因此，真正值得优化的不是“替换 CTAS receiver”，而是它上游几类 full-output 成本：

- `apply` 结果仍要经过标准 executor 的逐 tuple scan/slot 发射
- `FbApplyEmit` 在 `HeapTuple` / `TupleTableSlot` 之间仍有额外搬运

这些成本不仅影响 `CTAS`，同样也影响：

- 普通 `SELECT * FROM pg_flashback(...)`
- `COPY (SELECT * FROM pg_flashback(...)) TO ...`

## 方案选择

### 方案 A：只继续优化现有 `pg_flashback(...)` 主链

优点：

- 改动最小
- 不需要识别 `CTAS`

缺点：

- 最后落表仍然走通用 `CTAS` receiver
- 对“新表落地速度”提升有限

### 方案 B：保留 PostgreSQL 原生 sink，新增共享 full-output 快路径

优点：

- 仍使用 PostgreSQL 原生 `CTAS`
- 不新增新的用户入口
- 不与内核已存在的 bulk insert receiver 重复造轮子
- 能同时惠及 `SELECT` / `CTAS` / `COPY (query) TO`

缺点：

- 需要先精确识别 simple full-output 场景
- 需要改 `apply`/输出阶段而不是只改单个末端

### 方案 C：引入预物化历史表 / 大型 sidecar

优点：

- 极限性能上限最高

缺点：

- 架构侵入面、磁盘占用、维护成本都明显过大
- 已超出当前产品收口范围

## 决策

采用方案 B。

即：

- 产品层继续推荐用户写标准 `CTAS AS SELECT * FROM pg_flashback(...)`
- 扩展内部不替换 PostgreSQL 原生 `CTAS` receiver
- 改为识别 simple full-output 场景，并把优化重点放在 `apply` 上游的共享行产出快路径

## 设计

### 1. 共享 full-output 执行模型

保留当前主干：

- `FbWalIndexScan`
- `FbReplayDiscoverScan`
- `FbReplayWarmScan`
- `FbReplayFinalScan`
- `FbReverseSourceScan`
- `FbApplyScan`

对三种 simple full-output 场景，共享一条更轻的上游发射路径：

- 普通 `SELECT * FROM pg_flashback(...)`
- `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)`
- `COPY (SELECT * FROM pg_flashback(...)) TO ...`

而复杂查询继续走现有通用路径。

### 2. 识别策略

需要在 planner/executor 中识别：

- 查询主体可匹配 `pg_flashback(anyelement, text)`
- 输出是全行输出
- 当前不包含会显著改变输出语义的额外上层节点

首版应保守启用，仅覆盖：

- `SELECT * FROM pg_flashback(...)`
- `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)`
- `COPY (SELECT * FROM pg_flashback(...)) TO ...`
- 不带额外联表、聚合、排序、limit、投影重写

对更复杂场景继续回退现有通用路径，保证正确性优先。

### 3. full-output 快路径职责

快路径职责固定为：

- 尽量减少 `FbApplyEmit` 到最终输出之间的 tuple 搬运
- 避免为 full-output 场景保留不必要的通用 apply 分支
- 在不改变 `CTAS` / `COPY` 末端 receiver 的前提下，把更轻的 slot/tuple 直接送给它们
- 保持普通查询、CTAS、COPY 使用统一的 full-output 行产出内核

### 4. 性能方向

full-output 快路径的主要收益点应来自：

- 减少 `HeapTuple` / `TupleTableSlot` 搬运
- 降低 generic apply / emit 分支判断成本
- 让 `CTAS` 继续直接吃 PostgreSQL 内核 bulk insert receiver
- 让 `COPY (query) TO` 也共享同一条更轻的输出路径

## 风险

- 若 full-output 识别条件过宽，可能误接管复杂 query，带来语义偏差
- 若识别条件过窄，则命中率不高，收益受限
- 需要证明优化点确实在 full-output 上游，而不是别的阶段

## 验证

至少需要覆盖：

1. `EXPLAIN` 验证普通 `SELECT` 仍走现有 `CustomScan`
2. `EXPLAIN` 验证 simple full-output 场景命中新快路径
3. `COPY (SELECT * FROM pg_flashback(...)) TO ...` 与 `CTAS` 共享同一条 full-output 优化路径
4. 与当前实现做同数据量性能对比
5. 对空表、TOAST-heavy、大表场景做正确性验证
