# keyed fast path 设计

## 背景

当前 `FROM pg_flashback(...)` 已经由扩展自带 `CustomScan` 接管，并且输出链路已改成 slot-native，消除了 PostgreSQL `FunctionScan -> tuplestore` 与 `Datum -> ExecStoreHeapTupleDatum()` 两类额外膨胀。

但以下三类查询仍然会走“先生成全量历史结果，再由上层过滤/排序”的路径：

- `WHERE id = const`
- `WHERE id IN (const, ...)`
- `ORDER BY id [ASC|DESC] LIMIT N`

这会导致：

- 点查和小集合查仍然扫描整个当前表
- `ORDER BY ... LIMIT` 虽然可以依赖 bounded sort 控制排序内存，但仍需要先消费完整历史结果
- live case 中大量无谓 CPU、I/O 和 flashback 工作集长期驻留

## 目标

为 keyed relation 增加一条统一的 key-aware fast path，在 planner、CustomScan executor 与 apply 之间共享同一份执行规格，覆盖：

1. `WHERE 主键 = const`
2. `WHERE 主键 IN (const, ...)`
3. `ORDER BY 主键/稳定唯一键 ASC|DESC LIMIT N`

## 范围

第一阶段固定约束：

- 仅支持 `keyed` relation
- 仅支持单列稳定主键/唯一键
- 仅支持常量等值 / 常量 `IN` / 简单单列 `ORDER BY ... LIMIT`
- 不能证明正确或无法安全下推时，一律回退到当前全量 flashback 路径

明确不在第一阶段：

- bag relation
- 复合主键 / 复合唯一键
- 范围谓词（`>`, `<`, `BETWEEN`）
- 复杂 `OR`
- 非常量表达式

## 统一执行规格

新增内部描述 `FbFastPathSpec`，由 planner 生成、CustomScan 保存、executor 反序列化，并传入 `fb_flashback_query_begin()` / `fb_apply_begin()`。

建议字段：

- `mode`
  - `FB_FAST_PATH_NONE`
  - `FB_FAST_PATH_KEY_EQ`
  - `FB_FAST_PATH_KEY_IN`
  - `FB_FAST_PATH_KEY_TOPN`
- `attnum`
- `type_oid`
- `collation`
- `keys[]`
- `order_asc`
- `limit_count`
- `ordered_output`

统一语义：

- `EQ` 是单 key 特例
- `IN` 是 key 集合特例
- `TOPN` 是按 key 顺序生成历史结果并在达到 `limit` 后早停

## planner 识别规则

planner 仅在 `RTE_FUNCTION pg_flashback(...)` 且 relation 为单列 keyed 时尝试识别。

### `WHERE key = const`

识别 `OpExpr`：

- 左侧是 scan tlist 中的 key `Var`
- 右侧是非 NULL `Const`
- 操作符是该类型的等值操作符

### `WHERE key IN (...)`

识别 `ScalarArrayOpExpr`：

- 左侧是 key `Var`
- 右侧是常量数组
- 使用等值操作符
- 元素数量有限且非空

### `ORDER BY key [ASC|DESC] LIMIT N`

识别条件：

- `sortClause` 只有一项
- 排序目标是 key `Var`
- `limitCount` 是非 NULL 常量
- `LIMIT` 为正整数

一旦命中 `TOPN`：

- `CustomPath.pathkeys` 设置为 `root->query_pathkeys`
- 这样 planner 可以直接接受 `CustomScan` 的有序输出，消除外部 `Sort`

## apply 行为

### `EQ / IN`

不再 `table_beginscan()` 全表。

改为：

1. 用稳定主键/唯一键索引对当前表做点查
2. 对命中的当前 tuple 调用 keyed apply 当前行处理
3. 对未命中的 key，检查 keyed reverse 状态里是否存在 residual replacement tuple
4. 每个 key 最终返回 0 或 1 条历史行

### `TOPN`

不再“全量 flashback -> Sort -> Limit”。

改为：

1. 按主键索引方向扫描当前表
2. 对每个当前 key 做一次 keyed apply 判定
3. 每命中一条历史结果就计入输出
4. 达到 `LIMIT N` 后立即停止
5. 最后补上 residual replacement tuple，并按同一 key 顺序参与 top-N

## 正确性与回退

第一阶段必须保守。

以下场景直接回退：

- relation 不是 keyed
- `key_natts != 1`
- 无法拿到稳定主键/唯一键索引 oid
- `ORDER BY` 不是唯一支持的 key 顺序
- 复杂 `qual`
- 查询需要重扫
- keyed reverse 状态检测到 key-update 风险且当前 fast path 无法证明顺序与过滤语义正确

关于 key update：

- `EQ / IN` 第一阶段可以在 keyed state 内按“旧 key / 新 key replacement tuple”保守支持
- `TOPN` 顺序语义最敏感，若检测到 key update 风险则直接回退

## 预期收益

- `WHERE id = const`：从全表 apply 降到单 key flashback
- `WHERE id IN (...)`：复杂度近似按 key 数线性增长
- `ORDER BY id DESC LIMIT N`：从全量结果排序降到索引顺序扫描 + 早停

## 回归要求

新增 keyed fast-path 回归，至少覆盖：

- `EXPLAIN` 下 `WHERE id = const` 命中 fast path
- `EXPLAIN` 下 `WHERE id IN (...)` 命中 fast path
- `EXPLAIN` 下 `ORDER BY id DESC LIMIT N` 不再出现上层 `Sort`
- 结果正确性与当前全量 flashback 一致
- 不支持场景时正确回退
