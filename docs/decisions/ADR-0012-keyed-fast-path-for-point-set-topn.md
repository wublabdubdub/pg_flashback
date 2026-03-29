# ADR-0012: keyed fast path for point/set/top-N flashback queries

## 状态

已接受

## 背景

`pg_flashback()` 当前已经通过 `CustomScan` 和 slot-native 输出解决了大结果集的两类额外膨胀：

- PostgreSQL `FunctionScan -> tuplestore`
- `Datum -> ExecStoreHeapTupleDatum()` 造成的 `ExecutorState` 线性增长

但对 keyed relation 来说，下列查询仍然沿用“全量 flashback 结果流，再由上层过滤/排序”的口径：

- `WHERE 主键 = const`
- `WHERE 主键 IN (const, ...)`
- `ORDER BY 主键 ASC|DESC LIMIT N`

这与 PostgreSQL 内核对点查、主键集合查、主键顺序 top-N 的优化能力差距过大。

## 决策

项目引入统一的 keyed fast path。

第一阶段：

- 仅 keyed relation
- 仅单列稳定主键/唯一键
- 统一覆盖：
  - `WHERE key = const`
  - `WHERE key IN (const, ...)`
  - `ORDER BY key [ASC|DESC] LIMIT N`
- 不能证明正确时自动回退

实现上：

- planner 负责识别可下推模式
- `CustomScan` 负责携带 fast-path 规格
- apply 负责真正执行 key lookup / ordered top-N

## 原因

- 这三类查询本质上都可以收敛为“按 key 驱动 flashback”
- 用一套内部规格统一表达，比零散做三个特判更稳
- 先限制为单列稳定键，可以控制正确性和实现复杂度

## 后果

正面：

- `WHERE key = const` 与 `WHERE key IN (...)` 将不再扫完整当前表
- `ORDER BY key ... LIMIT N` 将具备真正的早停能力
- 大 live case 将从“全量重建后上层再裁剪”收敛为“按目标 key 工作”

负面：

- planner / CustomScan / apply 之间新增一条执行规格协议
- 需要保守处理 key update、复杂 qual 和非单列键

## 非目标

- bag relation 优化
- 复合键 fast path
- 范围谓词下推
- 任意排序列下推
