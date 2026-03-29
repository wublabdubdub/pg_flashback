# ADR-0013: `pg_flashback_to` 持久表导出

## 状态

Superseded by ADR-0016

## 决策

新增用户接口：

- `pg_flashback_to(regclass, text)`

行为固定为：

- 在源表同 schema 下创建 `table_flashback`
- 若同名表已存在则直接报错
- 创建对象固定为 `logged` 普通持久表
- 先批量写入 flashback 结果，再补：
  - `PRIMARY KEY`
  - `UNIQUE`
  - 普通索引
- 首版复制：
  - 列定义
  - `NOT NULL`
  - `DEFAULT`
  - `CHECK`
- 首版不复制：
  - 外键
  - 触发器

## 原因

- `SELECT * FROM pg_flashback(...)` 适合直接查询，但不适合用户对同一历史时间点反复做筛选、联表、导出
- 用户需要一个“建完即可直接使用”的普通表对象，而不是中间物化结果或临时对象
- 先写数据、后建索引约束，可以避免大结果集阶段逐行维护索引的额外放大

## 影响

- 扩展不再是“绝对只读无副作用”产品边界
- 但副作用范围仍限制为：
  - 不修改源业务表
  - 只创建新的导出表对象
- 核心 flashback 主链仍保持：
  - `WAL scan`
  - `RecordRef`
  - `checkpoint + FPI + block redo`
  - `ForwardOp / ReverseOp / apply`
- `pg_flashback_to` 只替换输出端，不作为 replay / apply 主链性能问题的主修复路线
