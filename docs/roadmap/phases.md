# 长期阶段规划

## Phase 0: Foundation

- 仓库初始化
- 文档结构
- 最小扩展骨架
- 最小测试入口

## Phase 1: Relation Gate

- relation 校验
- keyed / bag 模式选择
- TOAST 定位

## Phase 2: WAL Window Core

- 时间窗扫描
- 事务提交边界提取
- WAL 完整性校验
- DDL / rewrite 拒绝

## Phase 3: Decode Core

- `INSERT/DELETE/UPDATE` 行像提取
- `ForwardOp`
- `ReverseOp`

## Phase 4: Apply Engine

- keyed 应用
- bag 应用
- 历史结果集输出

## Phase 5: Export

- undo SQL
- reverse op 审计导出

## Phase 6: Compatibility

- 从 PG18 基线抽兼容层
- 扩到 PG14-18
