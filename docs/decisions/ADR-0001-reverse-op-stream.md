# ADR-0001: 采用 reverse op stream 作为主实现路径

## 状态

Accepted

## 决策

`pg_flashback` 首版以结构化 `reverse op stream` 为核心，不以物理页回放作为主查询路径。

## 原因

- 不需要后台维护大体量页历史
- 更适合只读历史查询
- 更适合同时支持结果查询与 undo 导出
- 更符合“当前表为基线、对目标时间窗逻辑回退”的模型

