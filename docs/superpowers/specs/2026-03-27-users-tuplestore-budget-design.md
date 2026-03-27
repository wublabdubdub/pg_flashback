# 2026-03-27 users tuplestore budget 设计

## 背景

materialized SRF 能绕开 TOAST flatten 热点，但会把结果保存在 tuplestore。

## 约束

- 只对 TOAST-heavy / 宽行关系启用
- 不能重新退化为“全量结果表”
- 需要给结果 materialize 明确预算与切换边界

## 当前口径

- 保留 value-per-call 作为默认路径
- 允许对高 TOAST 场景切 materialized SRF
- 后续再补大结果集流式 / 预算控制
