# 2026-03-27 users large output streaming 设计

## 问题

materialized SRF 对 TOAST-heavy 热点有效，但对超大结果集会引入新的结果缓存压力。

## 当前结论

- TOAST-heavy materialize 与“大结果集强流式”是两类不同约束
- 本轮先解决 TOAST flatten 热点
- 大输出预算与进一步流式化单独收敛
