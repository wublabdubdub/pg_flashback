# 2026-03-27 users tuplestore budget 计划

1. 在 `pg_flashback()` 内部分流 value-per-call 与 materialize。
2. 明确 materialize 只作为 TOAST-heavy 优化，不改变用户接口。
3. 继续评估大输出场景是否需要额外预算与流式化补偿。
