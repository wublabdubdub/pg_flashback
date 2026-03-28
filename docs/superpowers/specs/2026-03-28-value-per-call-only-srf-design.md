# 2026-03-28 value-per-call-only SRF 设计

## 背景

当前 `pg_flashback()` 只要执行器允许 `SFRM_Materialize`，就会优先走内部 `tuplestore` 分支。live 调试已确认这会在大结果集场景下把结果写入 `base/pgsql_tmp/*`，其 temp I/O 与 `pg_flashback.memory_limit` 无关。

## 目标

- `pg_flashback()` 统一收口为 `ValuePerCall` SRF
- 删除内部 materialized SRF / `tuplestore` 路径
- 保持当前公开 SQL 接口不变
- 用回归覆盖“低 `work_mem` 下 `count(*) FROM pg_flashback(...)` 不再由函数本身产生 temp spill”

## 非目标

- 不重写上层 PostgreSQL 执行器对 `Sort/HashAggregate` 等节点的 temp spill 行为
- 不改变 replay / reverse-op / apply 本身的数据结构
- 不重新引入新的结果模式 GUC

## 方案

1. 删除 `pg_flashback()` 内部 `prefer_materialize` 分支，统一走现有 `SRF_PERCALL_SETUP()` 路径。
2. 删除 `fb_apply_materialize()` 及其头文件声明、`tuplestore` 依赖。
3. 新增回归：在低 `work_mem` 下运行 `count(*) FROM pg_flashback(...)`，用 `pg_stat_database.temp_bytes` 差值断言函数路径本身不再制造 temp spill。
4. 更新 `STATUS/TODO/README/architecture/ADR`，明确当前只有 `ValuePerCall`。

## 预期收益

- 大结果集不再因为 `pg_flashback()` 自己先 materialize 而写 `pgsql_tmp`
- 函数行为与用户对“流式历史查询”的预期一致
- 代码和文档统一回到单路径
