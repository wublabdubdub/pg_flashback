# ADR-0009: `pg_flashback()` 收口为 ValuePerCall-only SRF

## 状态

Accepted

## 决策

`pg_flashback(anyelement, text)` 内部不再保留 materialized SRF 分支，统一只走 `ValuePerCall`。

具体收口为：

- 删除 `SFRM_Materialize` 优先分支
- 删除内部 `tuplestore` 发射路径
- `pg_flashback()` 继续只作为直接查询型 `SETOF anyelement` SRF 暴露
- 如果执行器不给 `SFRM_ValuePerCall`，继续明确报错，不再内部回退为 materialize

## 原因

- live case 已确认大结果集场景下，materialized SRF 会在 `stage 8` 期间把结果写入 PostgreSQL 临时文件 `base/pgsql_tmp/*`
- 这条 temp I/O 不受 `pg_flashback.memory_limit` 控制，而主要受执行器 `tuplestore/work_mem` 行为影响
- 对 `count(*)`、大表 `select *`、大时间窗历史查询而言，这条路径会引入额外的全量结果缓存与显著磁盘写放大
- 当前 flashback 主链已经具备稳定的流式 apply / SRF 返回模型，不需要 materialize 才能保证正确性
- 过去保留 materialize 的唯一明确收益点是 `TOAST-heavy + 小结果集` 的发射 CPU 热点；但这不是当前主线目标，也不足以抵消大结果集 temp spill 风险

## 影响边界

- 不改变用户 SQL 入口
- 不改变 keyed / bag apply 语义
- 不改变 reverse-op / replay / spill 主链
- 会移除内部 `tuplestore` 路径及相关维护负担
- 某些只允许 `SFRM_Materialize` 而不允许 `SFRM_ValuePerCall` 的执行环境将不再支持

## 取舍

- 优点：
  - 避免 `pg_flashback()` 自身在大结果集上制造 `pgsql_tmp` temp spill
  - 执行模型更单一，用户心智和维护成本更低
  - `pg_flashback.memory_limit` 与扩展内存行为的关系更清晰
- 代价：
  - 放弃内部 materialized SRF 对少数 `TOAST-heavy` 小结果集的潜在 CPU 优化
  - 若未来重新需要该优化，必须以更严格的 gated 特例重新引入，而不是恢复默认 fallback

## 与既有决策的关系

- `ADR-0007` 中“允许对 TOAST-heavy 关系内部切 materialized SRF”的方向被本 ADR 收回
- 后续若重新引入，只能作为新的、明确受限的特例决策处理
