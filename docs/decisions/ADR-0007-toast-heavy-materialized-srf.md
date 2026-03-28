# ADR-0007: TOAST-heavy 直查关系内部切换为 materialized SRF 发射

## 状态

Superseded by `ADR-0009`

## 后续状态说明

该 ADR 在当时为 `TOAST-heavy` 发射 CPU 热点提供了可行方向，但后续 live case 已确认内部 materialized SRF 在大结果集上会引入不可接受的 `pgsql_tmp` temp spill。

当前仓库口径已按 `ADR-0009` 收口为 `ValuePerCall-only`。如果未来需要重新引入 materialize，只能作为新的、严格受限的特例方案重新决策。

## 决策

`pg_flashback(anyelement, text)` 的用户调用形态保持不变，但内部结果发射模型不再强制统一为 value-per-call。

对 `TOAST-heavy` 关系，允许在函数内部改为：

- 仍然走直接查询入口
- 仍然返回 `SETOF anyelement`
- 但在 SRF 内部使用 `tuplestore` materialize 结果行
- 通过 `tuplestore_puttupleslot()` / `tuplestore_puttuple()` 直接写入 tuple
- 不再把当前行或 replacement tuple 先转换为 composite `Datum`

对无 TOAST 或窄行场景，保留现有 value-per-call SRF。

## 原因

- 真实 `patients` 压测的 `perf` 已确认主要热点落在：
  - `ExecFetchSlotHeapTupleDatum`
  - `heap_copy_tuple_as_datum`
  - `toast_flatten_tuple_to_datum`
  - `detoast_attr`
- 这条热点不是 keyed hash 命中问题，而是“SRF 逐行返回复合 Datum”触发的整行 flatten
- PostgreSQL 18 的 `HeapTupleHeaderGetDatum()` 在遇到 external TOAST 时仍会调用 `toast_flatten_tuple_to_datum()`，因此继续尝试“非 flatten 复合 Datum”没有正确收益
- `tuplestore` 内部保存 `MinimalTuple`，可以保留 direct query 语义，同时绕开当前 value-per-call 复合 Datum 热路径

## 影响边界

- 不改变用户 SQL 入口
- 不回到“创建结果表再查询”的旧模型
- 不影响 keyed / bag 的 reverse-op apply 语义
- 只调整 `pg_flashback()` 在 TOAST-heavy 场景下的结果交付方式

## 明确不做

- 不恢复公开结果表接口
- 不为所有场景一刀切改成 materialized SRF
- 不把这次优化扩展成新的用户可见 GUC 或 SQL 入口
