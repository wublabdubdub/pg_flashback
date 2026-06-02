# 2026-03-27 flashback TOAST datum fastpath 设计

## 问题

在 `patients` 这类 TOAST-heavy 关系上，stage 8/9 的热点不只来自 keyed 查找，还来自：

- `ExecFetchSlotHeapTupleDatum`
- `heap_copy_tuple_as_datum`
- `toast_flatten_tuple_to_datum`

## 初步结论

仅优化 keyed 命中路径不够，需要同时审视 SRF 结果交付模型。

## 尝试方向

- 缩短 unchanged current row 的复合 Datum 构造路径
- 区分 slot 发射与 tuple 发射
- 为 TOAST-heavy 关系保留 materialized SRF 选项
