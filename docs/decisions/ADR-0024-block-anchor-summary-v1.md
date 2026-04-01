# ADR-0024 Block-Anchor Summary V1

## 状态

进行中，已拍板实现方向。

## 背景

`summary v3` 已经把 `3/9 build record index` 的 relation prefilter、relation spans、xid outcomes、touched xids 与 unsafe facts 收到 segment sidecar 中。

但当前 `4/9 replay discover` 里的缺页锚点解析仍主要依赖 query-local `record_log` 反向扫描：

- discover pass 先找出缺页基线 block
- 再从 `record_log` 反向扫，查找这些 block 更早的 `FPI/INIT_PAGE`
- warm/final 继续沿用 query-local `record_index` 口径做 backtrack gate

这意味着现有 summary 仍主要优化 `3/9`，对 replay 缺页锚点回补帮助不足。

## 决策

在现有 segment summary 中新增 `block-anchor summary v1`，首版只记录 relation-scoped block anchor facts，不直接引入全量 block span-driven replay。

首版范围固定为：

1. `summary-*.meta` 新增 block anchor section
2. section 记录 relation / block 上最近可用的 `FPI/INIT_PAGE` 锚点 `LSN`
3. 查询侧优先使用 block anchor summary 解析 missing-block 的最近可用锚点
4. replay backtrack gate 从 `record_index` 收敛到 `anchor_lsn`
5. summary 缺失、损坏或覆盖不足时，继续回退到现有 WAL 反向扫描路径

## 为什么先做 block anchor，不直接做 block span

- 当前最直接的 replay 痛点是“缺页锚点解析”而不是“所有 block 都已完成精准 span 窄化”
- block anchor section 的体积和接入面都明显小于全量 block spans
- 先把 `anchor_lsn` 口径打通，后续才能安全扩展到 block span-driven replay 窄化

## 明确不做

首版不承诺：

- 直接优化 `8/9 applying reverse ops`
- 在 summary 中记录逐 record 行像或完整 block rewrite 历史
- 把 checkpoint sidecar 并入 summary
- 一次性引入“全 replay 主链只按 block spans 遍历”的大改

## 影响

预期收益：

- `4/9 replay discover` 缺页锚点解析更早命中 sidecar
- `5/9 replay warm` 与 backtrack gate 改为统一 `anchor_lsn` 口径
- 现有 relation/xid summary 继续保留，与 block anchor section 互补

代价：

- `summary` 文件格式需要升版本
- 查询侧 / replay 侧都要同步从 `record_index` 适配到 `anchor_lsn`
- 变更后需要删除旧 `meta/summary` 并重建，不能混用旧 sidecar

## 回退策略

若 block anchor section 缺失、损坏或与当前 query window 不匹配：

- 不影响 correctness
- 继续走旧的 `record_log` 反向扫描解析缺页锚点

## 实施后要求

- 手动清空现有 `DataDir/pg_flashback/meta/summary`
- 重新预建 summary
- 复跑 `fb_summary_v3`、`fb_recordref`、`fb_replay` 及相关 live case
