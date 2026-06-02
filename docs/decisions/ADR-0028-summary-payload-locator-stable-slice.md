# ADR-0028 Summary Payload Locator Stable Slice

## 状态

已接受

## 背景

`ADR-0027` 已把 `summary payload locator-first` 引入主链，目标是把
`3/9 build record index` 的 payload 阶段从“按 spans/window 顺扫大量无关 WAL”
推进到“按精确 record 入口定点 decode”。

这个方向在轻量 live case 上已经成立，但新的重型现场
`scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
确认暴露了新的查询期复杂度放大：

- payload plan 仍以高碎片 `payload_base_windows` 为输入
- 同一 segment 会因碎片 windows 被重复 lookup payload locators
- query cache 当前只缓存 summary 文件内容，不缓存 relation-scoped public locator slice
- `fb_summary_segment_lookup_payload_locators_cached()` 每次 lookup 仍重新拷贝并 `qsort`

结果是旧热点“无关 WAL decode”虽然下降了，但新热点转移成：

- `fb_build_summary_payload_locator_plan`
- `fb_summary_segment_lookup_payload_locators_cached`
- `pg_qsort`

因此当前 blocker 已不是 “locator-first 方向错误”，而是
“locator 查询期实现仍保留大量重复规划与重复排序”。

## 决策

保持当前 summary 上层模型不变：

- 仍然是“一段 WAL 一个 summary sidecar”
- 仍然是 query-side 可回退的可再生缓存
- 不引入跨 segment 的全局 payload 索引

在此基础上，把 payload locator 的契约进一步收敛为：

- summary build 期：
  - 对每个 relation 的 payload locator 先排序、去重
  - sidecar 内保存为 relation-scoped stable locator slice
- query cache 期：
  - 首次读取 summary 文件后，为每个 relation entry 物化 public locator slice
  - 后续 lookup 直接复用该 slice，不再重复 `qsort`
- payload plan 期：
  - 先按 segment 去重，再读取 locator slice
  - 不再允许碎片 `payload_base_windows` 重复驱动同一 segment lookup

## 为什么不继续只修 lookup 热路径

- 仅在 payload plan 外层加少量去重，仍会把“排序/去重”留在 query 热路径
- 仅缓存原始磁盘 locator section，仍然挡不住每次 lookup 的 public slice 物化与排序
- 当前现场已经证明真正昂贵的是“查询期现拼现排”，因此需要把排序/去重前移到
  build/cache 生命周期，而不是继续堆叠 query-side 微优化

## 后果

优点：

- payload locator 查询期复杂度从“窗口数 x lookup x 排序”收敛为：
  - 每 segment 最多一次 lookup
  - 每 relation entry 最多一次 public slice 物化
- 保留 `ADR-0027` 的 locator-first 语义收益
- 不改变 WAL fallback / recent tail / summary service 清理模型

代价：

- summary build 期新增每 relation payload locator 排序/去重成本
- query cache 结构需要扩展 relation-scoped public locator slice
- 调试观测与回归需要补齐，避免未来再次把重复排序带回 query 热路径

## 语义要求

- stable locator slice 仍然只是性能缓存，不是 correctness 唯一来源
- sidecar 缺失、损坏、旧版本或 recent tail 未覆盖时，必须继续安全 fallback
- query 路径不允许因 stable slice 缺失产生 payload 假阴性或漏 record
- 任何 payload locator 方案优化都不得改变 replay / reverse-op / apply 语义
