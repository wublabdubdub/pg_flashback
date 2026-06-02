# ADR-0033：3/9 Build Index Query-Side Stable Slice 与 Exact Xid Fill

## 状态

已采纳（2026-04-07）

## 背景

在 release gate 现场
`./run_release_gate.sh --from run_flashback_checks`
下，`3/9 build record index` 仍出现长时间停留。

2026-04-07 本机现场复核确认：

- 最近几轮优化主要收敛的是：
  - payload locator batching
  - locator-only payload stub
  - reusable WAL record materializer
- 这些优化已经明显压缩了 `3/9 payload`，但新的主耗时已经前移到：
  - `summary-span`
  - `xact-status`

现场证据：

- `pg_flashback_summary_progress` 显示：
  - `last_query_summary_ready = t`
  - `last_query_summary_span_fallback_segments = 0`
  - `last_query_metadata_fallback_segments = 0`
- backend 现场 `wait_event = WalRead`
- `perf` 栈落在：
  - `fb_wal_fill_xact_statuses_serial`
  - `fb_wal_visit_window`
  - `XLogReadRecord`
  - `fb_wal_read_page`

进一步读码确认：

- `summary-span` 当前仍由 `fb_build_summary_span_visit_windows()`
  逐 segment 调 `fb_summary_segment_lookup_spans_cached()`
- `fb_summary_segment_lookup_spans_cached()` 在 query 期仍会为匹配 relation
  重新 `palloc + copy` 一份 span 数组
- `xact-status` 当前仍先在 source windows 上读取 xid outcome slice；
  只要仍有 unresolved xid，就继续走 segment 级 WAL fallback

这说明：

- 现有 payload 优化并没有失效
- 只是 `3/9` 的主热点已经换成 query-side control-plane 路径

## 决策

将 `3/9 build record index` 的后续优化主线固定为两条：

### 1. `summary-span` 改成 stable public slice

- summary cache 期为 relation spans 暴露 stable public slice
- query 侧禁止继续对 spans 做 per-call `palloc + copy`
- `fb_build_summary_span_visit_windows()` 只做：
  - segment 去重
  - span 裁窗
  - window merge

### 2. `xact-status` 改成 metadata-spool-first + summary-first + exact-fill + bounded fallback

- serial metadata 主路径先产出 query-local `xact_summary_log`
- `fb_wal_fill_xact_statuses_serial()` 先消费这份 spool
- 继续保留 summary-first，但只对 spool 未回答的 xid 再读 summary
- 对 unresolved xid，优先走精确补洞，而不是立即回退到整段 WAL 扫描
- 只有精确补洞仍无法回答时，才允许继续原始 WAL fallback
- fallback 继续保持 unresolved xid 驱动，不再把大窗口直接放大成主要执行路径

## 为什么不继续只压 payload

- 现场已经确认 payload 不再是 `3/9` 第一热点
- 继续只做 payload 微优化，不会改变 `summary-span` 与 `xact-status`
  的 query-side 退化结构
- release gate 目标是压缩整个 `3/9`，而不是只压其中单一子阶段

## 结果

预期收益：

- `summary-span` 不再把时间浪费在 query 期重复复制 relation spans
- `xact-status` 不再因为少量 unresolved xid 就直接退化成大范围 `WalRead`
- metadata 与 xact-status 之间不再重复丢失已经顺手扫到的 `RM_XACT_ID` outcome
- `3/9` 总时长从“多个子阶段轮流冒头”收敛为稳定的小常数阶段

## 后果

正向：

- 优化点直接打在当前 release gate 的真热点
- query-side 行为更接近“消费 stable summary view”，而不是“重新拼 summary”
- 为后续把 `3/9` 压到可预测范围提供统一主线

代价：

- `fb_summary` query cache 会变复杂，需要维护 public span slice
- `xact-status` 需要新增一层 metadata xact spool / exact-fill 逻辑与观测字段
- 需要补新的回归和 release gate 现场验证

## 后续

- 为 `summary-span` 新增 stable slice / segment-dedupe 观测
- 为 `xact-status` 新增 exact-fill / WAL fallback 观测
- 直接用 release gate 现场复跑：
  - `./run_release_gate.sh --from run_flashback_checks`
- 若 `3/9` 仍无明显改善，继续迭代，不以单轮实现结束
