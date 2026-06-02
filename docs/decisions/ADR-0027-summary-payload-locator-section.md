# ADR-0027 Summary Payload Locator Section

## 状态

已接受

## 背景

当前 `summary v7` 已经把 `3/9 build record index` 的主链推进到：

- segment 级 bloom prefilter
- relation spans
- xid outcomes
- relation-scoped touched xids / unsafe facts
- relation-scoped block anchors

这套模型已经明显降低了“整段全扫”的成本，但 payload 主链仍有稳定瓶颈：

- summary 提供的是 `relation spans`
- 查询侧 payload 仍需在 span/window 内顺序 decode WAL record
- 对噪音 WAL 很多的 live case，`payload_scanned_records` 仍远高于
  `payload_kept_records`

典型现场已经确认：

- `scenario_oa_50t_50000r.approval_comments @ 2026-04-04 23:40:13`
  - `payload_scanned_records ~= 8.2M`
  - `payload_kept_records ~= 171k`
- 当前剩余热点不再主要来自 segment 命中范围，而是 payload window
  内部仍顺序 decode 了大量无关 record

## 决策

保持当前 summary 模型不变：

- 仍然是“一段 WAL 一个 summary sidecar”
- 仍然是 query-side 可回退的可再生缓存
- 不引入跨 segment 的全局索引

在此基础上新增一类 relation-scoped section：

- `payload locators`

其语义固定为：

- 对当前 payload materialize 会捕获的 relation-scoped WAL record，
  在所属 segment summary 中记录精确的 record 起点
- locator 以当前 segment 内偏移表示，不额外复制完整 payload
- 查询期优先从 summary locator section 构建 payload candidate stream
- 仅对 locator 缺失、summary 缺失、版本不匹配或 recent tail 未覆盖的部分，
  回退到现有 span/window decode 路径

## 为什么不继续只优化 spans

- span 仍然是区间信息，不是精确 record 入口
- 查询期即使窗口已经很窄，也仍要顺序 decode 区间内所有 record
- 继续仅在 span/window 合并策略上收缩，收益上限已明显逼近
- 当前需要解决的是“区间内无关 decode”，而不是“区间数量太多”

## Locator 格式原则

- 仍按 relation slot 分组
- 只记录 payload 需要的最小入口信息
- 默认使用 segment-relative offset，避免重复写完整 `XLogRecPtr`
- 优先按构建顺序 append，避免额外排序和高内存聚合
- 若文件体积有压力，可在 section 内引入轻量压缩/差分编码，但不改变
  “一段一 summary”的上层模型

## 后果

优点：

- 查询期 payload 可以从“按 span/window 顺扫”升级为“按精确 record 入口定点 decode”
- `3/9 payload` 的主耗时将从“无关 record decode”切到“目标 relation record decode”
- 保持当前 summary service、清理、探活、fallback 模型不变

代价：

- summary 文件格式需要再升版本
- summary build 会新增 locator 收集和写盘成本
- sidecar 文件体积会上升，需要继续控制 section 编码大小
- 查询期需要同时维护 locator-first 与 window fallback 两套路径

## 语义要求

- summary 仍然不是 correctness 唯一来源
- 缺 locator 不允许导致假阴性或漏 record
- current / recent tail 未被 summary 覆盖时，必须继续走现有安全 fallback
- locator section 只服务 payload，不改变 replay / apply / reverse-op 语义
