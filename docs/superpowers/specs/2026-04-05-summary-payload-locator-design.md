# Summary Payload Locator Design

## 目标

在保持当前 summary 模型不变的前提下，把 `3/9 build record index` 的 payload
阶段从“按 relation spans 缩窗后仍顺序 decode 大量无关 record”推进到“按 summary
提供的精确 payload locator 定点 decode”。

本轮固定目标：

- 显著降低 `payload_scanned_records`
- 保持当前 replay / apply / reverse-op 语义不变
- 保持“一段 WAL 一个 summary sidecar”的模型不变
- 同时兼顾 summary build 吞吐与 sidecar 文件大小

## 当前问题

当前 summary 已提供：

- relation spans
- xid outcomes
- touched xids
- unsafe facts
- block anchors

但 payload 侧仍然依赖：

1. 从 spans 生成 payload windows
2. 为安全处理跨 segment record 头，把 read window 拉回到 segment slice 起点
3. 在 window 内顺序 decode，并在 visitor 中筛出目标 relation record

因此 summary 当前做到的是：

- `relation locate`

还没有做到：

- `payload record locate`

这也是为什么 live case 里 `payload_kept_records` 已经不大，但
`payload_scanned_records` 仍很高。

## 推荐方案

在 summary sidecar 内新增 relation-scoped payload locator section。

每条 locator 记录：

- relation slot
- record kind
- 轻量 flags
- `record_start_offset`

约束：

- offset 固定为当前 segment 内偏移
- locator 只记录“payload materialize 需要 decode 的 record 起点”
- 不复制 main data / block image / block data
- 不依赖全局索引或额外 manifest

## 查询期数据流

查询期 `fb_wal_build_record_index()` 的 payload phase 改为：

1. 仍先用 summary spans / windows 收敛 payload 相关 segment 范围
2. 对每个 payload segment 优先读取 relation payload locators
3. 如果该 segment 有 locator：
   - 直接把 locator 追加到 payload candidate stream
   - payload decode 改为按 locator 顺序定点读取 record
4. 如果该 segment 没有 locator：
   - 回退到现有 window/sparse 读路径

这保证：

- summary locator 是性能增强，不是 correctness 前提
- recent tail、坏 summary、未预建 segment 仍可查询

## 为什么会明显提速

因为当前 payload 主复杂度接近：

- `O(window 内所有 WAL record decode)`

而 locator-first 后更接近：

- `O(target relation payload records decode)`

对噪音 WAL 很多的 live case，区间内无关 decode 会被直接拿掉。

## 文件大小与构建速度控制

本轮控制原则：

- section 使用 segment-relative `uint32 offset`
- 不额外写 `end_lsn`
- 不写完整 locator / reltag / xid
- 与 relation entries 复用 slot 映射
- 默认按扫描顺序 append，避免 build 期全量排序

如后续测得 section 体积偏大，再追加：

- delta offset 编码
- varint 编码
- section 级压缩

但首版先保持实现简单，优先验证查询期收益。

## 回归与观测

需要新增或扩展：

- summary v3/v7 相关回归，断言 summary payload locator 可用
- `fb_recordref_debug()` 输出：
  - `summary_payload_locator_hits`
  - `summary_payload_locator_fallback_segments`
  - `payload_locator_records`
- payload live case 对照：
  - `payload_scanned_records`
  - `payload_kept_records`
  - `3/9 payload` 耗时

## 风险

- PG10-18 上 `XLogReader` 定点读取兼容差异
- locator 命中路径与 tail inline / recovered_wal recent tail 的拼接
- summary 版本前滚后需要清空旧 meta，避免旧 sidecar 掩盖效果
- 极端 dense case 下 locator 数接近 payload record 数，可能抬高 summary build
  和 sidecar 体积，但仍应优于查询期重复 decode 无关 record
