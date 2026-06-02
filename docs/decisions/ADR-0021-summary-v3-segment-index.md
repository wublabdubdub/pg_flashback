# ADR-0021 Summary v3 升级为紧凑 segment 索引

## 状态

已接受

## 背景

当前 `summary-*.meta` 只承担 segment 级 relation prefilter：

- 可排除大量无关 WAL segment
- 但一旦 segment 命中，查询期仍需对命中 window 顺序 decode WAL record
- touched xid 的事务状态仍需在查询期再扫一轮 WAL 回填

这使得 `build record index` 仍承担明显的现场解析成本，summary 加速等级不够。

## 决策

将现有 summary 升级为 `summary v3`，保持“一段 WAL 一个 sidecar”，但在 segment sidecar 内增加两类紧凑索引：

- relation spans
  - 为 segment 内出现过的 relation 保存若干相关 LSN 区间
- xid outcomes
  - 为 segment 内解析到的事务结果保存 `xid -> status/commit_ts/commit_lsn`

保留：

- `locator_bloom`
- `reltag_bloom`

查询期固定语义：

- 先用 bloom 排除无关 segment
- 再用 relation spans 缩小 segment 内的真实 WAL decode 范围
- touched xid 状态优先从 sidecar outcome 获取
- sidecar 缺失、失效或覆盖不足时回退旧 WAL 扫描路径

## 为什么不采用全局单体索引

- 当前 WAL 来源解析、失效和清理都天然以 segment 为边界
- segment sidecar 更易重建、排障和局部失效
- 避免引入全局索引的 manifest / compaction / 并发写复杂度

## 后果

优点：

- 查询期不再只停留在“跳段”，还能显著减少命中 segment 内的无效 decode
- touched xid 状态可复用，减少第二轮 xact WAL 扫描
- 与现有 summary service 架构兼容

代价：

- sidecar 文件体积会明显大于现有 bloom-only summary
- 构建期 CPU 与内存开销上升
- 需要新增编码、回退与回归覆盖

## 语义要求

- sidecar 仍然是可再生缓存，不是 correctness 唯一来源
- 不允许因 sidecar 缺失或损坏产生查询假阴性
- relation spans 与 xid outcomes 都必须允许查询期按需回退
- 仍不引入跨 segment 的全局单体索引文件
