# pg_flashback Summary v3 Segment Index Design

## 目标

将当前只服务 segment 级 prefilter 的 `summary-*.meta` 升级为“紧凑 segment 索引”，在不引入全局单体索引的前提下，把更多查询期开销前移到 sidecar 构建期，重点提升：

- relation 相关 WAL record 的定位能力
- touched xid 的提交/回滚状态可复用能力
- `pg_flashback()` 主查询链中 `build record index` 阶段的整体吞吐

本次设计固定目标：

- 保持“一段 WAL 对应一个 sidecar”模型
- 保持 sidecar 可再生、可丢弃、可按 segment 独立失效
- 查询侧优先依赖 segment index；失效或缺失时回退现有路径
- 显著减少“命中 segment 后仍需整段 record decode”的情况
- 显著减少“查询期再扫 WAL 回填 xid status”的情况

## 非目标

- 不引入跨所有 segment 的全局单体索引
- 不把索引构建挂到 PostgreSQL WAL 写入/提交热路径
- 不在首版记录逐条 record 的全量 posting list
- 不在首版改变 replay / reverse-op / apply 的语义
- 不让 segment index 成为 flashback 查询唯一可用性前提

## 现状问题

当前 `summary-*.meta` 只保存：

- `locator_bloom`
- `reltag_bloom`
- 少量时间边界与 flags

它只能回答：

- 这个 WAL segment 是否“可能”和目标 relation 有关

不能回答：

- 目标 relation 的相关 record 位于 segment 内哪个 LSN 区间
- touched xid 的提交/回滚状态是什么

因此当前查询虽然能跳过很多无关 segment，但一旦某个 segment 命中：

- 仍需在查询期对该 window 内的 record 做顺序 decode
- 仍需再做一轮 `xact status` WAL 扫描来补 touched xid 的事务结果

这两部分成本仍集中落在 `build record index` 阶段。

## 总体方案

将 `summary` 升级为 `summary v3`，保持 segment 粒度，但从“极小粗筛摘要”演进为“紧凑分层索引”：

1. 保留现有 bloom gate，继续承担极低成本的 segment 级排除。
2. 为 segment 内出现过的 relation 建本地字典。
3. 为每个 relation 保存若干“相关 WAL LSN 区间（relation spans）”。
4. 为 segment 内解析到的事务结果保存 `xid outcome table`。
5. 查询侧命中 segment 后，不再默认整段 decode，而是仅解码目标 relation 对应 spans。
6. 查询侧补 touched xid 状态时，优先从 sidecar 读取 outcome；仅对缺口回退旧 WAL 扫描路径。

## 为什么采用 segment 级而不是全局索引

- 当前 WAL 来源解析、失效、清理都天然以 segment 为边界
- 单个 segment sidecar 损坏时只需局部重建
- 可继续复用现有 summary service 的 worker 模型
- 避免引入全局 manifest / compaction / 复杂并发写问题

这不是放弃全局索引能力，而是把“跨 segment 聚合”留给查询期在内存中完成，把持久化复杂度留在 segment 内。

## 文件格式

`summary v3` 仍使用 `summary-<identity>.meta` 文件名，但升级版本号与内部布局。

### Header

保留：

- `magic`
- `version`
- `source_kind`
- `file_identity_hash`
- `timeline_id`
- `wal_seg_size`
- `segno`
- `built_at`

保留并继续使用：

- `oldest_xact_ts`
- `newest_xact_ts`
- `flags`

新增：

- `section_count`
- `section directory`
- `dictionary_count`
- `span_count`
- `xid_outcome_count`

### Section A: Bloom Gate

继续保留：

- `locator_bloom`
- `reltag_bloom`

语义不变：

- 明确不命中则整个 segment 直接排除
- 命中则进入更精细的 relation spans probe

### Section B: Relation Dictionary

为 segment 内出现过的 relation 建一个本地小字典，条目类型分两类：

- `locator` key
  - `spcOid/dbOid/relNumber`
- `reltag` key
  - `dbOid/relOid`

每个 relation 条目分配一个紧凑 `rel_slot`。

### Section C: Relation Spans

每个 `rel_slot` 对应若干 LSN 区间：

- `start_lsn_delta`
- `end_lsn_delta`
- `flags`

规则：

- 区间按 LSN 升序保存
- 同 relation 的连续或近邻 record 自动合并
- 优先按 record 真实 LSN 聚类，不保存逐条 posting
- 编码使用 `varint + delta`

首版目标是让查询能把 segment 内扫描收敛到少量 spans，而不是继续全段顺序 decode。

### Section D: Xid Outcome Table

记录 segment 内出现过的事务结果：

- `xid`
- `status`
  - committed
  - aborted
  - prepared
- `commit_ts`
- `commit_lsn`

编码策略：

- 按 `xid` 排序
- `xid` delta 编码
- `commit_ts` / `commit_lsn` delta 编码
- `status` 使用 bit-pack

首版不在 sidecar 中存 subxid 展开结果；查询期仍可按当前逻辑处理 subxid 语义。

### Section E: Optional Stats

保留或新增只读统计字段，便于调试与进度观测：

- relation 条目数
- spans 数
- xid outcome 数
- 近似压缩率

## 构建规则

summary worker 在读取 WAL segment 时，除现有 bloom 采样外，再额外做两类聚合：

### Relation Spans 构建

- 对每条与 relation 有关的 WAL record，确定其关联 relation key
- 将 `reader->ReadRecPtr .. reader->EndRecPtr` 视为一个候选 span
- 对同 relation 的连续/近邻 span 做 merge
- 最终输出按 `rel_slot` 分组的 spans

relation 识别来源包括：

- block tag 的 main fork locator
- `SMGR CREATE/TRUNCATE`
- `HEAP TRUNCATE`
- `HEAP2 REWRITE`
- `STANDBY LOCK`
- 主表与 TOAST relation

### Xid Outcome Table 构建

- 仅在 `RM_XACT_ID` 相关结果记录上写入 outcome
- outcome 以“最终事务结果”为目标，不记录无用中间状态
- 对同一 xid 的重复结果以最后可见结果收敛

## 查询侧使用方式

查询期分三层：

1. Bloom gate
   - 明确不命中则跳过 segment
2. Relation spans
   - 命中后只读取目标 relation 的 spans
   - 仅对 spans 覆盖范围做真实 WAL decode
3. Xid outcomes
   - touched xid 状态优先从 sidecar outcome 查
   - 只有 sidecar 缺失或 outcome 不足时才回退 WAL xact 扫描

## 回退语义

以下情况必须安全回退到旧路径：

- sidecar 缺失
- version 不匹配
- `file_identity_hash` 不匹配
- section 损坏或解析失败
- relation spans 缺失
- xid outcome 覆盖不足

回退要求：

- correctness 优先，不允许因 sidecar 缺失导致假阴性
- sidecar 解析失败不得改变查询结果，只允许损失性能

## 编码与空间约束

本次设计的核心不是“多存信息”，而是“高信息密度”：

- relation key 先字典化，再引用 `rel_slot`
- LSN / xid / timestamp 采用排序后 delta 编码
- 区间合并优先于逐条 posting
- 稠密布尔/枚举位采用 bit-pack

预期空间特征：

- 比现有 bloom-only summary 大
- 但显著小于逐 record 精确 posting
- 在高命中 relation 场景下，对查询延迟的收益明显高于空间成本

## 与现有模块的关系

### `fb_summary`

职责升级为：

- 生成 `summary v3`
- 提供 bloom gate probe
- 提供 relation spans 读取 API
- 提供 xid outcome probe API

### `fb_summary_service`

职责不变：

- 仍按 segment 构建 sidecar
- 仍按 `meta/summary` 做独立容量管理

### `fb_wal`

需要改造：

- prefilter 后不再默认整段读命中 segment
- 为窗口扫描引入“span-driven visit”
- xid status 填充优先消费 sidecar outcome

## 测试策略

### 文件级

- `summary v3` 读写 round-trip
- relation dictionary / spans / xid outcomes 编解码
- sidecar identity mismatch 正确回退

### 查询级

- 命中 relation 的单 segment 查询，不再整段 record 扫描
- 多 segment 查询，spans 能正确裁剪窗口
- touched xid 状态命中 sidecar 后，不再回扫 WAL
- sidecar 缺失 / 损坏时回退旧路径且结果一致

### 回归与观测

- 新增 debug 接口查看 relation spans / xid outcome 统计
- 扩展 `pg_flashback_summary_progress` 或 debug 输出，暴露 `summary v3` 构建覆盖度

## 风险

- spans 过粗时收益有限
- spans 过细时 sidecar 膨胀
- xid outcome 不足时仍需保留旧回退路径
- 必须严格避免“sidecar 漏记导致查询假阴性”

## 决策

本次固定采用：

- segment 粒度
- bloom gate + relation spans + xid outcomes
- 允许 sidecar 变大，但优先通过字典化、区间合并、delta/varint 编码保持高信息密度
- 查询 correctness 不依赖 sidecar；sidecar 只负责显著提速
