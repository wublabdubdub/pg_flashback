# ADR-0034：启用 Bounded Raw Xact WAL Fallback 并行填充

## 状态

已采纳（2026-04-09）

## 背景

在 PG14 / release gate 现场
`scenario_oa_50t_50000r.documents @ '2026-04-09 06:25:40.377546+00'`
上，`3/9 build record index` 仍表现为长时间停留。

现场取证确认：

- `fb_summary_xid_resolution_debug(...)` 返回：
  - `summary_hits=19979`
  - `summary_exact_hits=9`
  - `unresolved_touched=86`
  - `fallback_windows=23`
- 同 case 的 `fb_recordref_debug(...)` 返回：
  - `xact_summary_spool_records=0`
  - `xact_summary_spool_hits=0`
  - `summary_xid_fallback=77`
  - `xact_fallback_windows=23`
  - `xact_fallback_covered_segments=836`
- 对 live backend 做 `gdb`，真实热点落在：
  - `fb_wal_fill_xact_statuses_serial`
  - `fb_wal_visit_window`
  - `XLogReadRecord`
  - `WALRead`

进一步读码确认：

- 本轮 `metadata_fallback_windows=0`
  ，metadata 期没有顺手扫描 WAL，因此 query-local
  `xact_summary_log` 为空
- summary-first 与 exact-fill 之后仍有少量 unresolved xid
- 现有 raw xact fallback 虽已有 worker 实现
  `fb_wal_fill_xact_statuses_parallel()`
  ，但函数入口被硬编码 `return false;`
- 结果是 bounded fallback windows 已经收窄到 `23`
  个，却仍由单 backend 串行扫描 `836` 个 covered segments

## 决策

启用 bounded raw xact fallback 的并行 worker 路径，并保持现有语义不变：

- 仅在以下条件同时满足时尝试并行：
  - unresolved xid fallback 集非空
  - fallback window 数量大于 `1`
  - `parallel_workers > 1`
  - 当前查询存在可用 `spool_session`
- 并行 worker 继续只消费 `RM_XACT_ID`
  记录，不改变 relation / payload / replay 语义
- worker 输出继续合并回同一份 `xid_statuses`
  与 target commit/abort 计数
- 任一条件不满足，或 worker 启动不足时，必须安全回退 serial
- leader backend 也必须参与 fallback window 扫描，
  不允许只等待 worker 退出
- worker 启动采用 all-or-fallback：
  若无法按本轮规划完整拉起，则整体回退 serial，
  不允许 partial launch 后继续执行

## 为什么不是继续只压 summary exact

- 这次现场 `summary-first + exact-fill` 已经工作，
  只是仍留下 `77` 个 unresolved xid
- 真正把时延放大的不是“是否还有 unresolved”，而是
  “剩余 fallback 仍被强制单线程串行扫完”
- 当前代码里已有并行 worker 主体，问题是主路径把它永久禁用了

## 结果

预期收益：

- PG14 release gate 上，少量 unresolved xid 不再自动退化成
  单 backend 长时间 `WALRead`
- 保持 bounded fallback 语义不变，只降低完成这段 fallback 的 wall time
- 为后续继续压缩 `3/9 xact-status`
  留下更稳定的观测基线
- 即使本轮只拿到 1 个动态 worker，
  也能通过 leader + worker 形成有效两路扫描

## 后果

正向：

- 改动点集中在现有 raw fallback 路径
- 不需要改变 summary 文件格式
- 不改变 unresolved xid 的 correctness 语义

代价：

- 需要维护 serial / parallel 两套 fallback merge 路径的一致性
- 需要补观测与回归，避免 worker 启停失败时静默退化
- 需要保证 leader 参与后，本地执行与 bgworker 执行的
  `xid_statuses` / unsafe merge 语义完全一致

## 后续

- 补最小回归或 debug 观测，锁住“bounded xact fallback 可以真正进入并行”
- 直接在 PG14 release gate 现场复跑
  `random_flashback_1.documents`
  验证 `3/9` 是否明显收敛
