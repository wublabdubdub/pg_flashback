# ADR-0031：并行 locator-first 路径上的 locator-only payload stub

## 状态

已采纳（2026-04-06）

## 背景

在 live case
`scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
上，`summary payload locator-first` 已经把查询期 locator 规划热点和
无关 WAL decode 清掉，但 `3/9 100% payload` 仍长期停留在三十秒级。

现场确认：

- `summary_payload_locator_records` 规模达到百万级
- 即使 payload body 不再完整落盘，只要 `3/9` 仍对全部 locator 做
  WAL decode / record skeleton materialize，`100% payload` 仍然过重
- 用户现场要求以 `SELECT * ... LIMIT 100` 为代表的 live case
  把 `3/9` 压到 `< 20s`

## 决策

在以下条件同时满足时：

- 当前走 `summary payload locator-first`
- locator plan 已完整覆盖本轮 payload，不需要 fallback windows
- `parallel_workers > 1`

新增 `locator-only payload stub` fast path：

- `3/9` 不再为 locator-first 路径全量物化 payload body
- build 期只把 `record_start_lsn` 级 stub 落到 record spool
- spool cursor / replay 侧在真正消费到该条记录时，再按 LSN 回填真实
  `FbRecordRef`

## 结果

- 2026-04-06 本机 PG18 live 复核：
  - SQL：
    `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
  - `3/9 55% xact-status` 约 `15-16s`
  - `3/9 100% payload` 约 `1s`
  - `3/9` 累计约 `17-18s`

## 后果

正向：

- `100% payload` 从三十秒级压到约 `1s`
- `3/9` 主瓶颈重新收敛为 `xact-status`

负向 / 待收敛：

- `precomputed_missing_blocks` / discover-round shortcut
  与该 fast path 仍有一处交互待继续收敛
- 当前 `fb_replay` 目标回归里，
  `skips_discover_rounds` 仍出现 `f` 的差异

## 后续

- 补最小 RED，锁住 `locator-only payload stub` 下的 discover shortcut 契约
- 收敛 `precomputed_missing_blocks` 在新 fast path 下的保留策略
