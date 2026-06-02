# ADR-0041：建立 3/9 通用 single-pass payload metadata contract

## 状态

已采纳（2026-04-12）

## 背景

在 `ADR-0038` 移除 `locator stub + stats sidecar` 后，
`scenario_oa_50t_50000r.documents @ '2026-04-11 07:40:40.223683+00'`
的 live 现场虽然已从 `140s+` 降到约 `28s`，但仍明显高于目标。

2026-04-12 晚间再次在 14pg `alldb` 原样复现，观察到：

- `3/9 payload ~= 24.023s`
- 随后 preflight 因 `estimated=1.396GB > 1GB` 失败
- `pg_flashback_summary_progress` 显示：
  - `last_query_summary_ready = t`
  - `last_query_summary_span_fallback_segments = 0`
  - `last_query_metadata_fallback_segments = 0`

因此慢点并不是 summary fallback。

live `gdb` 直接取到 backend 栈：

- `fb_wal_finalize_record_stats()`
- `fb_wal_record_cursor_read_skeleton()`
- `fb_wal_load_record_by_lsn()`
- `XLogReadRecord() -> WALRead() -> pread64()`

根因已经收口为：

- 当前代码虽然已经具备“首次 payload 访问时顺手产出 metadata”的能力
- 但这条能力只在部分 payload 路径启用
- pure locator 场景仍会在 `3/9` 尾部额外调用
  `fb_wal_finalize_record_stats()`
- 形成一次完整的 cursor/WAL 二次全量回读

这说明当前 `3/9` 还没有真正建立“首次 payload 访问就是唯一真源”的通用契约。

## 决策

建立通用 `single-pass payload metadata contract`：

1. locator
2. sparse windows
3. windowed fallback

这三类 payload 主链统一遵守同一条规则：

- 首次 payload 访问就是 metadata 唯一真源
- 在这次访问里同步产出：
  - `record stats`
  - `replay block metadata`
  - `precomputed missing-block facts`

`fb_wal_finalize_record_stats()` 只保留为严格兜底：

- 仅当某条 payload 路径无法证明 metadata 完整时才允许触发
- 正常 locator / sparse / windowed 主链默认都不再触发

## 具体约束

- 不允许再把 pure locator 视为“统计例外”
- 不允许只修 locator 特例而让 sparse / fallback 继续保留二次回读
- mixed locator + fallback window 必须共用同一套增量 collector，
  不能拆成两套统计口径
- `3/9` 新增的热路径代价只能是顺手 hash / counter 更新，
  不能再次退化成整条 cursor / WAL 第二遍读取

## 为什么这样做

- 真正昂贵的是第二遍 `XLogReadRecord()/pread64()`，不是顺手记几份元数据
- 只删掉某一条 `finalize` 调用是不够的，必须建立“metadata completeness”
  的统一契约，否则 mixed 场景仍会重新掉回兜底
- 这条契约是后续继续压 `4/9` / `6/9` 的前置条件：
  先把 `3/9` 的重复读取清干净，后续热点才不会混淆

## 结果

预期收益：

- `3/9` 在通用场景下默认不再做 payload 二次全量回读
- `documents` 这类大窗口 live case 的 payload 成本继续显著下降
- preflight 估算不再被“首次访问 + finalize 重扫”双份统计放大

接受的代价：

- payload 首次访问的热路径会多做少量增量 metadata 维护
- 这是可接受的 CPU/hash 成本，远低于再做一遍 WAL reread

## 后续

- 先补 contract regression，锁住：
  - 正常 payload 主链 `finalize_used = false`
  - metadata 仍保持完整
- 再把 locator-only / sparse / windowed 的增量 collector 统一为一个通用入口
- 完成后复跑 14pg live `documents` 现场，继续把 `3/9` 向目标值收口
