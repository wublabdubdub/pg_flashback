# ADR-0032：可复用 WAL record materializer 与 locator/deferred 通用物化

## 状态

已采纳（2026-04-06）

## 背景

在 live case
`scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
上，`ADR-0031` 的 `locator-only payload stub` 已把 `3/9 payload`
压到约 `1s`，但新的主热点右移到 `4/9 replay discover`。

现场确认：

- `record_count` 与 `summary_payload_locator_records` 都达到 `4530229`
- discover `round=1` 正在顺着整个 `record_log` 消费 locator-only stub
- `fb_wal_record_cursor_read()` 命中 stub 后，会逐条调用
  `fb_wal_load_record_by_lsn()`
- 该路径当前每条记录都会：
  - 新建 `XLogReader`
  - 重新解析 archive dir
  - 重新 `open/fadvise/close` segment
- `perf` 栈顶热点主要落在：
  - `fb_wal_record_cursor_read`
  - `fb_wal_load_record_by_lsn`
  - `fb_wal_open_segment`
  - `fb_open_file_at_path`
  - `posix_fadvise`

同时，类似浪费不只出现在 `4/9`：

- locator-only stub 在 replay discover / warm / final / anchor fallback 中都会走同一套逐条物化
- `payload_deferred` 的后续按需补 payload 也仍是 per-record 新建 reader
- 当前 `locator-only payload stub` 还会把 `precomputed_missing_blocks` 清零，
  使 discover shortcut 失效

## 决策

新增一套通用的 `WAL record materializer`，作为 `FbWalRecordCursor` 与
deferred payload 的底层共享能力，而不是为 `4/9` 单独加执行器快路径。

统一约束如下：

- 由 WAL 层提供可复用的 reader/materializer 对象
- 同一条查询内，locator-only 与 deferred payload 优先复用同一套：
  - `XLogReaderState`
  - `FbWalReaderPrivate`
  - 当前打开 segment
  - 已解析的 archive source 配置
- 对“顺扫窗口”和“按 LSN 单条/稀疏物化”区分 open hint：
  - 顺扫窗口保留 `SEQUENTIAL/WILLNEED`
  - locator/deferred 稀疏物化禁止每次 open 都做同样的 aggressive fadvise
- `FbWalRecordCursor` 对 locator-only 条目先返回轻量 record skeleton，
  仅在调用方确实需要 payload/body 时再做按需物化
- replay discover / warm / final / anchor fallback 与 deferred payload
  统一走同一套通用物化接口
- locator-only 模式下继续保留 `precomputed_missing_blocks` 或其等价轻量信息，
  不再因为 stub fast path 直接丢失 discover shortcut 所需信号

## 结果

预期收益：

- 消除 discover 阶段 per-record `Allocate/open/fadvise/close`
- 避免把 `3/9` 省下的 decode 以更差的随机 IO 形式转移到 `4/9`
- 让 replay discover、anchor fallback、deferred payload 共用同一套优化
- 保持执行器与用户 SQL 形态不变，优化收敛在 WAL 物化内核

## 后果

正向：

- 优化点位于通用 WAL 物化层，天然覆盖多个阶段
- 后续可继续在该层追加：
  - reader reset/reuse 统计
  - 轻量 skeleton/stub 扩展
  - 物化批量策略

负向 / 代价：

- `FbWalRecordCursor` 与 `fb_wal.c` 结构会更复杂
- 需要补专门 contract test，避免 reader 生命周期与 payload 物化语义回退

## 后续

- 为 materializer 复用补最小 RED：
  - locator-only cursor 连续读取不再每条重建 reader
  - deferred payload 连续物化不再每条重建 reader
  - locator-only 路径重新保留 discover shortcut 所需预计算缺页信息
- 用 live case 复核总时长：
  - `select * from pg_flashback(NULL::scenario_oa_50t_50000r.documents, '2026-04-04 23:40:13') limit 100`
  - 目标整体 `< 50s`
