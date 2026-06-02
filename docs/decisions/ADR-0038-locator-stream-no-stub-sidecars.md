# ADR-0038：Full Locator-Cover 路径改为 Direct Locator Stream，移除 Stub / Stats Sidecar 临时文件

## 状态

已采纳（2026-04-12）

## 背景

在 PG14 live 现场
`scenario_oa_50t_50000r.documents @ '2026-04-11 07:40:40.223683+00'`
上，原始 SQL：

```sql
select * from pg_flashback(NULL::"scenario_oa_50t_50000r"."documents",
                           '2026-04-11 07:40:40.223683+00')
order by id;
```

2026-04-12 实机复核结果：

- 总耗时在 `140.245s` 时因 `pg_flashback.memory_limit = 1GB` 触发 preflight 失败
- 其中 `3/9 build record index` 的已见分段为：
  - `0% prefilter ~= 35.246s`
  - `10% summary-span ~= 2.771s`
  - `30% metadata ~= 10.190s`
  - `55% xact-status ~= 2.835s`
  - `100% payload ~= 88.368s`
- `pg_flashback_summary_progress` 同时显示：
  - `last_query_summary_ready = t`
  - `last_query_summary_span_fallback_segments = 0`
  - `last_query_metadata_fallback_segments = 0`

也就是说，本次主瓶颈不是 summary 缺失或 query-side fallback，而是
“summary 已命中后的 locator-only 热路径”。

现场取证进一步确认：

- `gdb` 栈显示 leader 长时间停在：
  - `WaitForBackgroundWorkerShutdown`
  - `fb_wal_payload_wait_worker`
  - `fb_wal_capture_locator_stub_stats_parallel`
- `perf` 栈显示 CPU 主要消耗在：
  - `pwrite64`
  - `fb_index_append_locator_stub`
  - `fb_wal_capture_locator_stub_stats_parallel`
  - `fb_wal_merge_payload_stats_log`
  - `fb_wal_merge_missing_blocks`
- 当前 full locator-cover 路径会连续做三轮大搬运：
  1. leader 把全部 locator 逐条写成 `record_log` stub
  2. worker 将 payload stats / missing-block 写成 sidecar 临时文件
  3. leader 再把这些临时文件完整回读并 merge

对本 case，`locator_count` 已达到数百万级，这套模型把
`summary payload locator-first` 省下来的无关 WAL decode，又重新转换成了
大规模临时文件 I/O 与 query-local working set 膨胀。

## 决策

对以下条件同时满足的 full locator-cover case：

- `payload_locator_count > 0`
- `payload_locator_fallback_base_count == 0`
- 非 `count_only`

将查询期执行模型从：

```text
summary locator slice
  -> append locator-only stub to record spool
  -> parallel worker writes stats/missing sidecar files
  -> leader re-reads sidecar files and merges
  -> replay cursor materializes stub by LSN
```

改为：

```text
summary locator slice
  -> publish direct locator stream on FbWalRecordIndex
  -> parallel worker returns aggregate stats/missing facts in memory
  -> replay cursor consumes virtual locator head stream directly
  -> materializer loads WAL record by LSN only when cursor/replay actually reads it
```

具体约束如下：

- full locator-cover 路径不再向 `record_log` 逐条写 locator stub
- `FbWalRecordIndex` 直接持有当前查询的 locator stream 及其计数
- `FbWalRecordCursor` 必须支持“虚拟 locator head + 真实 tail spool”混合读取
- stats-only worker 不再写：
  - `wal-payload-stats-worker-*.bin`
  - `wal-payload-missing-worker-*.bin`
- worker 仅通过 DSM / task state 回传：
  - payload 统计聚合值
  - missing-block 聚合结果
  - reader reset/reuse 计数
- fallback windows、tail inline payload、deferred payload、replay/apply correctness
  语义保持不变

## 为什么不继续只做 stub/sidecar 微优化

- 当前 `3/9 payload` 的主耗时已经不是单点函数常数，而是整条
  “stub 落盘 -> sidecar 落盘 -> leader 回读” 数据路径
- 继续优化单个写入点，无法同时解决：
  - `88s` payload 时长
  - `1.8GB+` 预估 working set
  - leader 等待 worker + merge 的串行收尾
- 只有去掉整条 tempfile 模型，才能同时打掉 wall time 和 memory root cause

## 结果

预期收益：

- full locator-cover case 不再为数百万 locator 额外创建 record stub
- stats-only 并行路径不再产生 query-hot-path sidecar 临时文件
- replay / final / apply 改为直接消费 locator stream，减少 `BufFileWrite/Read`
- `3/9 payload` 耗时与预估 working set 同时明显下降
- 为将目标 SQL 压到 `< 60s` 提供唯一足够大的收益面

## 后果

正向：

- 优化点集中在 `fb_wal` / cursor / worker 协议，不改变用户接口
- `summary payload locator-first` 的精确选择语义保持不变
- 复用现有 materializer，避免再引入第二套 replay 物化入口

代价：

- `FbWalRecordIndex` / `FbWalRecordCursor` 会新增一套“虚拟 head stream”状态
- 并行 worker 的 DSM 返回结构会更复杂，需要显式合并 missing-block 结果
- 必须补契约回归，避免未来再次退回 stub / sidecar tempfile 路径

## 后续

- 补 regression-only contract：
  - full locator-cover path 的 `record_log` head item count 为 `0`
  - locator stream count 与计划出的 locator count 一致
  - stats-only path 不再依赖 sidecar spool 文件
- 14pg 复跑原始 SQL，确认：
  - `3/9 payload` 不再由 `BufFileWrite/Read` 主导
  - 总时长继续向 `< 60s` 收敛
