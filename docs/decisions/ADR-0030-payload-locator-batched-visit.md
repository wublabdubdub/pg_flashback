# ADR-0030 Payload Locator Batched Visit With Exact Record Filtering

## 状态

已接受

## 背景

`ADR-0027` / `ADR-0028` 已经把 `summary payload locator-first` 推进到：

- build 期 relation-scoped locator slice
- query cache 期 public slice 复用
- query plan 期 segment 去重

这批优化已经把 `3/9 build record index` 的旧热点从：

- `fb_build_summary_payload_locator_plan`
- `fb_summary_segment_lookup_payload_locators_cached`
- `pg_qsort`

移走。

但在 live case
`scenario_oa_50t_50000r.documents @ '2026-04-04 23:40:13'`
上，新的长尾已经转移到 payload locator 访问本身：

- `fb_wal_visit_payload_locators()` 当前仍对每条 locator 单独：
  - `XLogBeginRead(locator_lsn)`
  - `XLogReadRecord()`
- 现场 `gdb` 已确认：
  - `locator_count = 4530229`
  - backend 长时间停在 `XLogReadRecord -> WALRead`

也就是说，locator-first 已把“读错 WAL”问题压下去，但查询期仍保留了
“对正确 record 逐条重新 seek” 的 I/O 模式。

## 决策

保持 correctness 语义不变：

- payload locator 仍然代表“允许发射 payload 的精确 record start LSN”
- 不允许因为批量顺扫而把 run 内其他 relation record 错误发射进 payload

在此前提下，把 locator 访问改成两档：

- 高密度 locator case：
  - 不再逐条 `XLogBeginRead`
  - 先按 covered segment run 构建 batched visit windows
  - 对这些 windows 做顺序 WAL 访问
  - 发射期额外维护精确 locator 迭代器，只在当前 `reader->ReadRecPtr`
    与 locator `record_start_lsn` 精确相等时才允许 payload 发射
- 稀疏 locator case：
  - 继续保留 direct locator read
  - 避免为了减少 seek 而把 decode 范围扩大到明显不划算

## 为什么不直接把 batched window 当作新的 emit window

- batched window 只是 I/O 读范围，不是 correctness 范围
- 若直接把 `[first_locator, last_locator]` 当成 emit window：
  - run 内其他同 relation record 也会被 `fb_index_record_visitor()` 接受
  - 结果集会偏离 summary locator 的精确语义
- 因此必须额外维护“当前应命中的 locator 指针”，把 batched run 仅作为
  顺序解码手段，而不是记录选择条件

## 后果

优点：

- 把 locator 模式的 I/O 从“百万级随机-ish seek/read”收敛成“少量 run 顺扫”
- 保留 summary payload locator-first 的精确 payload 语义
- 不需要改变 summary 文件格式

代价：

- query-side 需要维护 locator 迭代器状态，检测 batched visit 是否与 locator 集脱同步
- 需要引入 locator 访问密度判定，避免稀疏 case 退化
- 调试与回归需要补充 batching 观测，避免未来退回逐条 direct read

## 语义要求

- batched run 仅决定读 WAL 的方式，不改变 payload 选择集合
- 若 batched visit 跳过了 locator 指向的 record，必须报错，不允许静默漏发射
- sidecar 缺失、旧版本、recent tail 或低密度 case 下，允许继续 direct locator read
- 任何 batching 优化都不得改变 replay / reverse-op / apply 语义
