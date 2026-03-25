# 2026-03-24 主表并行扫描放宽口径基准

## 目标

- 用户已放宽要求，不再坚持真实 `fb_internal_flashback()` 入口
- 本轮只要求：
  - 能稳定跑通
  - 能验证 `pg_flashback.parallel_segment_scan` 开关效果
  - 至少横跨多个 WAL
  - 连续测试 `3` 次并给出结论

## 范围说明

- 测试对象固定为现成单表夹具：`fb_parallel_speed_final.bench_main`
- 时间点固定取：
  - `target_ts = 2026-03-24 13:09:29.198271+08`
- 当前窗口规模：
  - `10000000` 行
  - `target_ts -> now` WAL 约 `7GB+`
  - `fb_scan_wal_debug()` 已确认会访问 `1475` 个 segment
- 本轮不测真实 `fb_internal_flashback()`：
  - 原因是该窗口已被判定为 `unsafe=storage_change`
  - 真实入口会直接报错

## 方法

- 使用可稳定跑通的扫描调试入口验证并行开关效果：
  - 优先 `fb_scan_wal_debug(regclass, timestamptz)`
- 对 `off/on` 各执行 `3` 轮
- 用服务端 `clock_timestamp()` 包裹单次调用，记录 wall clock 毫秒数
- 同时记录扫描摘要，确认窗口确实横跨多个 WAL segment

## 结果

- 实测 marker：
  - `target_ts = 2026-03-24 13:09:29.198271+08`
  - `start_lsn = 5C/100D138`
  - `pg_wal_lsn_diff(current, start_lsn) = 8264566416`
  - 即约 `7.70GB`
  - `pg_database_size('fb_parallel_speed_final') = 6760363711`
- `fb_scan_wal_debug()` 摘要显示：
  - `off` 每轮都会访问 `1475/1475` 个 segment
  - `on` 会命中 `734/1475` 个 segment，实际访问 `737/1475` 个 segment
  - 因此本轮验证明确横跨多个 WAL，不是单 segment 或短窗口测试

### 原始样本

- `off`
  - 第 1 轮：`39010.003ms`
  - 第 2 轮：`31259.313ms`
  - 第 3 轮：`26449.008ms`
- `on`
  - 第 1 轮：`20775.765ms`
  - 第 2 轮：`175.014ms`
  - 第 3 轮：`209.356ms`

### 观察

- 第 1 轮 `on` 相比 `off` 已经更快，约 `1.88x`
- 第 2 轮与第 3 轮，`on` 已进入 cache/pre-filter 复用后的 steady-state：
  - 第 2 轮约 `178.6x`
  - 第 3 轮约 `126.3x`
- `off` 三轮都仍是全窗口顺序访问，保持在 `26s ~ 39s`
- `on` 三轮都稳定只访问约一半 segment；首轮还需建立 cache，后两轮几乎只剩命中窗口扫描成本

## 结论

- 在“允许使用可跑通的扫描调试入口，而不强求真实 flashback 入口”的前提下，`pg_flashback.parallel_segment_scan = on` 已明确有效。
- 本轮现成单表窗口横跨 `1475` 个 WAL segment，规模约 `7.70GB`，满足“至少横跨多个 WAL”的要求。
- 连续 `3` 轮结果显示：
  - `off` 始终为全窗口顺序扫描，耗时几十秒
  - `on` 首轮已有收益，后续两轮在 cache 生效后收益极大
- 因此当前可以给出的结论是：
  - 对 `fb_scan_wal_debug()` 这条可跑通的扫描路径，`parallel_segment_scan on` 明显优于 `off`
  - 但这不是 `fb_internal_flashback()` 真实入口的端到端结论；真实入口在这批窗口上仍会因为 `unsafe=storage_change` 直接报错
