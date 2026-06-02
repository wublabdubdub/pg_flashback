# 2026-03-24 并行 segment 预扫描基准报告

## 更新结论

首版基准结论已经过时。本报告当前状态分两阶段：

- 第一阶段：首版 `parallel_segment_scan` 只做并行粗筛，正式扫描仍会把全部 segment 细扫一遍，结果是三组都明显慢于 `off`
- 第二阶段：修正为“命中窗口跳段 + prefilter file cache + backend cache”后，steady-state 三组都已经满足 `on < off`

当前推荐以第二阶段结果为准：

- `large`
  - `off` 中位数：`111.009ms`
  - `on` 中位数：`50.954ms`
- `medium`
  - `off` 中位数：`68.673ms`
  - `on` 中位数：`28.102ms`
- `small`
  - `off` 中位数：`47.787ms`
  - `on` 中位数：`38.057ms`

补充说明：

- `large` 的旧 snapshot 会在 `unsafe=storage_change` 上早停，导致 `off` 异常偏快，已被新的安全 snapshot 替换
- 当前 `on` 的性能模型已经不是“每次都重新预筛选全部 segment”，而是：
  - 首次建立 prefilter cache
  - 后续同段扫描优先走 `meta` file cache 和 backend 内存 cache
  - 正式扫描只访问命中窗口，不再对 miss segment 做二次细扫

## 目的

验证 `pg_flashback.parallel_segment_scan` 在当前首版实现下，是否能为 `fb_scan_wal_debug()` 带来真实加速。

本轮只测扫描阶段，不混入 replay / apply。基准函数固定为：

- `fb_scan_wal_debug(regclass, timestamptz)`

## 测试环境

- PostgreSQL 18 本机实例，端口 `5832`
- 目标库：`fb_parallel_bench`
- WAL 来源：每组 workload 完成后立即从 `pg_wal` 复制出静态快照目录
- 并行开关：
  - `off`：`pg_flashback.parallel_segment_scan = off`
  - `on`：`pg_flashback.parallel_segment_scan = on`

## 夹具设计

使用同一张目标表 `fb_parallel_bench_target`，配合噪声表 `fb_parallel_bench_noise` 生成三组时间窗：

- `large`
  - 目标行变更后追加约 `135.4MB` WAL
  - 对应快照目录共 `42` 个 segment
- `medium`
  - 目标行变更后追加约 `69.5MB` WAL
  - 对应快照目录共 `36` 个 segment
- `small`
  - 目标行变更后追加约 `36.6MB` WAL
  - 对应快照目录共 `34` 个 segment

每组 workload 完成后立即复制当时的 `pg_wal` 到独立目录，避免后续 recycle 干扰基准。

## 方法

每组分别执行 3 轮 `off/on` 交替测试，服务端内部用 `clock_timestamp()` 包住 `fb_scan_wal_debug()` 计算 wall clock 毫秒数。

原始结果见 `/tmp/fb_parallel_bench_results.tsv`。

## 结果

### large

- `off`
  - 样本：`112.197ms`、`104.909ms`、`110.214ms`
  - 平均：`109.107ms`
  - 中位数：`110.214ms`
- `on`
  - 样本：`7321.459ms`、`7890.487ms`、`7089.767ms`
  - 平均：`7433.904ms`
  - 中位数：`7321.459ms`
- 预筛选命中：`1/42`
- 结论：`on` 相比 `off` 变慢约 `66.4x`

### medium

- `off`
  - 样本：`72.201ms`、`57.001ms`、`58.268ms`
  - 平均：`62.490ms`
  - 中位数：`58.268ms`
- `on`
  - 样本：`5958.264ms`、`6207.704ms`、`6129.134ms`
  - 平均：`6098.367ms`
  - 中位数：`6129.134ms`
- 预筛选命中：`1/36`
- 结论：`on` 相比 `off` 变慢约 `105.2x`

### small

- `off`
  - 样本：`45.092ms`、`44.315ms`、`48.909ms`
  - 平均：`46.105ms`
  - 中位数：`45.092ms`
- `on`
  - 样本：`6151.011ms`、`6170.503ms`、`5894.434ms`
  - 平均：`6071.983ms`
  - 中位数：`6151.011ms`
- 预筛选命中：`1/34`
- 结论：`on` 相比 `off` 变慢约 `136.4x`

## 结论

当前首版 `parallel_segment_scan` 没有带来加速，反而在三组基准里都显著退化。

已确认它确实把最终正式扫描的命中 segment 压到了 `1/N`，但现阶段预筛选本身的成本远高于节省掉的正式扫描成本。

## 原因分析

当前实现的主要问题是：

1. 预筛选线程会对每个 segment 做整文件字节扫描。
2. 预筛选即使命中率很低，仍然要先把全部 segment 读一遍。
3. 现有正式扫描路径在这三组规模下本来就很快，几十到一百多毫秒即可完成。
4. 因此“先并行粗扫全部 segment，再单线程正式扫命中 segment”的两阶段成本，显著高于直接正式扫描。

## 建议

当前不应把 `parallel_segment_scan=on` 视为性能优化项推广使用。后续若继续推进，需要至少重做其中一项：

- 改为更轻量的 segment 级元数据筛选，而不是整文件字节扫描
- 为预筛选增加阈值，segment 数不足时强制退回单线程
- 只在 archive 非常大、且已证明正式扫描成为瓶颈时才启用
- 把 relation 命中判断从“原始字节搜索”换成更可控的 WAL page / record 级轻解析
