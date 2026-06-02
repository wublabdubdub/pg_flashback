# Build-Index Progress And Payload Design

## 背景

当前 live case：

```sql
select count(*)
from pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 01:40:13');
```

现场表现为：

- `3/9 100%` 后到 `4/9 0%` 之间仍有约 `23s`
- 用户会自然把这段时间误判为 `4/9 replay discover` 的耗时

但当前代码级复核已经确认：

- `fb_replay_debug(..., '2026-04-01 01:40:13+08')` 对应口径里：
  - `precomputed_missing_blocks=0`
  - `discover_rounds=0`
  - `discover_skipped=1`
- `4/9` 本身已经接近空转
- 这段 `23s` 实际更可能属于 `3/9 build record index` 的尾段 payload 工作，只是当前进度百分比过早打满，观测面误导了用户

## 目标

本轮同时完成两件事：

1. 修正 `3/9` 的用户侧观测，让 `NOTICE` 能直接回答“卡在 build-index 的哪一个子相位”
2. 针对 `3/9` 尾段 payload 主链继续收窄工作量，优先降低 live case 中这段约 `23s` 的真实耗时

本轮明确不做：

- 不新增第 `10` 段进度
- 不改用户可见总阶段数，仍固定为 `9` 段
- 不做 `count(*)` 特化或聚合下推
- 不先进行整条 replay 架构的激进重写

## 决策

### 1. `3/9` 保持单阶段，但细分固定子相位

`3/9` 仍保持用户可见标签：

- `scanning wal and building record index`

但 detail 和 percent 改成固定子相位映射，不再仅按“访问到第几个 segment”推进。

当前拍板子相位：

- `prefilter`
- `summary-span`
- `metadata`
- `xact-status`
- `payload`

建议 percent 映射：

- `0-10%`: `prefilter`
- `10-30%`: `summary-span`
- `30-55%`: `metadata`
- `55-70%`: `xact-status`
- `70-100%`: `payload`

用户看到的 `NOTICE` 仍只属于 `3/9`，但 detail 直接显示当前子相位，例如：

- `[3/9 10%] scanning wal and building record index prefilter`
- `[3/9 30%] ... summary-span`
- `[3/9 55%] ... metadata`
- `[3/9 70%] ... xact-status`
- `[3/9 100%] ... payload`

这样 `3/9 100% -> 4/9 0%` 之间的“伪空白”会消失。

### 2. `3/9 payload` 先做收窄，不做彻底重构

当前 `fb_wal_build_record_index()` 在 build-index 阶段仍会做 payload materialize。

本轮不把整条主链重写为“完全按需 payload”，而是先做更小且可验证的收窄：

- 为 payload 子相位补直接可观测的工作量计数
- 复用现有 summary/span window 结果，尽量避免无效 payload decode
- 优先减少“进入 payload 物化、但对最终 replay/final 没有贡献”的 record work

### 3. 用户观测只改 `NOTICE`

本轮用户要求只做查询时现场观测，不把 `3/9` 子相位统计额外落到 SQL 视图。

因此：

- `pg_flashback_summary_progress` 不新增 build-index 子相位列
- 用户通过查询时的 `NOTICE` 看 `3/9` 细分热点

## 实现要点

### 观测

- 在 `fb_wal_build_record_index()` 中显式标记：
  - `prefilter`
  - `summary-span`
  - `metadata`
  - `xact-status`
  - `payload`
- 用 `fb_progress_map_subrange()` 把每个子相位映射到 `3/9` 的固定百分比区间
- 停止单纯依赖 `fb_wal_prepare_segment()` 的 segment 访问计数来驱动 `3/9` 总体 percent

### payload 收窄

- 为 `fb_recordref_debug()` 增加 payload 相关计数，便于回归和 live case 对照：
  - payload windows
  - payload covered segments
  - payload scanned/decoded records
  - payload kept/materialized records
- 先在现有窗口生成链路上减少无效 payload work，而不是直接改 replay/final 接口

## 验收

### 1. 观测正确

用户 live case 中不应再出现“`3/9 100%` 后还长时间空白，直到 `4/9 0%`”这种误导性现象。

### 2. 回归可验证

- `fb_progress` 回归要能稳定断言新的 `3/9` 子相位 detail
- `fb_recordref` 或同类 debug 回归要能断言新的 payload 计数输出

### 3. live case 可复核

同一条：

```sql
select count(*)
from pg_flashback(NULL::scenario_oa_12t_50000r.documents, '2026-04-01 01:40:13');
```

至少应满足：

- `3/9` 的子相位边界清晰
- 可直接看出 `23s` 属于哪一段子相位
- 若 payload 收窄生效，`3/9` 总耗时应较当前基线下降
