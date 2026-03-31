# Summary Progress Surface Redesign

## 背景

当前 `fb_summary_progress` 视图混合了两类信息：

- 用户真正关心的“summary 连续覆盖已经到哪个时间点”
- launcher / worker / queue / cleanup 这类服务内部调度细节

实际使用中，用户会把 `covered_through_ts` 误读成“summary 已经建到哪一天”，而忽略它其实只是“从最新 stable segment 往回的连续覆盖前沿”。`cold_covered_until_ts` 又只有在最老端形成连续覆盖前缀时才非空，因此用户面对 `NULL` 时仍然不知道“现在到底卡在哪个洞”。

这说明当前视图不适合作为用户面主接口。

## 目标

重做当前 summary 进度视图，让用户直接回答这 4 个问题：

1. 当前 stable WAL 时间窗本身覆盖的是哪段时间
2. 从最新端往回，summary 已连续覆盖到哪个时间点
3. 从最老端往前，summary 已连续覆盖到哪个时间点
4. 如果还没连续覆盖，中间第一个洞卡在哪个 segno / 时间点

同时将对外用户可见 summary 观测面统一收口到 `pg_flashback_*` 前缀下，不再继续暴露 `fb_*` 命名给最终用户。

## 决策

### 1. `fb_summary_progress` 破坏兼容重做

旧视图不再作为用户主接口保留。当前实现直接改造成新的用户语义视图，并迁移到：

- `pg_flashback_summary_progress`

旧列名、旧列顺序、旧“queue/counter 混排”语义都不保留兼容。

### 2. 拆分用户视图与调试视图

对外拆成两层：

- 用户视图：`pg_flashback_summary_progress`
- 调试视图：`pg_flashback_summary_service_debug`

其中：

- `pg_flashback_summary_progress` 只放用户能直接理解的时间前沿 / 卡点 / 完成度
- `pg_flashback_summary_service_debug` 承载 launcher pid、worker、queue、cleanup 等服务内部细节

### 3. 用户视图字段

`pg_flashback_summary_progress` 固定输出：

- `service_enabled`
- `timeline_id`
- `stable_oldest_segno`
- `stable_newest_segno`
- `stable_oldest_ts`
- `stable_newest_ts`
- `near_contiguous_through_ts`
- `far_contiguous_until_ts`
- `first_gap_from_newest_segno`
- `first_gap_from_newest_ts`
- `first_gap_from_oldest_segno`
- `first_gap_from_oldest_ts`
- `completed_segments`
- `missing_segments`
- `progress_pct`

语义：

- `near_contiguous_through_ts`
  从最新 stable segment 往回，连续已有 summary 的最老事务时间点
- `far_contiguous_until_ts`
  从最老 stable segment 往前，连续已有 summary 的最晚事务时间点
- `first_gap_from_newest_*`
  从最新端往回遇到的第一个无 summary 洞
- `first_gap_from_oldest_*`
  从最老端往前遇到的第一个无 summary 洞

这样即使 `far_contiguous_until_ts` 仍为 `NULL`，用户也能直接看到“最老端第一个洞在哪里”，不再需要猜。

### 4. 调试视图字段

当前 `fb_summary_progress` 里偏内部的字段迁移到：

- `pg_flashback_summary_service_debug`

包括但不限于：

- `launcher_pid`
- `registered_workers`
- `active_workers`
- `queue_capacity`
- `hot_window`
- `pending_hot`
- `pending_cold`
- `running_hot`
- `running_cold`
- `summary_files`
- `summary_bytes`
- `scan_count`
- `enqueue_count`
- `build_count`
- `cleanup_count`
- `last_scan_at`

### 5. 命名规则

自本次改动起，新的用户可见 SQL 观测面统一使用 `pg_flashback_` 前缀，不再新增 `fb_*` 用户对象。例如：

- `pg_flashback_summary_progress`
- `pg_flashback_summary_service_debug`

`fb_*` 仅保留给扩展内部 C 符号、内部 SQL helper、回归调试对象。

## 实现要点

- 继续复用当前 snapshot 口径，不回摆到“直接拿文件系统最新 tail 做分母”
- 在现有遍历 stable candidates 的一次扫描中，同时算出：
  - stable 时间窗上下界
  - newest->oldest 连续覆盖前沿
  - oldest->newest 连续覆盖前沿
  - 两端第一个 gap 的 segno 与时间边界
- 时间点优先取已有 summary header 中的 `oldest_xact_ts/newest_xact_ts`
- 对于无 summary 的 gap，若拿不到事务时间，则至少给出 segno；若能从相邻已存在 summary 推导边界，则补时间点
- 现有 `fb_summary_service_plan_debug()` 继续只作为测试/开发 helper，不进入正式安装面

## 验收

用户应能通过一条查询直接回答：

- “近端已经到哪了”
- “远端已经到哪了”
- “如果远端还没形成连续前缀，第一个洞在哪”

即：

```sql
SELECT *
FROM pg_flashback_summary_progress;
```
