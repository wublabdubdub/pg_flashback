# Summary Backlog Throughput Design

## 目标

本轮只优化 `summary` 预建服务的历史 backlog 补齐速度，不以 recent 前沿延迟为主目标。

同时补一个用户可见观测：

- `pg_flashback_summary_progress.estimated_completion_at`

其语义固定为“按当前可见 backlog 与最近实际 build 速率估算的 summary 完成时间”，无法稳定估算时返回 `NULL`。

## 现状问题

当前 `summary` 预建吞吐主要慢在三类重复开销：

1. 调度侧：
   - hot / cold 共用同一套优先级抢占
   - cold backlog 容易被 recent 热前沿长期挤压
   - worker 逐段 claim，反复争抢 shared queue
2. 候选扫描侧：
   - launcher 会反复扫描所有 candidate
   - 对每个 candidate 通过 `open + read header` 判断 summary 是否已存在
3. 单段构建侧：
   - `touched_xids` / `unsafe_facts` / `block_anchors` 先 append，再在段尾 `qsort/dedup`
   - relation 子数组多次 `repalloc`

## 决策

### 1. backlog 调度优先级

- 固定保留 `1` 个 worker 优先跟 hot recent
- 其余 worker 默认优先吃 cold backlog
- hot worker 在 hot 队列为空时允许回退吃 cold
- cold worker 在 cold 队列为空时允许回退吃 hot

这样可以保证 recent 不饿死，同时把大部分并发预算让给历史 backlog。

### 2. cold 任务批量 claim

- worker 不再每次只领取 `1` 个 cold segment
- 改为一次领取一小批连续 cold segment
- 同一批任务在 worker 本地串行构建后再回到 shared queue

目标是减少：

- shared queue 锁竞争
- 任务选优扫描
- backlog 深盘面下的 launcher / worker 往返

### 3. “summary 已存在”判定快路径

- service 调度侧只需要判断“当前 sidecar 是否已经存在且文件身份匹配”
- 不再为每次 enqueue 都执行完整 section 级 header load
- 查询侧读取 summary 时仍保留严格校验

### 4. 构建器改为边扫边去重

单段构建固定改成：

- `touched_xids` 使用集合去重
- `unsafe_facts` 使用 hash 去重
- `block_anchors` 使用 `(relation, block)` -> latest anchor 的覆盖式聚合
- relation 子结构使用更粗粒度扩容

目标是把段尾整理成本前移并摊平，减少 backlog 构建的 CPU 峰值。

### 5. ETA 口径

- 输入：
  - `missing_summaries`
  - 最近滚动窗口内的 `build_count` 增量
  - 对应时间跨度
- 输出：
  - `estimated_completion_at = now + missing_summaries / recent_build_rate`
- 以下场景返回 `NULL`：
  - service 未启用
  - `missing_summaries = 0`
  - 最近无 build
  - 采样时间过短或速率不可用

## 非目标

- 不改查询期 summary-first 命中逻辑
- 不承诺优化 recent 热前沿首段延迟
- 不把 ETA 扩展成严格 SLA 或百分位预测
- 不删除 `/home/18pg/data/pg_flashback` 整个目录；只清 `meta/summary`

## 验证要求

- 修改后必须手动清空 `/home/18pg/data/pg_flashback/meta/summary`
- summary 服务重建期间确认：
  - cold backlog worker 占多数
  - `pg_flashback_summary_progress` 暴露 `estimated_completion_at`
- 回归至少覆盖：
  - progress 视图新字段存在
  - 无有效速率时 ETA 为 `NULL`
  - 产生 build 样本后 ETA 可计算
