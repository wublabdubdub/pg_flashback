# ADR-0025 Summary backlog 吞吐优先与 ETA 观测

## 状态

已接受

## 背景

当前 summary 预建服务已经具备：

- hot / cold 双队列
- launcher + worker pool
- 用户主视图 `pg_flashback_summary_progress`

但 backlog 很深时仍存在两个问题：

1. 历史 cold backlog 补齐速度偏慢
2. 用户只能看到完成度，无法看到大致完成时间

同时当前 backlog 构建热路径仍包含明显的重复 CPU 开销：

- 逐段 claim shared queue
- 对已存在 summary 做重复的重校验
- builder 先收集、再排序、再去重

## 决策

本轮固定口径如下：

- 吞吐目标优先级：
  - 先提升历史 cold backlog 补齐速度
  - recent hot frontier 只保留最低保障，不作为首要优化目标
- worker 配额：
  - 固定 `1` 个 worker hot-first
  - 其余 worker cold-first
  - 双方都允许在首选队列为空时回退到另一队列
- cold 任务 claim 粒度：
  - 从单段 claim 提升为小批量连续 cold segment claim
- builder 聚合口径：
  - `touched_xids` / `unsafe_facts` / `block_anchors` 改为边扫边去重
- 用户视图：
  - `pg_flashback_summary_progress` 新增 `estimated_completion_at`
  - 以最近 build 速率估算当前 backlog 补齐时间

## 为什么不继续只调 recent-first

- 当前用户痛点明确是历史 backlog 长时间补不齐
- hot frontier 只需要“不饿死”，不需要继续挤占大部分 worker
- cold backlog 优先更符合“历史段越早补齐，summary 覆盖越稳”的目标

## 后果

优点：

- 历史 backlog 会更快下降
- worker 锁争用和单段构建 CPU 会下降
- 用户可以看到预计完成时间，而不是只看百分比

代价：

- summary service 调度逻辑更复杂
- builder 内存占用会适度增加
- ETA 只能是 best-effort 估算，不能视为严格承诺

## 运维要求

- 变更后允许直接清空 `/home/18pg/data/pg_flashback/meta/summary`
- 不删除 `/home/18pg/data/pg_flashback` 整个目录
- 旧 summary 视为可再生缓存，重建一次即可
