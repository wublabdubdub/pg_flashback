# ADR-0022: Summary Progress Surface Redesign

## 状态

已接受

## 背景

当前 `fb_summary_progress` 将用户关心的覆盖前沿与服务内部调度细节混在一个视图里，导致用户无法直接判断：

- 近端已经连续覆盖到哪个时间点
- 远端已经连续覆盖到哪个时间点
- 若还未连续覆盖，当前第一个洞卡在哪

同时，用户可见对象继续暴露 `fb_*` 前缀，也不符合当前用户接口已收口到 `pg_flashback_` 前缀的长期方向。

## 决策

1. 以破坏兼容方式替换旧 summary 进度主视图
2. 用户主视图改为 `pg_flashback_summary_progress`
3. 服务内部调度细节迁移到 `pg_flashback_summary_service_debug`
4. 新的用户可见 summary 观测面统一使用 `pg_flashback_` 前缀，不再新增 `fb_*` 用户对象

## 结果

- 用户查询 summary 进度时，直接使用：

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

- 用户视图只表达：
  - stable 时间窗
  - 近端连续覆盖前沿
  - 远端连续覆盖前沿
  - 两端第一个 gap
  - 完成度

- launcher / worker / queue / cleanup 等调试信息继续保留，但不再混入用户主视图
