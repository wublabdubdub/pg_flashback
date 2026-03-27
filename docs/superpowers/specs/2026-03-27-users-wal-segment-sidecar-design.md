# 2026-03-27 users WAL segment sidecar 设计

## 目标

为 `fb_wal` 增加 segment 级 sidecar 元数据，减少重复扫描与无效段进入主链。

## 方向

- sidecar 记录段级命中窗口 / 事务摘要
- 优先服务真实 flashback 主路径，不做脱离主链的旁路系统
- 与 bounded spill / runtime 目录统一管理
