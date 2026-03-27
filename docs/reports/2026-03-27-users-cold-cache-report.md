# 2026-03-27 users 冷缓存复测报告

## 范围

针对已知 `users` 用例，在每次查询后重启数据库并清理页缓存，尽量去除热缓存干扰，重新观察 flashback 热点。

## 结论摘要

- 冷缓存下，`fb_wal` / replay 前置扫描仍有成本，但 stage 8/9 的 TOAST flatten 热点依旧显著
- 因此本轮优化方向继续保留：
  - bounded spill / sidecar 压缩前置高水位
  - materialized SRF 降低 TOAST-heavy 行发射成本

## 备注

- 本报告只记录方向性结论，具体 perf 细节仍以同轮会话日志和后续复测为准
