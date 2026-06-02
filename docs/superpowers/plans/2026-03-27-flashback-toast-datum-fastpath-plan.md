# 2026-03-27 flashback TOAST datum fastpath 计划

1. 用 `perf` 锁定 stage 8/9 的 TOAST 热点。
2. 区分：
   - unchanged current row
   - replacement tuple / residual tuple
3. 验证 value-per-call SRF 是否是主要 flatten 来源。
4. 若确认热点在 SRF 复合 Datum 发射，转为 materialized SRF 方案。
