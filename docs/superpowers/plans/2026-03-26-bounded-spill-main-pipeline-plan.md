# 2026-03-26 bounded spill 主链计划

## 目标

把 flashback 主链从“reverse op 纯内存暂存”推进到“受内存上限约束、必要时顺序 spill”的统一模型。

## 步骤

1. 增加 `fb_spool` query 级 runtime 文件管理。
2. 为 reverse op 定义统一 source / reader 抽象。
3. 先把 `fb_reverse_ops` 与 `fb_apply` 改到 `FbReverseOpSource`。
4. 以回归确保：
   - 用户 SQL 入口不变
   - keyed / bag 语义不变
   - memory charge 仍能拦截异常膨胀
5. 后续再把 `fb_wal` / `fb_replay` 接到同一条 bounded spill 主链。

## 风险

- runtime 文件清理
- TOAST payload 在 spill 边界上的所有权
- progress 与 reader 生命周期对齐
