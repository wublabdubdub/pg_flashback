# 2026-03-27 恢复记录 Stage B

## 目标

在 `Stage A` 的“可编译 bounded spill 骨架”基础上，继续恢复 `2026-03-26 21:10` 后半段到 `2026-03-27` 的 spill follow-up、materialized SRF、`fb_wal` sidecar 与相关文档。

## 真实恢复路径

- 恢复基线提交：`1bbfddb9fb546092e532a146b62f0377b50a311d`
- 真实工作目录：`/walstorage/pg_flashback`
- 继续恢复所用会话：
  - `019d2a44-6248-7b31-bf3a-c4288f14e9eb`
  - `019d2d96-b12e-7a81-8136-d73137722336`
  - `019d2e72-7242-75c3-a9cc-a31846e9de3e`
  - `019d2eb9-ce62-77b0-b857-7780ca2ef26d`

## 过程要点

- 已改为只回放日志里 `apply_patch` 成功落地的 patch，跳过失败重试记录。
- `05:32` 那组多文件 patch 在本地重放时出现一个上下文分叉：
  - `src/fb_apply_keyed.c` 的旧上下文在日志里仍用 `fb_keyed_find_string_entry(state, identity)`
  - 但当前顺序回放后的前像已经切到 `state->buckets`
- 对该点做了最小上下文适配后，`05:32` 组 patch 在探针树闭合，并继续顺序重放到后续会话尾部。

## 当前稳定恢复点

- 当前继续恢复后的稳定临时树：
  - `/tmp/pgfb_exact_probe`
- 当前工作区已同步到该恢复点。

## 当前已同步回仓库的重点内容

- spill / source 主链：
  - `fb_spool`
  - `sql/fb_spill.sql`
  - `FbReverseOpSource`
- apply / entry：
  - `fb_apply` emit / materialize 双路径
  - `fb_apply_keyed`
  - `fb_apply_bag`
  - `pg_flashback()` 内部 materialized SRF 分支
- wal / replay：
  - `fb_wal` sidecar / tail inline 恢复代码
  - `sql/fb_recordref.sql`
  - `sql/fb_toast_flashback.sql`
  - `sql/fb_wal_sidecar.sql`
- 文档：
  - bounded spill 设计 / ADR
  - TOAST-heavy materialized SRF ADR
  - 相关 plans / specs
  - 冷缓存报告

## 当前验证

- `/tmp/pgfb_exact_probe`：
  - `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`
  - 结果：通过
- `/root/pg_flashback`：
  - `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`
  - 结果：通过

## 仍未补齐

- 还没按恢复后的代码重新补跑：
  - `make install`
  - `installcheck`
  - `tests/deep/`
  - 冷缓存复测
- 部分历史文档更新是按“新增缺失文档 + 当前 STATUS/TODO/overview 对齐”方式恢复，不是对旧文档逐 patch 原样重放。

## 下次接手建议

- 以 `/tmp/pgfb_exact_probe` 和本文件为继续恢复 / 验证起点。
- 若再次压缩失败，不要回到 `Stage A` 重新摸排；优先沿当前 `Stage B` 编译通过点继续补验证。
