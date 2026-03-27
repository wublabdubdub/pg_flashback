# 2026-03-27 恢复记录 Stage A

## 目标

把 `2026-03-26 11:00` 之后那轮“12GB 大表 / bounded spill / 内存优化”相关代码和文档，从本机 `~/.codex` 会话日志中重新恢复出来，避免继续按当前仓库公开接口口径倒推。

## 当前已落盘的恢复点

- 当前工作区已经同步到一个可编译的 Stage A 恢复点
- 当前稳定临时树保留在：
  - `/tmp/pgfb_stageA_clean`
- 当前已验证：
  - `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`
  - 结果：通过

## 恢复来源

本轮 Stage A 主要来自以下会话的前半段稳定 patch：

- `019d29aa-df57-7d41-a4ca-e104bac942d3`
- `019d2a06-d7bc-7093-8b67-91d7973ed061`
- `019d2a44-6248-7b31-bf3a-c4288f14e9eb`

## 已恢复文件

- `Makefile`
- `include/fb_apply.h`
- `include/fb_memory.h`
- `include/fb_replay.h`
- `include/fb_reverse_ops.h`
- `include/fb_spool.h`
- `include/fb_wal.h`
- `sql/fb_spill.sql`
- `src/fb_apply.c`
- `src/fb_apply_bag.c`
- `src/fb_apply_keyed.c`
- `src/fb_entry.c`
- `src/fb_progress.c`
- `src/fb_replay.c`
- `src/fb_reverse_ops.c`
- `src/fb_spool.c`
- `src/fb_wal.c`

## 当前恢复边界

当前 Stage A 是“可编译恢复点”，不是整轮优化的最终点。

继续顺序重放时，当前已确认的下一个稳定阻断点位于：

- 会话：`019d2a44-6248-7b31-bf3a-c4288f14e9eb`
- 时间：`2026-03-26T13:23:09.397Z`
- 文件：`src/fb_apply_keyed.c`

再往后仍有以下会话待继续恢复：

- `019d2a44-6248-7b31-bf3a-c4288f14e9eb` 后半段
- `019d2d96-b12e-7a81-8136-d73137722336`
- `019d2e72-7242-75c3-a9cc-a31846e9de3e`
- `019d2eb9-ce62-77b0-b857-7780ca2ef26d`

## 备注

- `/walstorage/pg_flashback` 已不存在，当前恢复只能依赖本机会话日志
- 本轮恢复用到的可继续重放脚本仍在：
  - `/tmp/replay_code_only.py`
- 若后续会话再次压缩失败，优先从本文件列出的 Stage A 工作区与会话边界继续，不要重新从全量历史摸排
