# Heap Insert Root Cause Debug Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 查清 `scenario_oa_12t_50000r.documents @ '2026-04-01 08:10:13'` 历史报错 `failed to replay heap insert` 的真正根因，并在可复现前提下完成修复与回归。

**Architecture:** 调试从 live SQL 入口出发，逆向追踪 `record_log -> replay discover/warm/final -> fb_replay_heap_insert()`。先确定旧错误依赖的 build/runtime 条件，再用最小 instrumentation 和 `gdb` 锁定第一页状态偏离点，最后补最小 RED 用例并做单点修复。

**Tech Stack:** PostgreSQL 18, PGXS, C, gdb, pg_waldump, psql, installcheck

---

### Task 1: 重建复现条件

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Inspect: `src/fb_replay.c`
- Inspect: `src/fb_wal.c`

- [ ] **Step 1: 记录当前假设与目标现场**

记录到 `STATUS.md` / `TODO.md`：
- 旧错误点：`toast rel 1663/33398/16395804 blk 213310`
- 旧失败 LSN：`81/71035B40 off=27 maxoff=20`
- 目标：判定错误源于哪一层，而不是只验证“新 build 不复现”

- [ ] **Step 2: 确认工作区 build/runtime 状态**

Run: `git status --short`
Expected: 明确哪些文件已被修改，避免把旧错误与当前脏树混淆

- [ ] **Step 3: 复跑 live SQL，区分不同 build/runtime 条件**

Run:
```bash
su - 18pg -c "psql alldb -v ON_ERROR_STOP=1 -Atqc \"SET pg_flashback.show_progress = off; SET pg_flashback.memory_limit='8GB'; select count(*) from pg_flashback(NULL::scenario_oa_12t_50000r.documents,'2026-04-01 08:10:13');\""
```
Expected: 记录成功/失败、返回值或报错文本

- [ ] **Step 4: 如果 live 仍不复现，切换到旧 build/runtime 线索**

检查旧日志、已安装 `.so`、postmaster 重启前后差异，收敛“必须满足的复现条件”

### Task 2: 定位第一次页状态偏离

**Files:**
- Modify: `src/fb_replay.c`
- Inspect: `src/fb_wal.c`
- Inspect: `include/fb_replay.h`

- [ ] **Step 1: 为目标块增加最小 trace**

只对 `toast rel 16395804 blk 213310` 打点：
- 进入 `fb_replay_heap_insert/delete/update`
- `maxoff` 变化
- `discover/warm/final` phase
- 记录 `record_index` / `lsn` / `offnum`

- [ ] **Step 2: 重编译并安装**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config`
Expected: build 成功

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
Expected: 安装成功

- [ ] **Step 3: 重启 PG18 并复跑 live SQL**

Run: `su - 18pg -c "pg_ctl -D /isoTest/18pgdata restart -m fast -w"`
Expected: PostgreSQL 正常启动

- [ ] **Step 4: 从日志中提取第一页状态异常点**

Run: `tail -n 200 /isoTest/18pgdata/pg_log/<latest-log>`
Expected: 找到第一次 `maxoff` 与预期偏离的位置，判断偏离发生在 discover、warm 还是 final

- [ ] **Step 5: 必要时挂 gdb**

在 `fb_replay_heap_insert`、`fb_replay_ensure_block_ready`、`fb_replay_run_pass` 附近下断点，观察 `state->initialized`、`page` 内容和 `record_index`

### Task 3: 建立最小 RED

**Files:**
- Modify: `sql/fb_flashback_toast_storage_boundary.sql` 或新增更小专项 SQL
- Modify: `expected/*.out`
- Test: `Makefile`

- [ ] **Step 1: 把现场模式缩成最小用例**

覆盖：
- 同一 TOAST 页连续 `insert 21..26`
- `delete 21..22`
- 后续 `insert 27..28`

- [ ] **Step 2: 运行回归并确认修复前失败**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg REGRESS=<test-name> installcheck`
Expected: 修复前稳定失败，且错误与现场一致或等价

### Task 4: 单点修复与验证

**Files:**
- Modify: `src/fb_replay.c` 或 `src/fb_wal.c`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: 实现单点修复**

只修第一次状态偏离的真正来源，不混入额外重构

- [ ] **Step 2: 先跑最小回归**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg REGRESS=<test-name> installcheck`
Expected: PASS

- [ ] **Step 3: 再跑相关回归 bundle**

Run:
```bash
make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg REGRESS="fb_recordref fb_flashback_toast_storage_boundary fb_toast_flashback fb_summary_overlap_toast fb_wal_sidecar" installcheck
```
Expected: PASS

- [ ] **Step 4: 复跑 live SQL**

Run:
```bash
su - 18pg -c "psql alldb -v ON_ERROR_STOP=1 -Atqc \"SET pg_flashback.show_progress = off; SET pg_flashback.memory_limit='8GB'; select count(*) from pg_flashback(NULL::scenario_oa_12t_50000r.documents,'2026-04-01 08:10:13');\""
```
Expected: 不再报 `failed to replay heap insert`

- [ ] **Step 5: 收尾文档**

更新 `STATUS.md` / `TODO.md`：
- 根因
- 修复点
- 回归
- live 复核结果
