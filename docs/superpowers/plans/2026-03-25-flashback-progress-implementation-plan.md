# Flashback Progress Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `pg_flashback()` 增加默认开启、可关闭的 `NOTICE` 进度输出，并按固定 9 段向 `psql` 客户端展示当前执行阶段，其中 `3/4/5/6/7/8/9` 输出百分比，百分比统一按 `20%` 桶节流。

**Architecture:** 新增独立 `fb_progress` 模块维护 backend-local 进度上下文和统一 `NOTICE` 输出；`pg_flashback()` 顶层负责创建/清理当前进度，`fb_wal` / `fb_replay` / `fb_reverse_ops` / `fb_apply_*` 在各自天然循环中上报阶段和百分比。现有回归默认关闭 progress，单独新增 `fb_progress` 回归验证默认开启与显式关闭行为。

**Tech Stack:** PostgreSQL extension (PGXS), C, SQL install script, pg_regress, Markdown documentation

---

### Task 1: 先更新项目记录并登记新模块

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Create: `docs/superpowers/specs/2026-03-25-flashback-progress-visibility-design.md`

- [ ] **Step 1: 记录“psql 进度 NOTICE”需求与阶段模型**

把 9 段模型、百分比阶段、默认开启可关闭的要求写入状态、待办和设计稿。

- [ ] **Step 2: 登记 `fb_progress` 模块职责**

在架构总览中增加新模块，明确它只负责客户端进度可视化，不参与 flashback 正确性计算。

- [ ] **Step 3: 人工核对文档一致性**

Run: `rg -n "show_progress|fb_progress|9 段|NOTICE" STATUS.md TODO.md docs/architecture/overview.md docs/superpowers/specs/2026-03-25-flashback-progress-visibility-design.md`
Expected: 能看到统一的“9 段 + 默认开启 + 可关闭 + NOTICE”口径

### Task 2: 测试先行，新增 progress 回归并让它先失败

**Files:**
- Modify: `Makefile`
- Create: `sql/fb_progress.sql`
- Create: `expected/fb_progress.out`
- Modify: 现有调用 `pg_flashback()` 的回归 SQL

- [ ] **Step 1: 新增专门验证 progress 的回归 SQL**

覆盖两种行为：

- 默认 `show_progress = on` 时会输出 progress `NOTICE`
- `SET pg_flashback.show_progress = off` 后不再输出 progress `NOTICE`

- [ ] **Step 2: 在既有非 progress 回归里显式关闭 show_progress**

避免默认开启后污染所有已有 expected。

- [ ] **Step 3: 将新回归加进 `Makefile`**

把 `fb_progress` 加入 `REGRESS`。

- [ ] **Step 4: 运行目标回归验证当前失败**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck REGRESS="fb_progress"'`
Expected: 失败，原因是当前还没有 `pg_flashback.show_progress` GUC 和 progress `NOTICE`

### Task 3: 实现 progress 基础设施与 GUC

**Files:**
- Create: `include/fb_progress.h`
- Create: `src/fb_progress.c`
- Modify: `include/fb_guc.h`
- Modify: `src/fb_guc.c`
- Modify: `Makefile`

- [ ] **Step 1: 新增 `pg_flashback.show_progress` GUC**

默认 `on`，提供只读 accessor 给其他模块使用。

- [ ] **Step 2: 实现 backend-local progress context**

在 `fb_progress` 模块中提供：

- begin/reset
- enter stage
- update percent
- stage complete
- error-path cleanup

- [ ] **Step 3: 将新模块接入构建**

更新 `Makefile` 让 `fb_progress.c` 进入扩展编译。

- [ ] **Step 4: 运行 `fb_progress` 回归，确认仍处于半绿状态**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && make && make install && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck REGRESS="fb_progress"'`
Expected: GUC 已存在，但阶段与百分比 `NOTICE` 仍未完全符合 expected

### Task 4: 接入 9 段阶段与百分比上报

**Files:**
- Modify: `src/fb_entry.c`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_replay.c`
- Modify: `src/fb_reverse_ops.c`
- Modify: `include/fb_apply.h`
- Modify: `src/fb_apply_keyed.c`
- Modify: `src/fb_apply_bag.c`

- [ ] **Step 1: 在 `pg_flashback()` 顶层接入阶段 `1/2/9` 与异常清理**

使用 `PG_TRY/PG_CATCH` 保证 progress context 不残留。

- [ ] **Step 2: 在 `fb_wal` 接入阶段 `3` 百分比**

按本轮访问 segment 总数推进索引阶段百分比。

- [ ] **Step 3: 在 `fb_replay` 接入阶段 `4/5/6` 百分比**

discover/warm/final 都按 `record_index` 推进，并让 discover 输出 `round=N`。

- [ ] **Step 4: 在 `fb_reverse_ops` 接入阶段 `7` 百分比**

按已处理 `ForwardOp` 数推进，阶段结束后补 `100%`。

- [ ] **Step 5: 在 apply 路径接入阶段 `8` 百分比**

将“扫描当前表”映射到 `0%~40%`，“应用 reverse ops”映射到 `40%~100%`。

- [ ] **Step 6: 在 `pg_flashback()` 物化循环接入阶段 `9` 百分比**

按“已写入结果表的行数 / 最终结果总行数”推进物化阶段进度，并在空结果时直接补 `100%`。

- [ ] **Step 7: 运行 `fb_progress` 回归转绿**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && make && make install && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck REGRESS="fb_progress"'`
Expected: 通过

### Task 5: 回归收口与文档同步

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture/核心入口源码导读.md`
- Modify: `docs/architecture/源码级维护手册.md`
- Modify: `docs/architecture/调试与验证手册.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: 受影响的 existing SQL / expected

- [ ] **Step 1: 补用户与维护文档**

说明：

- `pg_flashback.show_progress`
- 默认开启
- 如何关闭
- 9 段模型与百分比口径

- [ ] **Step 2: 跑目标回归集**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && make && make install && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck REGRESS="fb_smoke fb_runtime_gate fb_flashback_keyed fb_flashback_bag pg_flashback fb_user_surface fb_memory_limit fb_toast_flashback fb_progress"'`
Expected: 通过

- [ ] **Step 3: 跑全量回归**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck'`
Expected: 全部回归通过

- [ ] **Step 4: 更新收尾记录**

在 `STATUS.md` 和 `TODO.md` 中勾掉已完成项，并补“已完成 / 进行中 / 下一步”。
