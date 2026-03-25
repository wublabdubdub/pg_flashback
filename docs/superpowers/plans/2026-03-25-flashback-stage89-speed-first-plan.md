# Flashback Stage 8/9 Speed-First Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 去掉 `tuplestore -> TEMP TABLE` 二次物化，把 `pg_flashback()` 的结果直接写入 `UNLOGGED heap` 结果表，为后续并行 apply/write 建立 sink 边界。

**Architecture:** 保留现有 `RecordRef -> replay -> ForwardOp -> ReverseOp -> apply` 主链，只重画结果输出边界。`fb_apply_*` 改为向统一 sink 输出最终行，`pg_flashback()` 预先创建 `UNLOGGED` 结果表并提供 table sink；stage `9` 从入口层搬运改为 apply 末尾直写。

**Tech Stack:** PostgreSQL extension (PGXS), C, SQL regression, Markdown docs

---

### Task 1: 更新文档与产品口径

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Create: `docs/superpowers/specs/2026-03-25-flashback-stage89-speed-first-design.md`

- [ ] **Step 1: 记录速度优先的 `8/9` 重构目标**
- [ ] **Step 2: 明确结果表改为 `UNLOGGED heap`**
- [ ] **Step 3: 说明首轮只做直写，不做真并行 worker**

### Task 2: 先写回归，让语义变化先红

**Files:**
- Modify: `sql/pg_flashback.sql`
- Modify: `expected/pg_flashback.out`

- [ ] **Step 1: 增加结果表持久化类型检查**
- [ ] **Step 2: 运行 `pg_flashback` 回归，确认当前失败**

### Task 3: 引入 result sink 边界

**Files:**
- Modify: `include/fb_apply.h`
- Modify: `src/fb_apply_keyed.c`
- Modify: `src/fb_apply_bag.c`

- [ ] **Step 1: 定义统一 result sink 接口**
- [ ] **Step 2: keyed 路径改成 sink 输出**
- [ ] **Step 3: bag 路径改成 sink 输出**
- [ ] **Step 4: 保持 stage `8/9` 进度语义**

### Task 4: 将入口改成直写 `UNLOGGED heap`

**Files:**
- Modify: `src/fb_entry.c`

- [ ] **Step 1: 结果表建表 SQL 改为 `CREATE UNLOGGED TABLE`**
- [ ] **Step 2: 删除 `tuplestore` 二次搬运循环**
- [ ] **Step 3: 新增 table sink，使用 bulk insert state 写表**
- [ ] **Step 4: 跑目标回归转绿**

### Task 5: 文档与验证收口

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/核心入口源码导读.md`
- Modify: `docs/architecture/源码级维护手册.md`
- Modify: `docs/architecture/调试与验证手册.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: 写明结果表不再是 `TEMP TABLE`**
- [ ] **Step 2: 标记这是并行 apply/write 的前置重构**
- [ ] **Step 3: 运行目标回归集**
- [ ] **Step 4: 运行全量回归**
