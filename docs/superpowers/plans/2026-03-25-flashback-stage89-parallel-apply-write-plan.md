# Flashback Stage 8/9 Parallel Apply/Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `pg_flashback()` 新增 opt-in 的 bgworker 并行 apply/write 路径，在 `parallel_apply_workers > 0` 时把 stage `8/9` 切到真正的并行结果表写入。

**Architecture:** 默认保留当前串行 `UNLOGGED` 直写路径；并行模式新增 `fb_parallel` 模块，使用 dynamic bgworker + DSM + `shm_mq`。leader 先通过 helper bgworker 自主事务创建结果表，再把 current tuples 和 primitive reverse-op items 分发到多个 apply worker；worker 本地完成 keyed/bag apply，汇总行数后并行写结果表，leader 统一更新 stage `8/9` 进度。

**Tech Stack:** PostgreSQL extension (PGXS), C, dynamic background workers, DSM, `shm_mq`, SQL regression, Markdown docs

---

### Task 1: 先把设计与仓库状态登记清楚

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Create: `docs/superpowers/specs/2026-03-25-flashback-stage89-parallel-apply-write-design.md`

- [ ] **Step 1: 记录 `ParallelContext` 只读限制与未提交结果表不可见这两个 blocker**
- [ ] **Step 2: 固定 opt-in 的 `parallel_apply_workers` 口径**
- [ ] **Step 3: 在架构总览登记即将新增的 `fb_parallel` 模块职责**

### Task 2: 先写失败回归，让并行合同先变红

**Files:**
- Create: `sql/fb_parallel_apply.sql`
- Create: `expected/fb_parallel_apply.out`
- Modify: `Makefile`

- [ ] **Step 1: 增加 keyed 场景，显式开启 `parallel_apply_workers = 2`**
- [ ] **Step 2: 让 expected 要求 stage `8` detail 出现 `parallel workers=2`**
- [ ] **Step 3: 增加 `relpersistence = 'u'` 与结果正确性断言**
- [ ] **Step 4: 跑新回归，确认当前失败**

### Task 3: 引入并行 GUC 与入口决策

**Files:**
- Modify: `src/fb_guc.c`
- Modify: `include/fb_guc.h`
- Modify: `src/fb_entry.c`

- [ ] **Step 1: 新增 `pg_flashback.parallel_apply_workers`**
- [ ] **Step 2: 在入口按 GUC 选择串行或并行路径**
- [ ] **Step 3: 保持默认 `0` 时当前行为不变**

### Task 4: 搭建 `fb_parallel` 基础设施

**Files:**
- Create: `include/fb_parallel.h`
- Create: `src/fb_parallel.c`
- Modify: `Makefile`

- [ ] **Step 1: 定义 helper/apply worker 共享结构、消息类型和 DSM key**
- [ ] **Step 2: 实现结果表创建 helper**
- [ ] **Step 3: 实现 apply worker 启动、等待、消息发送和关闭**
- [ ] **Step 4: 实现错误清理 helper**

### Task 5: 提炼 worker 本地 apply 状态机

**Files:**
- Modify: `include/fb_apply.h`
- Modify: `src/fb_apply_keyed.c`
- Modify: `src/fb_apply_bag.c`

- [ ] **Step 1: 把 keyed/bag 状态构建与 primitive item 应用抽成可复用 helper**
- [ ] **Step 2: 让 leader 串行路径继续复用这些 helper**
- [ ] **Step 3: 让 bgworker 路径只依赖可序列化 tuple message**

### Task 6: 接上并行 stage 8/9 进度与结果写表

**Files:**
- Modify: `src/fb_progress.c`
- Modify: `src/fb_entry.c`
- Modify: `src/fb_parallel.c`

- [ ] **Step 1: stage `8` 增加 `parallel workers=<n>` detail**
- [ ] **Step 2: 用 worker apply 完成数推动 stage `8` 的 `80 -> 100`**
- [ ] **Step 3: 用全局 `total_rows / emitted_rows` 推动 stage `9`**
- [ ] **Step 4: 跑新回归转绿**

### Task 7: 文档、验证、风险收口

**Files:**
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/源码级维护手册.md`
- Modify: `docs/architecture/核心入口源码导读.md`
- Modify: `docs/architecture/调试与验证手册.md`

- [ ] **Step 1: 写明并行路径是显式开启且结果表生命周期独立于调用方事务**
- [ ] **Step 2: 写明默认仍是串行直写**
- [ ] **Step 3: 运行目标回归集**
- [ ] **Step 4: 运行全量回归**
