# TOAST Reconstruction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `pg_flashback` 增加历史 TOAST 值重建能力，并在实现后执行规模化 TOAST 测试、记录问题并修复。

**Architecture:** 继续沿用现有 `checkpoint -> RecordRef -> BlockReplayStore -> ForwardOp/ReverseOp` 主线，不打乱 WAL 全局 LSN 顺序。TOAST 方案通过“先重放 TOAST relation block，再在主表行像构造阶段把 external toast pointer 解析为 inline 值”实现，读取路径重定向到扩展内部历史 TOAST store，而不是修改 tuple 内部指针结构。

**Tech Stack:** PostgreSQL C extension, PG18 heap/TOAST internals, PGXS regression tests, deep test scripts.

---

### Task 1: 固化 TOAST 失败测试

**Files:**
- Create: `sql/fb_toast_flashback.sql`
- Create: `expected/fb_toast_flashback.out`
- Modify: `Makefile`

- [ ] **Step 1: 写最小 TOAST 场景回归**
- [ ] **Step 2: 运行单测确认当前失败点与 TOAST 历史值缺失相关**

### Task 2: 建立 TOAST 接口与数据结构

**Files:**
- Modify: `include/fb_toast.h`
- Modify: `src/fb_toast.c`
- Modify: `src/fb_replay.c`
- Modify: `include/fb_replay.h`

- [ ] **Step 1: 定义历史 TOAST chunk store 接口**
- [ ] **Step 2: 实现最小 `(chunk_id, chunk_seq, chunk_data)` 存取**
- [ ] **Step 3: 将 TOAST store 接入 replay 上下文**

### Task 3: 将 TOAST relation 接入页级重放

**Files:**
- Modify: `src/fb_replay.c`

- [ ] **Step 1: 对 toast heap insert/delete/update 更新 chunk store**
- [ ] **Step 2: 保持主表和 TOAST relation 按全局 LSN 同步重放**
- [ ] **Step 3: 对 chunk store 增加最小内存计量**

### Task 4: 主表行像的 TOAST 读取重定向

**Files:**
- Modify: `src/fb_replay.c`
- Modify: `src/fb_toast.c`
- Modify: `include/fb_toast.h`

- [ ] **Step 1: 在行像构造阶段识别 external toast pointer**
- [ ] **Step 2: 用历史 TOAST store 拼出 inline varlena**
- [ ] **Step 3: 构造不依赖 live TOAST 的 `HeapTuple`**
- [ ] **Step 4: chunk 缺失时给出明确错误**

### Task 5: 回归覆盖与全量验证

**Files:**
- Modify: `sql/fb_toast_flashback.sql`
- Modify: `expected/fb_toast_flashback.out`
- Modify: `Makefile`

- [ ] **Step 1: 扩到 `INSERT/UPDATE/DELETE` 全覆盖**
- [ ] **Step 2: 跑全量回归并修正实现**

### Task 6: 规模化 TOAST 测试与问题闭环

**Files:**
- Create: `tests/deep/sql/toast_schema.sql`
- Create: `tests/deep/sql/toast_workload.sql`
- Create: `tests/deep/sql/toast_validate.sql`
- Create: `tests/deep/bin/run_toast_deep.sh`
- Create: `docs/reports/2026-03-23-toast-deep-report.md`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: 构造带大字段、多轮 DML 的 TOAST 深测场景**
- [ ] **Step 2: 运行规模化 TOAST 测试**
- [ ] **Step 3: 把发现的问题逐条记入报告**
- [ ] **Step 4: 逐项修复问题并回归**
