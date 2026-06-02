# pg_flashback Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立 `pg_flashback` 的长期项目骨架、最小扩展结构和首个可运行测试入口。

**Architecture:** 先固定文档、模块边界和最小 PGXS 扩展骨架，再进入 PG18 基线的 WAL 解码实现。查询主路径固定为 reverse op stream，当前阶段只搭入口和接口占位，不实现真实 flashback。

**Tech Stack:** PostgreSQL extension (PGXS), C, SQL install script, pg_regress, Markdown documentation

---

### Task 1: 仓库与文档基础

**Files:**
- Create: `README.md`
- Create: `AGENTS.md`
- Create: `PROJECT.md`
- Create: `STATUS.md`
- Create: `TODO.md`
- Create: `docs/specs/2026-03-22-pg-flashback-design.md`
- Create: `docs/architecture/overview.md`
- Create: `docs/architecture/reverse-op-stream.md`
- Create: `docs/architecture/row-identity.md`
- Create: `docs/architecture/wal-decode.md`
- Create: `docs/architecture/error-model.md`
- Create: `docs/decisions/ADR-0001-reverse-op-stream.md`
- Create: `docs/decisions/ADR-0002-read-only-first.md`
- Create: `docs/roadmap/phases.md`

- [ ] **Step 1: 写入设计与状态文档**

确保文件职责明确，内容与已拍板约束一致。

- [ ] **Step 2: 人工检查文档一致性**

Run: `rg -n "pfb|best-effort|自动执行 undo" README.md AGENTS.md PROJECT.md STATUS.md TODO.md docs`
Expected: 不出现违背约束的内容

### Task 2: 最小测试先行

**Files:**
- Create: `sql/fb_smoke.sql`
- Create: `expected/fb_smoke.out`

- [ ] **Step 1: 编写最小失败测试**

测试目标：
- 能创建扩展
- 能调用 `fb_version()`
- `pg_flashback()` 目前返回明确的“尚未实现”错误

- [ ] **Step 2: 运行测试验证当前失败**

Run: `make installcheck`
Expected: 失败，原因是扩展骨架尚未实现或无法安装

### Task 3: 最小扩展骨架

**Files:**
- Create: `Makefile`
- Create: `pg_flashback.control`
- Create: `sql/pg_flashback--0.1.0.sql`
- Create: `include/fb_common.h`
- Create: `include/fb_catalog.h`
- Create: `include/fb_entry.h`
- Create: `include/fb_error.h`
- Create: `src/fb_entry.c`
- Create: `src/fb_catalog.c`
- Create: `src/fb_error.c`

- [ ] **Step 1: 实现最小可编译扩展**

包含：
- `fb_version()`
- `fb_check_relation(regclass)`
- `pg_flashback(regclass, timestamptz)` 占位错误
- `fb_export_undo(regclass, timestamptz)` 占位错误

- [ ] **Step 2: 运行测试验证通过**

Run: `make && make installcheck`
Expected: smoke test 通过；未实现 API 返回预期错误

### Task 4: 为长期实现预留模块边界

**Files:**
- Create: `include/fb_decode.h`
- Create: `include/fb_reverse_ops.h`
- Create: `include/fb_apply.h`
- Create: `include/fb_export.h`
- Create: `include/fb_toast.h`
- Create: `include/fb_compat.h`
- Create: `src/fb_decode.c`
- Create: `src/fb_reverse_ops.c`
- Create: `src/fb_apply_keyed.c`
- Create: `src/fb_apply_bag.c`
- Create: `src/fb_export.c`
- Create: `src/fb_toast.c`
- Create: `src/fb_compat_pg18.c`

- [ ] **Step 1: 添加占位模块和注释**

每个模块只保留最小函数签名和责任注释。

- [ ] **Step 2: 确认代码可继续编译**

Run: `make`
Expected: 编译成功

### Task 5: 收尾

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: 更新状态文件**

记录已完成内容、风险、下一步。

- [ ] **Step 2: 提交基础骨架**

```bash
git add .
git commit -m "chore: initialize pg_flashback foundation"
```

