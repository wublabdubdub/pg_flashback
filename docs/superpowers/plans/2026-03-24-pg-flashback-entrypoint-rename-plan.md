# pg_flashback Public Entrypoint Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将公开用户入口从 `fb_create_flashback_table(text, text, text)` 切换为 `pg_flashback(text, text, text)`，并移除旧入口的公开暴露。

**Architecture:** 保持现有“创建临时结果表”的行为和参数语义不变，只替换公开 SQL 名称与相关回归/文档。核心 flashback 执行链路继续复用 `fb_entry` 中的现有实现，避免引入与本次接口迁移无关的行为改动。

**Tech Stack:** PostgreSQL extension (PGXS), C, SQL install script, pg_regress, deep SQL scripts, Markdown documentation

---

### Task 1: 先更新项目记录

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/decisions/ADR-0003-user-interface.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: 记录接口迁移决策**

把“新主入口为 `pg_flashback(text, text, text)`、旧入口不保留兼容别名”写入状态、待办和 ADR。

- [ ] **Step 2: 人工检查记录是否自洽**

Run: `rg -n "pg_flashback\\(text, text, text\\)|已移除" STATUS.md TODO.md docs/decisions/ADR-0003-user-interface.md docs/architecture/overview.md`
Expected: 能看到“新入口已落地”和“旧入口已移除”的记录，不出现互相冲突的说法

### Task 2: 测试先行切到新入口

**Files:**
- Modify: `sql/fb_smoke.sql`
- Modify: `sql/fb_runtime_gate.sql`
- Modify: `sql/fb_flashback_keyed.sql`
- Modify: `sql/fb_flashback_bag.sql`
- Modify: `sql/fb_flashback_storage_boundary.sql`
- Modify: `sql/fb_user_surface.sql`
- Modify: `sql/fb_memory_limit.sql`
- Modify: `sql/fb_toast_flashback.sql`
- Modify: `sql/pg_flashback.sql`
- Modify: `tests/deep/sql/31_batch_a_keyed_validate.sql`
- Modify: `tests/deep/sql/61_batch_d_source_validate.sql`
- Modify: `tests/deep/sql/80_toast_scale.sql`
- Modify: `Makefile`

- [ ] **Step 1: 将测试调用改成 `pg_flashback(...)`**

保留断言语义不变，只替换函数名和必要的回归名。

- [ ] **Step 2: 运行目标回归验证当前失败**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck REGRESS="fb_smoke pg_flashback fb_user_surface"'`
Expected: 失败，原因是安装脚本尚未提供 `pg_flashback(text, text, text)`

### Task 3: 实现并切换公开安装面

**Files:**
- Modify: `include/fb_entry.h`
- Modify: `src/fb_entry.c`
- Modify: `sql/pg_flashback--0.1.0.sql`

- [ ] **Step 1: 将公开 SQL 入口实现为 `pg_flashback(text, text, text)`**

可以继续复用现有“解析文本参数并创建 `TEMP TABLE`”逻辑，但安装脚本必须只暴露新接口。

- [ ] **Step 2: 移除旧接口的公开安装**

安装脚本中不再创建 `fb_create_flashback_table(text, text, text)`。

- [ ] **Step 3: 运行目标回归转绿**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && make && make install && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck REGRESS="fb_smoke pg_flashback fb_user_surface"'`
Expected: 通过

### Task 4: 同步产品文档与验收

**Files:**
- Modify: `README.md`
- Modify: `PROJECT.md`
- Modify: `docs/specs/2026-03-22-pg-flashback-design.md`
- Modify: `docs/reports/2026-03-23-toast-scale-report.md`
- Modify: `docs/reports/2026-03-23-toast-full-report.md`
- Modify: `docs/specs/2026-03-23-embedded-ckwal-design.md`
- Modify: 其他仍引用旧公开用法的文档

- [ ] **Step 1: 将用户示例和“当前公开安装面”统一到 `pg_flashback(...)`**

仅保留历史语境中必要的旧接口说明，避免把它写成当前可用主入口。

- [ ] **Step 2: 运行最终验证**

Run: `su - 18pg -c 'cd /walstorage/pg_flashback && PGPORT=5832 rm -rf results regression.out regression.diffs && make installcheck'`
Expected: 全部回归通过

- [ ] **Step 3: 更新收尾记录**

在 `STATUS.md` 和 `TODO.md` 里补完已完成 / 进行中 / 下一步。
