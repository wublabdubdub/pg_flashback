# TOAST Truncate Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `pg_flashback()` 在目标表 TOAST relation 发生 `SMGR TRUNCATE` 后，仍能在历史 TOAST chunk 可重建时成功返回正确历史结果。

**Architecture:** 保持主表 storage boundary 直接拒绝；把 TOAST `SMGR TRUNCATE` 从 WAL 扫描阶段 fatal unsafe 改成可继续 replay 的事件。历史结果仍由主表 replay + TOAST chunk store 重建，真正缺 chunk 时再报错。

**Tech Stack:** PostgreSQL extension C, PGXS regress, WAL scan/index, heap redo, TOAST chunk store

---

### Task 1: 写出新的失败回归

**Files:**
- Modify: `sql/fb_flashback_toast_storage_boundary.sql`
- Modify: `expected/fb_flashback_toast_storage_boundary.out`
- Test: `sql/fb_flashback_toast_storage_boundary.sql`

- [ ] **Step 1: 把现有回归改成“TOAST truncate 后仍应成功 flashback”**

把脚本改成：
- 记录 `target_ts`
- 建立 truth 表
- 更新宽列为小值并 `VACUUM`
- 校验 `toast_shrank`
- 比较 `pg_flashback(...)` 与 truth 的 `diff_count`

- [ ] **Step 2: 跑单测确认当前实现失败**

Run: `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_toast_storage_boundary'`
Expected: FAIL，当前仍会报 TOAST storage boundary 不支持

### Task 2: 收窄 WAL unsafe 边界

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`（仅当需要新 helper 声明时）
- Test: `sql/fb_flashback_toast_storage_boundary.sql`

- [ ] **Step 1: 添加“是否必须直接拒绝”的判定 helper**

规则：
- 主表 truncate / rewrite / storage change => true
- TOAST `SMGR TRUNCATE` => false
- TOAST `STANDBY LOCK` 若被同 xid 的 `SMGR TRUNCATE` 具体化 => false
- 其他 TOAST unsafe 保持保守

- [ ] **Step 2: 在 XACT commit 处按 helper 决定是否真正置 `ctx->unsafe`**

避免在 metadata 扫描阶段因为 TOAST truncate 提前退出。

- [ ] **Step 3: 若存在无 xid 的 TOAST truncate 直达分支，同样避免直接置 fatal unsafe**

### Task 3: 验证 replay/TOAST 主链

**Files:**
- Modify: `src/fb_replay.c`（仅当测试显示仍需最小修补）
- Modify: `src/fb_toast.c`（仅当测试显示仍需最小修补）
- Test: `sql/fb_flashback_toast_storage_boundary.sql`

- [ ] **Step 1: 先在不改 replay/toast 的前提下重跑回归**

Run: `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_toast_storage_boundary'`
Expected: PASS；若仍失败，记录具体缺 chunk / finalize 错误

- [ ] **Step 2: 只有在回归仍失败时，补最小 replay/TOAST 修正**

优先顺序：
- TOAST image 同步时机
- warm pass 覆盖范围
- finalize 时 chunk 可见性

不扩展到完整 relation-size 时态恢复。

### Task 4: 文档和回归收口

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Modify: `docs/architecture/overview.md`（若职责描述需要）
- Modify: `.gitignore`

- [ ] **Step 1: 更新状态文档**

记录：
- TOAST truncate 新边界
- 新回归口径
- 验证命令与结果

- [ ] **Step 2: 允许跟踪 `expected/fb_flashback_toast_storage_boundary.out`**

在 `.gitignore` 增加白名单，避免 expected 持续被忽略。

- [ ] **Step 3: 跑相关回归**

Run:
- `make PG_CONFIG=/home/18pg/local/bin/pg_config clean`
- `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`
- `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_storage_boundary fb_flashback_toast_storage_boundary fb_toast_flashback'`

Expected:
- TOAST truncate 场景通过
- 主表 storage boundary 继续报错
- 既有 TOAST flashback 回归不回退
