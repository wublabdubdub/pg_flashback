# Deep Full Baseline Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `tests/deep/bin/run_all_deep_tests.sh --full` 在 baseline 只导入一次的前提下，通过单份 PGDATA 快照恢复来重复执行各 batch，并支持失败/断连后的续跑。

**Architecture:** 在 `tests/deep/bin/common.sh` 增加 full 专用的快照、状态文件、磁盘容量校验与 PG 控制 helper。`run_all_deep_tests.sh --full` 改成“准备 baseline 快照一次，然后每个 batch 从同一快照恢复运行”的状态机，避免重复执行 `load_baseline.sh`。同时补一条 shell 级自测，验证快照 create/restore 和状态推进逻辑。

**Tech Stack:** Bash, PostgreSQL `pg_ctl`, `rsync`, existing deep test scripts

---

### Task 1: 先写 shell 级失败用例

**Files:**
- Create: `tests/deep/bin/test_full_snapshot_resume.sh`
- Modify: `tests/deep/bin/common.sh`

- [ ] **Step 1: 写一个会失败的 shell 测试**

测试目标：
- baseline 快照可创建
- 修改伪造的 PGDATA 后可恢复
- 状态文件可记录 `completed_batches`

- [ ] **Step 2: 运行测试，确认在实现前失败**

Run: `bash tests/deep/bin/test_full_snapshot_resume.sh`
Expected: 因缺少 `fb_deep_create_baseline_snapshot` / `fb_deep_restore_baseline_snapshot` / 状态 helper 而失败

### Task 2: 在 common.sh 增加 full 快照与状态 helper

**Files:**
- Modify: `tests/deep/bin/common.sh`
- Test: `tests/deep/bin/test_full_snapshot_resume.sh`

- [ ] **Step 1: 增加 full 快照目录、状态文件、PGCTL 配置**
- [ ] **Step 2: 增加磁盘空间校验 helper**
- [ ] **Step 3: 增加 stop/start cluster helper**
- [ ] **Step 4: 增加 create/restore/remove baseline 快照 helper**
- [ ] **Step 5: 增加状态文件读写与 batch 完成记录 helper**
- [ ] **Step 6: 运行 `bash tests/deep/bin/test_full_snapshot_resume.sh`，确认转绿**

### Task 3: 改 run_all_deep_tests.sh 的 full 状态机

**Files:**
- Modify: `tests/deep/bin/run_all_deep_tests.sh`
- Modify: `tests/deep/bin/bootstrap_env.sh`
- Modify: `tests/deep/bin/load_baseline.sh`
- Test: `tests/deep/bin/test_full_snapshot_resume.sh`

- [ ] **Step 1: full 模式先检查或创建 baseline 快照**
- [ ] **Step 2: 每个 batch 从 baseline 快照恢复后执行**
- [ ] **Step 3: batch 成功后标记状态，失败时保留状态以便续跑**
- [ ] **Step 4: 终端断连后再次执行 `--full` 时，从未完成 batch 续跑**
- [ ] **Step 5: 重跑 shell 自测，确保状态推进逻辑仍为绿色**

### Task 4: 文档与 dry-run 输出同步

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `tests/deep/README.md`

- [ ] **Step 1: 更新已完成/进行中状态**
- [ ] **Step 2: 在 `README` 中说明快照恢复与空间前置条件**
- [ ] **Step 3: 若 `--dry-run` 输出改变，更新说明**

### Task 5: 实机验证

**Files:**
- Modify: `tests/deep/bin/common.sh`
- Modify: `tests/deep/bin/run_all_deep_tests.sh`

- [ ] **Step 1: 跑 shell 自测**

Run: `bash tests/deep/bin/test_full_snapshot_resume.sh`
Expected: PASS

- [ ] **Step 2: 跑 full dry-run**

Run: `bash tests/deep/bin/run_all_deep_tests.sh --full --dry-run`
Expected: 输出 baseline 快照 + batch 恢复执行的流程

- [ ] **Step 3: 选一个 batch 做手工 smoke**

Run: 先创建 snapshot，再人为写入状态文件，重新执行 `--full`
Expected: 会从未完成 batch 续跑，而不是再次 load baseline
