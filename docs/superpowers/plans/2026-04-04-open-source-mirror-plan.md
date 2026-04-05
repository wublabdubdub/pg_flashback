# Open Source Mirror Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repeatable `open_source/pg_flashback/` mirror that keeps only the public PostgreSQL extension assets and can be refreshed from the internal repository with one script.

**Architecture:** The repository root remains the source of truth. A whitelist-driven sync script rebuilds `open_source/pg_flashback/` from root-owned public assets, while internal docs, reports, logs, and heavy test assets stay in the main repository only.

**Tech Stack:** Bash, PGXS repository layout, Markdown documentation

---

### Task 1: 写入公开镜像规则文档

**Files:**
- Create: `docs/superpowers/specs/2026-04-04-open-source-mirror-design.md`
- Create: `open_source/README.md`
- Create: `open_source/manifest.txt`

- [ ] **Step 1: 写入已确认设计规格**

在 `docs/superpowers/specs/2026-04-04-open-source-mirror-design.md` 记录：

- `open_source/pg_flashback/` 是长期维护的开源镜像目录
- 根仓库仍为权威源
- 白名单同步与明确排除列表

- [ ] **Step 2: 写入开源目录说明**

在 `open_source/README.md` 说明：

- `open_source/pg_flashback/` 的用途
- 如何执行同步脚本
- 哪些内容不会进入开源镜像

- [ ] **Step 3: 写入白名单清单**

在 `open_source/manifest.txt` 列出：

- 需要同步的文件和目录
- 明确排除的路径类别

### Task 2: 实现同步脚本与镜像骨架

**Files:**
- Create: `scripts/sync_open_source.sh`
- Create: `open_source/pg_flashback/.gitignore`
- Create: `open_source/pg_flashback/README.md`
- Create: `open_source/pg_flashback/docs/README.md`
- Create: `open_source/pg_flashback/tests/README.md`

- [ ] **Step 1: 写一个失败前提检查**

脚本先检查仓库根目录下这些路径存在：

```bash
Makefile
README.md
pg_flashback.control
include
src
sql
expected
```

- [ ] **Step 2: 实现镜像目录重建**

脚本应：

```bash
rm -rf open_source/pg_flashback
mkdir -p open_source/pg_flashback
```

然后仅复制白名单路径。

- [ ] **Step 3: 写入镜像内专用说明文件**

为镜像目录补充：

- `.gitignore`
- 面向 GitHub 的 `README.md`
- `docs/README.md`
- `tests/README.md`

- [ ] **Step 4: 加入排除检查**

脚本同步后断言以下路径不存在：

```bash
open_source/pg_flashback/docs/reports
open_source/pg_flashback/docs/superpowers
open_source/pg_flashback/tests/deep
open_source/pg_flashback/tests/release_gate
open_source/pg_flashback/results
```

### Task 3: 同步项目状态与架构文档

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: 在 STATUS.md 登记当前完成项**

补充：

- 已建立开源镜像目录方案
- 已新增同步脚本与白名单
- 根仓库继续保留内部资料

- [ ] **Step 2: 在 TODO.md 登记后续维护项**

补充一个持续任务：

- 每次准备同步 GitHub 前执行 `scripts/sync_open_source.sh`

- [ ] **Step 3: 在架构总览补充仓库发布结构**

在 `docs/architecture/overview.md` 简要说明：

- 根仓库为研发源
- `open_source/pg_flashback/` 为白名单同步生成的公开镜像

### Task 4: 执行验证

**Files:**
- Verify: `open_source/pg_flashback/`

- [ ] **Step 1: 运行同步脚本**

Run: `bash scripts/sync_open_source.sh`
Expected: 同步完成，镜像目录生成成功

- [ ] **Step 2: 检查目录边界**

Run: `find open_source/pg_flashback -maxdepth 3 | sort`
Expected: 只出现公开白名单内容和镜像内说明文件

- [ ] **Step 3: 检查排除项**

Run: `find open_source/pg_flashback -path '*/reports/*' -o -path '*/superpowers/*' -o -path '*/deep/*' -o -path '*/release_gate/*'`
Expected: 无输出

- [ ] **Step 4: 复核工作区变更**

Run: `git status --short`
Expected: 只出现本次文档、脚本和开源镜像相关改动
