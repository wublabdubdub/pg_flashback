# Core Archive Autodiscovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `pg_flashback` 在未显式设置 `pg_flashback.archive_dest` 时，优先按 PostgreSQL 内核当前生效的归档配置自动推断本地 WAL 归档目录，并保留显式覆盖优先级。

**Architecture:** 归档目录解析统一收口到 `fb_guc`，对外继续提供单一“effective archive dir”接口。解析顺序固定为：显式 `pg_flashback.archive_dest` -> 兼容 `pg_flashback.archive_dir` -> 可安全识别的 PostgreSQL `archive_command` / `pg_probackup archive-push` 本地目录。`archive_library` 非空或命令不可安全识别时，不做猜测，要求用户显式配置。

**Tech Stack:** PostgreSQL C extension, PGXS regress, SQL regression fixtures

---

### Task 1: 同步文档口径

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/wal-source-resolution.md`
- Modify: `docs/decisions/ADR-0004-wal-source-resolution.md`
- Modify: `README.md`

- [ ] **Step 1: 记录新来源解析规则**

写明：
- `pg_flashback.archive_dest` 仍是最高优先级显式覆盖项
- 未设置时可从 PostgreSQL 内核 `archive_command` 自动推断本地 archive 目录
- `archive_library` 非空或复杂命令不自动猜测
- `pg_probackup archive-push -B ... --instance ...` 本地写法可自动识别

- [ ] **Step 2: 自检文档一致性**

Run: `rg -n "archive_dest|archive_dir|archive_command|pg_probackup" STATUS.md TODO.md PROJECT.md docs/architecture/overview.md docs/architecture/wal-source-resolution.md docs/decisions/ADR-0004-wal-source-resolution.md README.md`
Expected: 文档口径一致，不再只强调手工配置 `archive_dest`

### Task 2: 先写失败回归

**Files:**
- Modify: `Makefile`
- Modify: `sql/fb_wal_source_policy.sql`
- Create: `expected/fb_wal_source_policy.out`

- [ ] **Step 1: 写出失败用例**

覆盖三类行为：
- 显式 `pg_flashback.archive_dest` 优先，不受 `archive_command` 干扰
- 未显式设置时，可从简单 `archive_command = 'cp %p /path/%f'` 推断归档目录
- `pg_probackup archive-push -B backup_dir --instance instance_name --wal-file-name=%f` 可推断为 `backup_dir/wal/instance_name`
- `archive_library` 非空或复杂 `archive_command` 时，回退到“要求显式配置”

- [ ] **Step 2: 运行回归确认先失败**

Run: `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_wal_source_policy'`
Expected: FAIL，原因是当前还没有内核归档配置自动解析能力

### Task 3: 实现最小自动发现

**Files:**
- Modify: `include/fb_guc.h`
- Modify: `src/fb_guc.c`
- Modify: `src/fb_wal.c`
- Modify: `src/fb_ckwal.c`

- [ ] **Step 1: 在 `fb_guc` 中新增统一解析入口**

实现：
- 显式 `pg_flashback.archive_dest`
- 兼容 `pg_flashback.archive_dir`
- 自动读取 PostgreSQL `archive_library`
- 自动读取 PostgreSQL `archive_command`

- [ ] **Step 2: 只支持安全可识别的本地命令模式**

最小支持：
- `cp %p /path/%f`
- `test ! -f /path/%f && cp %p /path/%f`
- `pg_probackup archive-push -B backup_dir --instance=instance_name --wal-file-name=%f`
- `pg_probackup archive-push -B backup_dir --instance instance_name --wal-file-name=%f`

拒绝：
- `archive_library` 非空
- 远程 / 变量展开 / wrapper script / 无法稳定提取本地目录的写法

- [ ] **Step 3: 让 WAL 路径消费者只走统一解析结果**

确保 `fb_wal` / `fb_ckwal` / 运行时 gate 都使用新的 effective archive dir / source mode 解析，不分散复制逻辑。

### Task 4: 验证并收尾

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [ ] **Step 1: 构建并安装**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config && make PG_CONFIG=/home/18pg/local/bin/pg_config install`
Expected: success

- [ ] **Step 2: 跑目标回归**

Run: `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_wal_source_policy fb_guc_defaults fb_progress'`
Expected: All tests passed.

- [ ] **Step 3: 同步状态文档**

把完成情况写回 `STATUS.md` / `TODO.md`，记录本轮验证命令与结果。
