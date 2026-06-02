# Summaryd Watchdog / Cron Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `summaryd` 在缺 WAL 和异常退出场景下保持可自动恢复，并把 bootstrap 的默认交付面收口为单脚本 `start/stop/status` + cron 保活 watchdog。

**Architecture:** shell runner 改为固定路径的 watchdog 管理器，负责每秒拉起 child `pg_flashback-summaryd`；bootstrap 安装 per-user cron 只负责保活 watchdog；daemon 对缺 WAL 现场只进入等待重试，不再把生命周期交给异常退出。

**Tech Stack:** bash, PGXS summary daemon, smoke tests, markdown docs

---

### Task 1: 先补 runner/watchdog 失败测试

**Files:**
- Modify: `tests/summaryd/summary_runner_smoke.sh`
- Test: `tests/summaryd/summary_runner_smoke.sh`

- [ ] **Step 1: 写失败测试，锁住新 help 与新入口**

让 smoke test 断言：

- help 只出现 `start/stop/status`
- 不再出现 `run-once`
- 无需 `--config`

- [ ] **Step 2: 运行测试，确认当前实现失败**

Run: `bash tests/summaryd/summary_runner_smoke.sh`
Expected: FAIL，因为当前 runner 仍要求 `--config` 且 help 仍包含 `run-once`

- [ ] **Step 3: 继续补 child 被杀后自动拉起断言**

用 fake daemon 记录 `start_count`，杀死 child 后等待 watchdog 再次拉起。

- [ ] **Step 4: 再次运行测试，确认仍失败**

Run: `bash tests/summaryd/summary_runner_smoke.sh`
Expected: FAIL，因为当前 runner 没有 watchdog

### Task 2: 先补 bootstrap cron 失败测试

**Files:**
- Modify: `tests/summaryd/bootstrap_help_smoke.sh`
- Modify: `tests/summaryd/bootstrap_manual_runner_smoke.sh`
- Create: `tests/summaryd/bootstrap_cron_smoke.sh`
- Test: `tests/summaryd/bootstrap_cron_smoke.sh`

- [ ] **Step 1: 写失败测试，锁住 bootstrap 文案改成 start/stop/status**

- [ ] **Step 2: 写 cron install/remove 幂等 smoke**

让 fake `crontab` 记录当前 block，断言：

- setup 安装 block
- 重复 setup 不重复
- remove 清理 block

- [ ] **Step 3: 跑新测试确认失败**

Run: `bash tests/summaryd/bootstrap_cron_smoke.sh`
Expected: FAIL，因为当前 bootstrap 还不会安装 cron

### Task 3: 先补缺 WAL 等待失败测试

**Files:**
- Modify: `tests/summaryd/missing_segment_race_smoke.sh`
- Test: `tests/summaryd/missing_segment_race_smoke.sh`

- [ ] **Step 1: 写失败测试，锁住缺段后不退出的语义**

断言：

- 进程在等待窗口内仍存活
- `state.json` 持续刷新
- `debug.json` 的 `last_error` 体现等待态

- [ ] **Step 2: 运行测试确认当前实现失败**

Run: `bash tests/summaryd/missing_segment_race_smoke.sh`
Expected: FAIL，因为当前核心仍会把缺段 iteration 记为失败态

### Task 4: 实现 runner watchdog

**Files:**
- Modify: `scripts/pg_flashback_summary.sh`
- Test: `tests/summaryd/summary_runner_smoke.sh`

- [ ] **Step 1: 增加固定路径解析与隐藏 watchdog 模式**
- [ ] **Step 2: 实现 `start` 幂等拉起 watchdog**
- [ ] **Step 3: 实现 `stop` 同停 watchdog 与 child**
- [ ] **Step 4: 实现 `status` 输出 watchdog / child 两层状态**
- [ ] **Step 5: 跑 runner smoke 直到通过**

Run: `bash tests/summaryd/summary_runner_smoke.sh`
Expected: PASS

### Task 5: 实现 bootstrap cron 安装与清理

**Files:**
- Modify: `scripts/b_pg_flashback.sh`
- Modify: `tests/summaryd/bootstrap_help_smoke.sh`
- Modify: `tests/summaryd/bootstrap_manual_runner_smoke.sh`
- Create: `tests/summaryd/bootstrap_cron_smoke.sh`

- [ ] **Step 1: 增加 cron block 生成/安装/删除函数**
- [ ] **Step 2: setup 路径调用 cron 安装**
- [ ] **Step 3: remove 路径调用 cron 清理**
- [ ] **Step 4: 更新 dry-run / help /输出字段**
- [ ] **Step 5: 跑 bootstrap cron 相关 smoke**

Run: `bash tests/summaryd/bootstrap_help_smoke.sh && bash tests/summaryd/bootstrap_manual_runner_smoke.sh && bash tests/summaryd/bootstrap_cron_smoke.sh`
Expected: PASS

### Task 6: 实现 daemon 缺 WAL 等待语义

**Files:**
- Modify: `summaryd/fb_summaryd_core.c`
- Modify: `summaryd/fb_summaryd_core.h`
- Possibly Modify: `src/fb_summary.c`
- Test: `tests/summaryd/missing_segment_race_smoke.sh`

- [ ] **Step 1: 分类缺 WAL / 缺 next segment 为 waitable error**
- [ ] **Step 2: waitable error 不再把 `service_enabled` 置为 `false`**
- [ ] **Step 3: 保留 `last_error` 供状态面观测**
- [ ] **Step 4: 跑缺段 smoke 直到通过**

Run: `bash tests/summaryd/missing_segment_race_smoke.sh`
Expected: PASS

### Task 7: 更新 README 与开源镜像同步面

**Files:**
- Modify: `README.md`
- Modify: `summaryd/README.md`
- Test: `tests/summaryd/readme_surface_smoke.sh`

- [ ] **Step 1: 更新用户面文档到 start/stop/status + cron**
- [ ] **Step 2: 删除 run-once 的用户面表述**
- [ ] **Step 3: 跑 README smoke**

Run: `bash tests/summaryd/readme_surface_smoke.sh`
Expected: PASS

### Task 8: 汇总验证与开源镜像同步

**Files:**
- Modify: `Makefile`
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/decisions/ADR-0036-summaryd-watchdog-and-cron-supervision.md`
- Modify: `docs/specs/2026-04-11-summaryd-watchdog-and-cron-design.md`

- [ ] **Step 1: 把新 smoke 加入 `check-summaryd`**
- [ ] **Step 2: 跑 summaryd / bootstrap 相关验证**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config check-summaryd`
Expected: PASS

- [ ] **Step 3: 同步开源镜像**

Run: `bash scripts/sync_open_source.sh`
Expected: PASS

- [ ] **Step 4: 回填 STATUS/TODO 完成态**
