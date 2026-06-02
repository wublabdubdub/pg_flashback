# Full-Output pg_flashback Fast Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)` as the official full-table flashback materialization path while accelerating the shared full-output path used by plain `SELECT`, `CTAS`, and `COPY (query) TO`.

**Architecture:** Preserve the existing `FbWalIndexScan -> FbReplay* -> FbReverseSource -> FbApply` chain, but add a shared full-output fast path for simple `SELECT *`, `CTAS AS SELECT *`, and `COPY (query) TO`. PostgreSQL native `CTAS` bulk insert receiver remains in place; the optimization target is the upstream row production and tuple-movement cost inside `pg_flashback(...)`.

**Tech Stack:** PostgreSQL extension C, CustomScan/planner hooks, executor APIs, PGXS regress tests, `EXPLAIN (VERBOSE)`, PG18 local runtime

---

### Task 1: 固化用户面与计划入口

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `PROJECT.md`
- Create: `docs/decisions/ADR-0018-ctas-over-pg-flashback-direct-sink.md`
- Create: `docs/superpowers/specs/2026-04-02-ctas-flashback-sink-design.md`

- [ ] **Step 1: 核对并补齐文档约束**

确认文档中明确写出：

- `pg_flashback_to(regclass, text)` 删除后
- 库内新表落地正式走 `CTAS AS SELECT * FROM pg_flashback(...)`
- `COPY (SELECT * FROM pg_flashback(...)) TO ...` 仅作为导出路径

- [ ] **Step 2: 写入 ADR 与设计稿**

确保 ADR 和设计稿都覆盖：

- 为什么 `COPY` 不是建表主路径
- 为什么当前最值得优化的是 CTAS 末端落表
- 为什么不新增新的公开 helper

- [ ] **Step 3: 自查文档一致性**

Run: `rg -n "CTAS|COPY \\(SELECT|pg_flashback_to" STATUS.md TODO.md PROJECT.md docs/decisions docs/superpowers/specs -S`

Expected:

- 新设计文档与 ADR 已出现
- 现行产品边界不再把 `pg_flashback_to` 当成未来路线

### Task 2: 写回归，先让 full-output 快路径红灯

**Files:**
- Create: `sql/fb_flashback_ctas.sql`
- Create: `expected/fb_flashback_ctas.out`
- Modify: `Makefile`

- [ ] **Step 1: 写一个只覆盖简单 CTAS 的新回归**

测试 SQL 需要至少断言：

- `EXPLAIN (VERBOSE, COSTS OFF)` 的以下三种写法最终会共享同一条 full-output 口径：
  - `SELECT * FROM pg_flashback(...)`
  - `CTAS AS SELECT * FROM pg_flashback(...)`
  - `COPY (SELECT * FROM pg_flashback(...)) TO ...`
- `SELECT * FROM pg_flashback(...)` 与 `CTAS` 的结果行数一致
- `COPY (SELECT * FROM pg_flashback(...)) TO STDOUT` 不会回退到不同的低效实现

- [ ] **Step 2: 预埋新的 explain/debug 断言**

回归中预期先写成未来目标，例如：

- explain 输出出现 `Flashback Full Output Fast Path: true`

实现前先让该断言失败。

- [ ] **Step 3: 运行回归确认红灯**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_flashback_ctas'`

Expected: FAIL，diff 显示缺少 full-output 快路径标记

### Task 3: 抽离 full-output 模块边界

**Files:**
- Create: `include/fb_full_output.h`
- Create: `src/fb_full_output.c`
- Modify: `Makefile`
- Modify: `docs/architecture/overview.md`

- [ ] **Step 1: 新建 full-output 模块骨架**

定义最小接口，例如：

```c
typedef struct FbFullOutputSpec
{
    bool enabled;
} FbFullOutputSpec;

bool fb_full_output_plan_enabled(PlannedStmt *stmt, Query *query);
void fb_full_output_prepare(FbFullOutputState *state, TupleDesc tupdesc);
bool fb_full_output_emit_slot(FbFullOutputState *state, TupleTableSlot *slot,
                              FbApplyEmit *emit);
```

- [ ] **Step 2: 在架构文档登记职责**

补充 `fb_full_output` 负责：

- simple full-output 场景识别
- 共享行产出/tuple 搬运优化
- 不替换 PostgreSQL 原生 `CTAS` receiver

- [ ] **Step 3: 编译确认模块骨架通过**

Run:

```bash
make PG_CONFIG=/home/18pg/local/bin/pg_config clean
make PG_CONFIG=/home/18pg/local/bin/pg_config -j4
```

Expected: PASS

### Task 4: 识别 simple full-output 场景

**Files:**
- Modify: `src/fb_custom_scan.c`
- Modify: `include/fb_custom_scan.h`
- Modify: `src/fb_entry.c`

- [ ] **Step 1: 写一个保守匹配器**

匹配条件仅覆盖：

- 查询主体是简单 `SELECT * FROM pg_flashback(...)`
- 无 join / agg / sort / limit / projection rewrite

- [ ] **Step 2: 将匹配结果放进计划私有信息**

把是否启用 full-output 快路径写进 `CustomScan` private 或 executor 初始化状态。

- [ ] **Step 3: 用 explain 暴露命中状态**

在 `EXPLAIN (VERBOSE)` 中输出清晰标记，例如：

- `Flashback Full Output Fast Path: true`

- [ ] **Step 4: 运行回归验证 explain 已命中**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_flashback_ctas fb_user_surface'`

Expected: PASS 或仅剩行发射相关断言未满足

### Task 5: 把 apply 输出收口到 full-output 快路径

**Files:**
- Modify: `src/fb_apply.c`
- Modify: `include/fb_apply.h`
- Modify: `src/fb_full_output.c`
- Modify: `include/fb_full_output.h`
- Modify: `src/fb_custom_scan.c`

- [ ] **Step 1: 增加 apply emit 到 sink 的最小桥接**

优先支持：

- `FB_APPLY_EMIT_TUPLE`
- `FB_APPLY_EMIT_SLOT`

统一转换成目标表写入可接受的 `HeapTuple` 或 slot。

- [ ] **Step 2: 在 full-output 分支下减少通用 emit 搬运**

让 simple full-output 命中时：

- `apply` 继续生成历史结果
- 尽量避免多余 `HeapTuple` / `TupleTableSlot` 往返
- 保持外层仍使用 PostgreSQL 原生 receiver

- [ ] **Step 3: 保持复杂查询完全不变**

明确保留：

- `fb_apply_next_output_slot()`
- 现有 `ExecScan(...)` 路径

只让 simple full-output 命中时走新分支。

- [ ] **Step 4: 跑新回归和现有用户面回归**

Run:

```bash
make PG_CONFIG=/home/18pg/local/bin/pg_config clean
make PG_CONFIG=/home/18pg/local/bin/pg_config -j4
make PG_CONFIG=/home/18pg/local/bin/pg_config install
make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_flashback_ctas fb_user_surface pg_flashback'
```

Expected: PASS

### Task 6: 做正确性与三条路径一致性补强

**Files:**
- Modify: `sql/fb_flashback_ctas.sql`
- Modify: `expected/fb_flashback_ctas.out`
- Modify: `src/fb_full_output.c`

- [ ] **Step 1: 增加空表、小表、TOAST-heavy 的 CTAS 覆盖**

至少断言：

- 空结果 `CTAS` 不报错
- 小表结果行数一致
- TOAST-heavy 场景不会因 sink 写入破坏结果

- [ ] **Step 2: 增加三条路径一致性断言**

至少覆盖：

- `SELECT` 与 `CTAS` 行数一致
- `SELECT` 与 `COPY (query) TO` 行数/输出一致
- 复杂查询不误命中新快路径

- [ ] **Step 3: 运行定向回归**

Run: `make PG_CONFIG=/home/18pg/local/bin/pg_config PGPORT=5832 PGUSER=18pg installcheck REGRESS='fb_flashback_ctas fb_toast_flashback fb_progress'`

Expected: PASS

### Task 7: 做性能验证并把结论写回文档

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Optional: `docs/reports/2026-04-02-ctas-flashback-sink-report.md`

- [ ] **Step 1: 选 3 个时间点做冷启动/热缓存对比**

建议沿用：

- `2026-03-31 22:40:13`
- `2026-04-01 01:40:13`
- `2026-04-01 23:15:13`

- [ ] **Step 2: 分别测试 3 条路径**

至少记录：

- `SELECT count(*) FROM pg_flashback(...)`
- `CREATE TABLE ... AS SELECT * FROM pg_flashback(...)`
- `COPY (SELECT * FROM pg_flashback(...)) TO PROGRAM 'cat >/dev/null'`

- [ ] **Step 3: 明确收益归因**

记录：

- `3/9` 是否变化
- replay/apply 是否变化
- `CTAS` 总耗时下降多少
- 新瓶颈是否转移到表写入/WAL/索引创建

- [ ] **Step 4: 回写状态与剩余问题**

若 full-output 快路径已成立，则在 `STATUS.md` / `TODO.md` 中更新：

- 已完成项
- 剩余性能瓶颈
- 是否还需要更激进的 sidecar / pre-materialize 路线
