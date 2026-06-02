# Flashback Main-Pipeline Parallel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变 flashback 语义正确性的前提下，把当前仅覆盖 WAL 前置辅助阶段的并行，逐步扩展到 WAL 主扫描、replay/reverse-source 和 apply 主链，让不同 SQL 形态都能从并行中普遍受益。

**Architecture:** 先清理当前并行基线，把“是否使用 prefilter”和“是否使用多 worker”解耦；随后按“WAL payload -> WAL metadata -> replay/reverse-source -> apply”顺序分阶段并行化。每一阶段都必须保持相同的 `anchor`、`unsafe`、`xid_statuses`、`RecordRef` 语义与最终历史结果，不允许为了并行改写结果语义。

**Tech Stack:** PostgreSQL extension (PGXS), C, pthreads, CustomScan, SQL regression, deep/manual benchmarks, Markdown docs

---

### Task 1: 先把路线、约束和阶段进度记入仓库

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `docs/decisions/ADR-0017-unified-flashback-parallel-switch.md`
- Create: `docs/superpowers/plans/2026-03-29-flashback-parallel-main-pipeline-plan.md`

- [x] **Step 1: 写明当前并行只覆盖 WAL 前置辅助阶段**
- [x] **Step 2: 固定分阶段改造顺序：serial prefilter baseline -> payload -> metadata -> replay/reverse-source -> apply**
- [x] **Step 3: 记录所有阶段都必须保持语义不变**

### Task 2: 先写失败回归，清理当前并行基线

**Files:**
- Modify: `sql/fb_wal_scan.sql`
- Modify: `expected/fb_wal_scan.out`
- Modify: `sql/fb_wal_sources.sql`
- Modify: `expected/fb_wal_sources.out`

- [x] **Step 1: 让 `parallel_workers = 0` 仍然要求 `prefilter=on`**
- [x] **Step 2: 保持 `parallel=off` 与 `parallel=on` 的用户口径不变**
- [x] **Step 3: 跑 WAL 相关回归，确认当前失败在 `prefilter` 语义**

### Task 3: 实现 Phase 1，保留串行 prefilter 基线

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Test: `sql/fb_wal_scan.sql`
- Test: `sql/fb_wal_sources.sql`

- [x] **Step 1: 把 prefilter 是否启用从 `parallel_workers > 0` 中拆出来**
- [x] **Step 2: `parallel_workers = 0` 时改为串行执行 prefilter/sidecar/validate**
- [x] **Step 3: `parallel_workers > 0` 时保持现有 worker fanout 行为不变**
- [x] **Step 4: 跑目标回归转绿**

### Task 4: 实现 Phase 2，WAL payload/materialize pass 并行

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [x] **Step 1: 写 payload pass 顺序保持合同测试**
- [x] **Step 2: 给 payload windows 分配 worker，本地落各自 spool log**
- [x] **Step 3: leader 按 window/LSN 顺序 merge，保持 `RecordRef` 时序不变**
- [x] **Step 4: 跑 recordref/wal-sidecar 相关回归**
  - 当前补充记录：
    - 已补 raw spool merge + anchor rebuild，减少 leader merge 开销
    - 已补连续大 payload window 的 segment 细分
    - 已补 overlap read + logical emit boundary，修复跨 segment record 丢失
    - live case `scenario_oa_12t_50000r.documents` 上，`parallel_workers=8` 时 `FbWalIndexScan` 约 `19.8s`，同环境 `parallel_workers=0` 对照约 `28.8s`

### Task 5: 实现 Phase 3，WAL metadata 两段式并行

**Files:**
- Modify: `src/fb_wal.c`
- Modify: `include/fb_wal.h`
- Modify: `STATUS.md`
- Modify: `TODO.md`

- [x] **Step 1: 写 touched_xids/xid_statuses/unsafe 不变合同测试**
- [x] **Step 2: Phase A 并行收集 checkpoint/touched_xids/unsafe 候选**
- [x] **Step 3: leader reduce 全局 touched_xids/anchor**
- [x] **Step 4: Phase B 改为 leader 串行扫描 `RM_XACT_ID` 回填事务状态**
- [x] **Step 5: 跑 WAL 与用户面回归**
  - 当前补充记录：
    - metadata 两段式并行 correctness 合同已验证
    - worker `xact_summary` spool / reduce 路径已放弃，不再保留为默认实现
    - 但 live case 上仍未打赢串行 metadata 基线，当前 prototype 保持关闭

### Task 6: 实现 Phase 4，replay/reverse-source 分片并行

**Files:**
- Modify: `src/fb_replay.c`
- Modify: `src/fb_reverse_ops.c`
- Modify: `include/fb_replay.h`
- Modify: `include/fb_reverse_ops.h`

- [ ] **Step 1: 先定义 replay/reverse-source 的分片和 merge 语义**
- [ ] **Step 2: 让每个 worker 只处理自己的输入分片**
- [ ] **Step 3: leader 统一 merge，保持结果顺序与 bag/keyed 语义**
- [ ] **Step 4: 跑 replay/reverse-op 相关回归**

### Task 7: 实现 Phase 5，apply 通用并行

**Files:**
- Modify: `src/fb_apply.c`
- Modify: `src/fb_apply_keyed.c`
- Modify: `src/fb_apply_bag.c`
- Modify: `src/fb_custom_scan.c`
- Modify: `include/fb_apply.h`

- [ ] **Step 1: keyed 按稳定 key hash 分片**
- [ ] **Step 2: bag 按 row identity hash 分片**
- [ ] **Step 3: 让 `FbApplyScan` 具备并行感知所需的共享状态**
- [ ] **Step 4: 跑 keyed/bag/user-surface 回归与 live benchmark**
  - 当前补充记录：
    - 已验证过一版 keyed query-side apply prototype
    - 路径为 shared reverse-source + parallel table scan + per-worker tuple spool + leader residual merge
    - correctness 路径可成立，但 live case 明显慢于稳定基线
    - 当前仍保持关闭，后续需要改成更低物化开销的通用方案后再打开

### Task 8: 阶段性验证与文档更新

**Files:**
- Modify: `STATUS.md`
- Modify: `TODO.md`
- Modify: `README.md`
- Modify: `docs/architecture/overview.md`
- Modify: `docs/architecture/wal-decode.md`

- [x] **Step 1: 每完成一个 phase，更新 STATUS/TODO 的“已完成/进行中/下一步”**
- [ ] **Step 2: 每完成一个 phase，补该阶段的 benchmark/验证记录**
- [ ] **Step 3: 最终补跑 installcheck、deep 和目标 live case**
