# 闪回失败修复台账

## 维护规则

- 本文档只收录“已经定位根因并已完成修复”的闪回失败问题。
- 每次新增或修复一条导致 `pg_flashback()` 查询失败、崩溃、或被错误阻断的问题时，必须同步更新本文档。
- 每条记录固定使用以下结构：
  - 时间
  - 报错现象
  - 原因
  - 修复方式
- 纯性能优化、纯测试夹具问题、尚未修复完成的 blocker，不写入本文档。

---

## 2026-04-08

### 记录 1：`failed to replay heap multi insert`

- 时间：
  - 2026-04-08
- 报错现象：
  - `scenario_oa_50t_50000r.leave_requests @ '2026-04-08 01:37:20.067024+00'`
    执行
    `select * from pg_flashback(...)`
    时稳定报：
    - `WARNING: will not overwrite a used ItemId`
    - `ERROR: failed to replay heap multi insert`
- 原因：
  - prune lookahead 的 future constraints 旧逻辑没有覆盖
    `HEAP2_MULTI_INSERT`
  - data prune 的 future guard 又把 future old tuple 过度从
    `nowdead/redirected` 中剔除，和当前 dead/redirect 可追踪语义不一致
  - 两者叠加后，final replay 会保留错误页状态，导致后续 multi-insert
    目标 offset 被错误视为“已占用”
- 修复方式：
  - 为 `FB_WAL_RECORD_HEAP2_MULTI_INSERT` 补齐 future compose 与
    same-block future support
  - 调整 prune future guard：
    - future old tuple 仅阻止 `nowunused`
    - future new slot 将 `nowdead/redirected` 收敛到 `nowunused`
  - 保留 `state->page_lsn > record->end_lsn` 的 warm-state hardening
  - 新增回归 `fb_replay_prune_future_state`

### 记录 2：`failed to replay heap update`

- 时间：
  - 2026-04-08
- 报错现象：
  - `scenario_oa_50t_50000r.users @ '2026-04-08 00:38:25.357868+00'`
    稳定报：
    - `lsn=BA/4472E390 ... failed to replay heap update`
- 原因：
  - final replay 复用 warm pass 页状态时，
    `HEAP2_PRUNE` 的 image preserve 判定过早 short-circuit
  - prune lookahead 旧逻辑没有把后续
    `PRUNE_VACUUM_CLEANUP` 释放出来的 slot
    反向折算回 future constraints
  - 结果是更早 prune image 被误判为 future insert 不可满足，
    final replay 错保留了 pre-cleanup 页状态
- 修复方式：
  - 为 `FB_WAL_RECORD_HEAP2_PRUNE` 的 future compose
    增加“后续 `nowunused` 会释放 future insert slot”语义
  - 增加 `state->page_lsn > record->end_lsn` 的额外 hardening
  - 新增独立回归 `fb_replay_prune_future_state`

### 记录 3：`ERROR: pfree called with invalid pointer`

- 时间：
  - 2026-04-08
- 报错现象：
  - release gate `users` case 在 flashback 期间触发：
    - `ERROR: pfree called with invalid pointer`
- 原因：
  - `fb_summary_segment_lookup_payload_locators_cached()`
    在处理 multi-match payload locator 时，
    第二轮收集先写 `matched_slices[slice_index]` /
    `matched_counts[slice_index]`，后确认 `slice_count`
  - 当最后一个正样本后还存在 `slice_count = 0` 的匹配 relation 时，
    会发生越界写，最终在 `pfree(positions)` 崩溃
- 修复方式：
  - 改成先取临时 `slice/slice_count`，仅在 `slice_count > 0` 时再写入 scratch arrays
  - 删除旧的 `positions` k-way merge，改成安全收集后统一
    `sort + deduplicate`
  - 新增回归 `fb_summary_payload_locator_merge`

---

## 2026-04-09

### 记录 1：`fb_wal_fill_xact_statuses_serial()` cleanup `SIGSEGV`

- 时间：
  - 2026-04-09
- 报错现象：
  - `scenario_oa_50t_50000r.documents @ '2026-04-08 13:33:00.700288+00'`
    在验证 `summary_xid_fallback=110` 修复时，
    执行
    `select count(*) from pg_flashback(...)`
    会把 backend 打崩：
    - `client backend ... was terminated by signal 11: Segmentation fault`
- 原因：
  - `fb_wal_fill_xact_statuses_serial()` 新增了 cleanup 阶段对
    `state.assigned_xids` 的释放
  - 但旧代码直到函数后半段、进入 raw WAL fallback 前才
    `MemSet(&state, 0, sizeof(state))`
  - 当 unresolved xid 已在 summary 阶段被完全解出时，
    函数会提前 `goto cleanup`
  - 此时 `state.assigned_xids` 仍是未初始化栈垃圾，cleanup 中
    `hash_destroy(state.assigned_xids)` 直接触发 `SIGSEGV`
- 修复方式：
  - 将 `FbWalSerialXactVisitorState state` 的零初始化前移到函数入口
  - 复跑同一 live query，确认：
    - 不再崩溃
    - `3/9 55% xact-status` 收敛到约 `301 ms`
    - 查询已完整返回 `count = 1950007`

---

## 2026-04-06

### 记录 4：重复 replay 导致 `failed to replay heap insert`

- 时间：
  - 2026-04-06
- 报错现象：
  - `fb_custom_scan.sql` 的 cursor `MOVE`
    和 `fb_replay_debug('fb_custom_scan_target', target_ts)`
    都会报：
    - `WARNING: will not overwrite a used ItemId`
    - `ERROR: failed to replay heap insert`
- 原因：
  - final replay 的 record cursor 仍可能收到“相邻两条 start LSN 完全相同”的重复 WAL record
  - 同一条 `heap insert` 被 replay 第二次后，目标 offset 已被占用
- 修复方式：
  - `fb_replay_run_pass()` 对 exact duplicate `record.lsn` 做硬去重
  - 新增/复核 `fb_custom_scan` 定向回归

---

## 2026-04-03

### 记录 5：`missing FPI for block 9990`

- 时间：
  - 2026-04-03
- 报错现象：
  - `scenario_oa_12t_50000r.roles @ '2026-04-02 22:10:13'`
    在 `4/9 replay discover` 报：
    - `missing FPI for block 9990`
- 原因：
  - query-side payload window 把跨 segment 的记录头裁掉了
  - `89/EFFFF9D8 DELETE ... blk=9990 FPW` 实际存在于 WAL 中，
    但 payload read/emit 边界过窄，导致这条锚点记录没进入索引
- 修复方式：
  - payload read window 为首窗补读前一连续 segment
  - payload emit gate 从“只看 `ReadRecPtr >= emit_start`”
    改成允许 `EndRecPtr = emit_start` 的紧邻前驱记录进入索引

### 记录 6：旧 summary sidecar 导致 `missing FPI` / `failed to replay heap insert`

- 时间：
  - 2026-04-03
- 报错现象：
  - `documents @ '2026-04-01 23:15:13'`
    曾先表现为：
    - `missing FPI for block 216136`
  - 继续追查后真实 first blocker 推进为：
    - `84/AE079278 / blk=216125 / failed to replay heap insert`
- 原因：
  - 查询侧仍在信任旧版本 `meta/summary` sidecar
  - 旧 summary 只覆盖了 segment 前半段 relation spans，
    导致同块更早的必要 WAL 记录没有进入 payload record index
  - 最终表现为 discover/final replay 页基线落后，后续 insert 无法落到正确页态
- 修复方式：
  - 前滚 `FB_SUMMARY_VERSION`，强制旧 summary 失效
  - 查询侧自动回退为安全 WAL 扫描或等待新版本 summary 重建
  - 定向复核该 live case，确认不再停在旧 blocker

---

## 2026-04-01

### 记录 7：`failed to replay heap insert` 的根因收敛

- 时间：
  - 2026-04-01
- 报错现象：
  - `scenario_oa_12t_50000r.documents @ '2026-04-01 08:10:13'`
    曾报：
    - `ERROR: failed to replay heap insert`
- 原因：
  - 根因不在 `fb_replay_heap_insert()` 原语本身
  - 真实问题是 payload window 非单调/重叠，导致 query-side record 输入序列页状态落后
  - 页状态落后后，后续 insert 看起来像“重复插入到已占用 offset”
- 修复方式：
  - 对 summary/payload candidate windows 做全局 merge-normalize
  - 增加 `emit_floor`，避免重叠窗口把同一条 WAL record 反复送入 replay
  - 将现场从 replay 错误收敛为真实 preflight 内存门限或正确结果

---

## 2026-03-29

### 记录 8：`heap update record missing new tuple data`

- 时间：
  - 2026-03-29
- 报错现象：
  - `PG18` 的 `same-block HOT_UPDATE + FPW-only`
    场景稳定误报：
    - `heap update record missing new tuple data`
- 原因：
  - `fb_replay_heap_update()` 没有正确尊重 `apply_image`
  - 对“new page 镜像已经是最终页态”的 update/hot update，
    仍错误要求 tuple payload
- 修复方式：
  - `fb_replay_heap_update()` 改为尊重 `apply_image`
  - 对 image-only 的 new page 不再强制要求 new tuple data
  - 新增回归 `fb_flashback_hot_update_fpw`

### 记录 9：跳过 aborted heap record 导致页槽位漂移

- 时间：
  - 2026-03-29
- 报错现象：
  - 页级重放后续会出现：
    - `invalid new offnum during heap update redo`
    - 或同类 slot/offset 漂移错误
- 原因：
  - `fb_replay` 主循环错误跳过了 `record.aborted`
  - PostgreSQL 虽然逻辑上回滚事务，但物理页上的 tuple/slot 变化仍参与后续 WAL 语义
- 修复方式：
  - `fb_replay` 主循环不再跳过 `record.aborted`
  - 保证页级物理状态演进与真实 WAL 保持一致

---

## 2026-03-23

### 记录 10：`heap insert record missing block data`

- 时间：
  - 2026-03-23
- 报错现象：
  - TOAST full 深测初版在 flashback 过程中报：
    - `heap insert record missing block data`
- 原因：
  - replay 旧逻辑把 insert / multi-insert 统一当成“必须二次 `PageAddItem()`”
  - 但 PG18 中部分带可应用 image 的 insert / multi-insert 记录，
    页镜像本身已经是最终状态
- 修复方式：
  - `fb_replay_heap_insert()` / `fb_replay_heap2_multi_insert()`
    接入 `apply_image`
  - 遇到 image-only redo 时，直接从恢复后的页读取 tuple，
    不再二次插入

### 记录 11：锁/清理类记录在缺页基线时过早打断 flashback

- 时间：
  - 2026-03-23
- 报错现象：
  - TOAST full 跑到中途时，
    `heap_lock` / `HEAP2_LOCK_UPDATED` / `HEAP2_PRUNE_*`
    在缺页基线时会直接把整个 flashback 打断
- 原因：
  - 旧 replay 对纯锁/清理类记录采用“无页基线即报错”的过保守策略
  - 这类记录在没有 image/init 且当前查询内没有对应 block state 时，
    实际可以安全跳过
- 修复方式：
  - 对纯锁/清理类记录补最小 replay
  - 当记录自身没有 image/init，且当前查询内也不存在对应 block state 时，
    允许安全跳过，而不是直接报错
