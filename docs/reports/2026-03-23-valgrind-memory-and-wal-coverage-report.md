# 2026-03-23 valgrind 内存分析与 WAL record 覆盖报告

## 背景

当前 head 已连续两次 fresh TOAST full 通过，但真实运行中 flashback 查询 backend RSS 仍可到约 `7.2GB ~ 7.4GB`。这轮目标不是直接改实现，而是先回答两个问题：

1. 当前是否存在扩展侧“真实泄漏”？
2. 基于 PG18 头文件与当前代码，哪些 WAL record 已妥善处理，哪些仍未处理或只做最小处理？

## 分析环境

- PostgreSQL：`18.0`
- `valgrind`：`3.16.0`
- suppression：`/home/18pg/postgresql-18.0/src/tools/valgrind.supp`
- 分析方式：
  - 为避免污染当前开发库，先短暂停主库，将 `/isoTest/18pgdata` 拷贝到 `/tmp/18pgdata_valgrind`
  - 主库随后立即恢复启动
  - 后续全部在副本上用 `postgres --single` 跑 `valgrind`

## 复现命令

### 1. 泄漏检查

```bash
su - 18pg -c "valgrind --tool=memcheck --leak-check=full --show-leak-kinds=all \
  --num-callers=20 --error-limit=no \
  --suppressions=/home/18pg/postgresql-18.0/src/tools/valgrind.supp \
  --log-file=/tmp/pg_flashback_valgrind/memcheck_toast.log \
  /home/18pg/local/bin/postgres --single -D /tmp/18pgdata_valgrind fb_deep_test \
  < /tmp/pg_flashback_valgrind/memcheck_toast_single.sql"
```

### 2. 小 TOAST 场景 heap-only 峰值分析

```bash
su - 18pg -c "valgrind --tool=massif --time-unit=B \
  --massif-out-file=/tmp/pg_flashback_valgrind/massif_toast_small_heaponly.out \
  /home/18pg/local/bin/postgres --single -D /tmp/18pgdata_valgrind fb_deep_test \
  < /tmp/pg_flashback_valgrind/memcheck_toast_single.sql"
```

### 3. pilot 规模整体工作集分析

```bash
su - 18pg -c "valgrind --tool=massif --time-unit=B --pages-as-heap=yes \
  --massif-out-file=/tmp/pg_flashback_valgrind/massif_toast_pilot.out \
  /home/18pg/local/bin/postgres --single -D /tmp/18pgdata_valgrind fb_deep_test \
  < /tmp/pg_flashback_valgrind/massif_toast_pilot_single.sql"
```

说明：

- `memcheck_toast_single.sql` 是单行版的小 TOAST 回归脚本，逻辑等价于 `sql/fb_toast_flashback.sql`
- `massif_toast_pilot_single.sql` 是单行版 pilot TOAST 规模脚本，用于看峰值构成，不拿来给正确性背书

### 4. 本轮内存优化后再次复跑 pilot `massif`

```bash
su - 18pg -c "valgrind --tool=massif --time-unit=B --pages-as-heap=yes \
  --massif-out-file=/tmp/pg_flashback_valgrind/massif_toast_pilot_after_memfix.out \
  /home/18pg/local/bin/postgres --single -D /tmp/18pgdata_valgrind fb_deep_test \
  < /tmp/pg_flashback_valgrind/massif_toast_pilot_single.sql"
```

说明：

- 这次复测针对刚完成的三项修改：
  - `RecordRef` payload arena
  - TOAST `retired_chunks` spill 到 `runtime/`
  - replay 到 `ForwardOp` 之间去掉重复 `heap_copytuple`
- 该次运行过程中 `valgrind` 打印过一次 `brk segment overflow in thread #1`，但进程最终以 `0` 退出，`massif` 输出可正常使用

## 泄漏结论

`memcheck` 结果：

- `definitely lost: 184 bytes in 1 blocks`
- `indirectly lost: 752 bytes in 22 blocks`
- `still reachable: 2,861,834 bytes in 466 blocks`
- `ERROR SUMMARY: 1 errors from 1 contexts`

唯一 `definitely lost` / `indirectly lost` 栈：

- `save_ps_display_args()`
- `main()`

这是一条 PostgreSQL 启动期路径，不在 `fb_*` 代码中。此次 `memcheck` 没有给出任何扩展侧 `fb_*` 调用栈上的 `definitely lost` / `indirectly lost`。

因此本轮可以得出的结论是：

- 当前测试路径**没有证据表明存在扩展侧真实泄漏**
- 当前更大的问题是：
  - MemoryContext 生命周期内的“still reachable”
  - 以及查询过程中峰值工作集过大

## 内存使用结论

### 1. 小 TOAST 场景：heap-only `massif`

- 峰值约 `4.48MB`
- 这是把动态 loader / shared memory 等噪声尽量拿掉后，更接近扩展自身 `palloc/repalloc` 的视角

在峰值附近，扩展侧可见热点主要是：

- `fb_replay_get_block()` / `fb_replay_ensure_block_ready()`：
  - 约 `519KB`
  - 对应 `BlockReplayStore` page state
- `heap_copytuple() <- fb_copy_page_tuple() <- fb_replay_heap_insert()`：
  - 约 `459KB`
  - 对应 replay 阶段复制页面 tuple 成为 row image
- `fb_tuple_identity() <- fb_row_image_set_identities()`：
  - 约 `96KB + 65KB`
  - 对应 row/key identity 文本构造
- `fb_toast_rewrite_tuple()` / `fb_toast_reconstruct_datum()`：
  - 约 `48KB + 48KB`
  - 对应 finalize 阶段把 external datum 改写成历史可见 inline datum
- `fb_append_selected_segment()` / `fb_wal_prepare_scan_context()`：
  - 约 `69KB`
  - 对应 segment 选择与数组扩容

这说明在小场景里，扩展自身最大的几类堆内存并不是“泄漏”，而是：

- block state
- tuple copy
- identity string
- TOAST rewrite 临时对象

### 2. pilot 规模：`pages-as-heap` `massif`

- 峰值约 `798.6MB`
- 这条运行在单机副本 + `massif` 环境里后段触发了：
  - `ERROR: failed to replay heap update`
- 因此它不能作为“功能通过”结论，但在报错前已经给出了足够清晰的热点排序

把 PostgreSQL 自身 shared memory / 动态加载噪声扣掉后，扩展侧最显著的热点是：

- `fb_copy_bytes() <- fb_fill_record_block_ref() <- fb_copy_heap_record_ref()`：
  - 约 `191,991,808B`
  - 这是当前最重的一类分配
  - 说明 WAL 索引阶段对 `main_data` / `block data` 的复制是最大热点
- `fb_record_block_copy_image() <- fb_copy_xlog_fpi_record_ref()`：
  - 约 `33,570,816B`
  - 说明 FPI image 的整页复制也很重
- `heap_copytuple() <- fb_copy_page_tuple()`：
  - 约 `159,461,376B`
  - 主要来自：
    - `fb_replay_heap_delete()`
    - `fb_replay_heap_insert()`
- `fb_toast_store_put_tuple()` / `fb_toast_store_sync_page()` / `fb_toast_store_remove_tuple()`：
  - 合计约 `100MB` 量级
  - 说明 TOAST live/retired chunk store 是第二梯队大户
- `fb_replay_get_block()`：
  - 约 `74,448,896B`
  - 对应 `BlockReplayStore`
- `fb_index_append_record()`：
  - 约 `27,267,072B`
  - 对应 `RecordRef` 数组扩容本身

结合此前真实 full 运行的 `7.2GB ~ 7.4GB` RSS，可以把当前内存问题收敛为：

1. WAL 索引阶段复制过多
2. replay 阶段 tuple copy 与 page state 驻留过多
3. TOAST store 没有进入统一硬上限与收敛机制

### 3. pilot 规模复测：内存修改后的对比结论

旧样本：

- 输出：`/tmp/pg_flashback_valgrind/massif_toast_pilot.out`
- 峰值：约 `837.4MB`
- 该次运行后段停在 `failed to replay heap update`

新样本：

- 输出：`/tmp/pg_flashback_valgrind/massif_toast_pilot_after_memfix.out`
- 峰值：约 `1.769GB`
- 本次运行已完整走到结果阶段并以 `0` 退出

可以明确下来的事实有两点：

1. 本轮修改已经命中真实热路径。
   新样本的主热点已变成 `fb_payload_arena_alloc() <- fb_copy_bytes()`，而不再是旧版逐块 `palloc` 路径，说明 arena 确实生效。
2. 端到端峰值并没有下降。
   就这次 pilot `massif` 而言，峰值反而更高；更合理的解释不是“arena 让内存变差”，而是旧样本在更早阶段失败，导致它没有走到后面那些继续累积工作集的阶段。

因此，这次复测能证明：

- 代码路径已经切换成功
- 但单靠本轮的 `1/3/4` 三项修改，还不足以把 pilot 级端到端峰值压下来

这也意味着后续真正该打的大头仍然是：

- `ForwardOp / ReverseOp / apply` 工作集
- TOAST live chunk store
- 更进一步的 query-scope spool / spill / eviction

### 4. 第二轮收敛后的复测结论

本轮继续做了三项直接针对 peak snapshot 的修改：

- `ForwardOp / ReverseOp / apply` 工作集接入统一 memory limit
- keyed 模式不再保留无用的 `row_identity`，bag 模式不再保留无用的 `key_identity`
- apply 载入当前表和 reverse stream 时，尽量复用已有 tuple / identity，不再额外 `heap_copytuple()` / `pstrdup()`

对应复测命令：

```bash
su - 18pg -c "valgrind --tool=massif --time-unit=B --pages-as-heap=yes \
  --massif-out-file=/tmp/pg_flashback_valgrind/massif_toast_pilot_after_applyfix.out \
  /home/18pg/local/bin/postgres --single -D /tmp/18pgdata_valgrind fb_deep_test \
  < /tmp/pg_flashback_valgrind/massif_toast_pilot_single.sql"
```

结果：

- 输出：`/tmp/pg_flashback_valgrind/massif_toast_pilot_after_applyfix.out`
- 峰值：约 `1.231GB`
- 相比上一轮 `massif_toast_pilot_after_memfix.out` 的约 `1.769GB`，下降约 `538MB`
- 本次同样完整走到 `count = 4000` 并以 `0` 退出

热点变化：

- `fb_row_image_set_identities()` 已不再出现在 peak snapshot 的主要热点中
- `fb_keyed_upsert_row()` / `fb_apply_keyed_mode()` 的额外 copy 也已退出主要热点
- 当前头部热点继续收敛到：
  - `fb_payload_arena_alloc() <- fb_copy_bytes()`
  - `fb_replay_get_block()` / `BlockReplayStore`
  - `fb_toast_store_put_tuple()` / `sync_page()`
  - replay 阶段剩余的 `heap_copytuple()`

这说明第二轮修改不仅把 memory limit 补到了之前漏掉的工作集，还确实消掉了上一轮 `massif` 里最明显的两块“无效持有”：

- keyed 路径上无意义的 `row_identity`
- apply 阶段重复 tuple copy

## 优化方案

按收益排序，建议先做：

### 1. 降低 `RecordRef` 复制成本

优先级：最高

当前最大热点是 `fb_copy_bytes()`。应优先改掉以下全量复制：

- `record.main_data`
- `block_ref.data`
- `block_ref.image`

可选方向：

- 将 `RecordRef` 从“自带 payload”改为“轻量引用 + 按需解码”
- 为大时间窗引入 query-scope spool：
  - 扫描阶段只记 offset/length/segment
  - replay 时按需读取或批量预取
- 对 FPI 采用单独 arena / mmap spool，而不是每条记录直接 `palloc(BLCKSZ)`

### 2. 给 `ForwardOp/ReverseOp` 和 apply 工作集上硬上限

优先级：最高

当前 `pg_flashback.memory_limit_kb` 只覆盖到：

- `RecordRef`
- FPI image
- block data
- main data
- `BlockReplayStore`

尚未覆盖：

- `heap_copytuple()` 产生的 row image
- `ForwardOpStream`
- `ReverseOpStream`
- apply 侧 keyed/bag 工作集
- identity string

应先加分项统计：

- `forward_tuple_bytes`
- `reverse_tuple_bytes`
- `identity_bytes`
- `apply_working_set_bytes`

### 3. 收敛 TOAST store

优先级：高

`FbToastStore` 当前是 hash + chunk copy，且 live/retired 都常驻当前 query context。建议：

- 把 live/retired chunk store 接入统一 memory accounting
- 以 `toast valueid` 为粒度做分代/淘汰
- 对 retired chunks 增加“仅保留仍可能被窗口内 old_row 引用的 valueid”策略
- 大窗口下允许 chunk spill 到临时文件

### 4. 减少 replay 阶段 tuple copy

优先级：高

当前 `fb_copy_page_tuple()` + `heap_copytuple()` 在 pilot 规模上已到 `~159MB`。建议：

- 在 `ForwardOp` 生成阶段只保留最小 row image 表达
  - 能延迟 flatten / rewrite 的就不要提前做
- 对 keyed 模式评估“流式 reverse apply”
  - 避免同时持有完整 forward + reverse + apply 三份数据
- 对 bag 模式评估只在最终输出前做 tuple materialize

### 5. 缩短对象生命周期

优先级：中

当前 discover pass 已有独立 `discover_ctx`，但 final 阶段对象仍大量堆在 query 生命周期里。建议：

- 将 `RecordRef payload`、`ForwardOp tuple`、TOAST rewrite 临时值拆到更细粒度 context
- 在 phase 切换后显式 `MemoryContextReset/Delete`
- 对 finalized 后不再需要的原始 tuple / identity 及时释放或转短生命周期 context

### 6. 给内存分析加长期观测点

优先级：中

建议增加开发期调试输出：

- `recordref_main_data_bytes`
- `recordref_block_data_bytes`
- `recordref_fpi_bytes`
- `block_replay_store_bytes`
- `toast_live_bytes`
- `toast_retired_bytes`
- `forward_tuple_bytes`
- `identity_bytes`

否则后面很难判断优化是否真的打在大头上。

## 基于 PG18 源码的 WAL record 覆盖矩阵

### RM_HEAP_ID

已妥善处理：

- `XLOG_HEAP_INSERT`
- `XLOG_HEAP_DELETE`（不含 `XLH_DELETE_IS_SUPER`）
- `XLOG_HEAP_UPDATE`
- `XLOG_HEAP_HOT_UPDATE`
- `XLOG_HEAP_CONFIRM`
- `XLOG_HEAP_INPLACE`

当前只做最小处理，仍需增强：

- `XLOG_HEAP_LOCK`
  - 当前已进入 `RecordRef`，replay 也会更新 tuple lock bits
  - 但 `TODO.md` 已明确还要重新审视其“对后续页状态是否足够安全”

按产品边界显式拒绝：

- `XLOG_HEAP_TRUNCATE`
  - 当前作为 unsafe window 直接报错，符合项目已拍板边界

当前未接入：

- `XLOG_HEAP_DELETE` with `XLH_DELETE_IS_SUPER`
  - 当前在索引阶段直接跳过
  - 需要补文档决定：首版显式拒绝，还是后续专门支持

### RM_HEAP2_ID

已妥善处理：

- `XLOG_HEAP2_VISIBLE`
- `XLOG_HEAP2_MULTI_INSERT`
- `XLOG_HEAP2_LOCK_UPDATED`

当前只做最小处理，仍需增强：

- `XLOG_HEAP2_PRUNE_ON_ACCESS`
- `XLOG_HEAP2_PRUNE_VACUUM_SCAN`
- `XLOG_HEAP2_PRUNE_VACUUM_CLEANUP`
  - 当前合并为 `FB_WAL_RECORD_HEAP2_PRUNE`
  - replay 主要是推进 `page_lsn`
  - 这已经足以推进一些场景，但还不是“基于 PG18 语义的安全最小重放”

按产品边界显式拒绝：

- `XLOG_HEAP2_REWRITE`
  - 当前记为 unsafe rewrite，直接报错

当前未接入：

- `XLOG_HEAP2_NEW_CID`

### RM_XLOG_ID

已妥善处理：

- `XLOG_CHECKPOINT_SHUTDOWN`
- `XLOG_CHECKPOINT_ONLINE`
  - 用于 anchor 选择
- `XLOG_FPI`
- `XLOG_FPI_FOR_HINT`
  - 用于建立页基线与推进页状态，不生成 `ForwardOp`

当前未纳入本项目主路径：

- 其他 `RM_XLOG_ID` record
  - 当前既不进入 `RecordRef`，也不参与 replay
  - 现阶段属于“未证明需要”，不是当前主风险项

### 其他已接入但不做业务 replay 的 rmgr

- `RM_XACT_ID`
  - 已用于 commit / abort / `commit_lsn` 跟踪
- `RM_SMGR_ID`
  - 已用于 storage change 检测并触发 unsafe

## 当前结论

- 目前没有证据显示扩展侧存在明确“真实泄漏”
- 当前主要问题是峰值工作集过大，而不是释放遗漏
- 真正的大头按优先级依次是：
  - WAL 索引阶段 payload 复制
  - replay 阶段 tuple copy
  - TOAST store 常驻
  - `ForwardOp/ReverseOp/apply` 工作集未纳入硬上限
- WAL record 覆盖矩阵方面，当前最大的明确空洞/弱项是：
  - `XLOG_HEAP_LOCK`
  - `XLOG_HEAP2_PRUNE_*`
  - `XLOG_HEAP2_NEW_CID`
  - `XLH_DELETE_IS_SUPER` 分支的产品边界决策
