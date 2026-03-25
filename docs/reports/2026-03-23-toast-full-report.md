# 2026-03-23 TOAST full 深测报告

## 目标

在 `tests/deep/` 的 full 模式下，对 TOAST 历史值重建主链进行比 pilot 更高强度的验证，覆盖：

- 更大的 TOAST 基线数据量
- 更长时间窗内的 `UPDATE / DELETE / INSERT / ROLLBACK`
- 大量 TOAST chunk 的历史读取与拼接
- 在 full 模式下验证当前 TOAST 路径与页级重放、WAL/FPI 约束之间的交互

## full 配置

- 脚本入口：`tests/deep/bin/run_toast_scale.sh --full`
- SQL：`tests/deep/sql/80_toast_scale.sql`
- 当前 full 参数：
  - `toast_row_count = 25000`
  - `toast_update_count = 12500`
  - `toast_delete_count = 2500`
  - `toast_insert_count = 5000`
  - `toast_rollback_count = 7000`
- `pg_flashback.memory_limit_kb = 1048576`

## 这轮 full 深测中新发现并已修复的问题

### 问题 1：deep full 环境把 `memory_limit_kb` 设到了无效值

现象：

- 原先 full 模式把 `FB_DEEP_MEMORY_LIMIT_KB` 设为 `4194304`
- 该值超过当前 GUC 有效上限，数据库回退到默认 `64MB`
- full TOAST 在 `RecordRef array` 上直接报内存超限

修复：

- 将 deep full 默认值下调为合法且足够大的 `1048576`
- 位置：`tests/deep/bin/common.sh`

### 问题 2：带可应用 FPI 的 `heap insert` / `heap2 multi_insert` 仍被要求必须携带 block data

现象：

- full TOAST 初版在 `heap insert record missing block data` 上失败

根因：

- 当前 replay 把 insert 类记录统一当成“必须二次 PageAddItem”
- 但 PG18 中某些带可应用 image 的 insert / multi-insert 记录，页镜像本身就是最终状态
- 这类记录不应再强制依赖 block data

修复：

- `fb_replay_heap_insert()` / `fb_replay_heap2_multi_insert()` 接入 `apply_image`
- 遇到 image-only redo 时，直接从恢复后的页读取 tuple，不再二次插入

### 问题 3：`HEAP_LOCK` / `HEAP2_LOCK_UPDATED` / `HEAP2_PRUNE_*` 的“无页基线即报错”过于保守

现象：

- full TOAST 跑到中途时，先后暴露 `heap_lock` 类记录在缺页基线时直接打断 flashback

修复：

- 对纯锁/清理类记录补了更宽松的最小 replay
- 当记录自身没有 image/init，且当前查询内也不存在对应 block state 时，允许安全跳过，而不是直接把整次 flashback 打断

## 当前 full 深测结果

当前 TOAST full 深测**已执行并形成稳定复现场景，但尚未通过**。

阶段 1 blocker：

- 错误：
  - `missing FPI for block 18750`
- 详细信息：
  - `kind=heap_delete`
  - `toast=true`
  - `has_image=false`
  - `has_data=false`
  - `init_page=false`

这说明：

- 当前 TOAST full 模式已经不再主要卡在测试环境、image-only insert 或 lock-only record
- 现在剩下的 blocker 是：
  - **TOAST relation 上真实 `heap_delete` 记录在当前锚点策略下缺少可用页基线**

## 共享的按-block 更早 FPI 回溯实施后

本轮已实现：

- discovery pass 收集本轮 replay 中全部 `missing FPI` block
- 对缺基线 block 集共享回扫更早 `RecordRef`
- 为主表与 TOAST relation 统一查找更早 `FPI/INIT_PAGE`
- 将补锚结果回灌到正式 replay

实施后重新验证结果：

1. 全量回归仍通过：`All 16 tests passed.`
2. 原始 `toast=true` 的 `heap_delete` `missing FPI` 已不再是 first blocker
3. 在 deep full 重新执行后，新的 residual blocker 曾一度表现为：
   - `kind=heap_delete`
   - `toast=false`
   - 主表 block 仍有 `missing FPI`

另外，本轮还验证了一个运行时细节：

- 当库级 `pg_flashback.memory_limit_kb` 维持 `1048576`（1GB）时，shared backtracking 会带来更多 `BlockReplayStore` 追踪块，full 可能先触发内存上限
- 将库级限额提高到合法更高值后，first blocker 才继续推进为新的主表 `missing FPI`

## 结论

- TOAST pilot：通过
- TOAST full：**已完成执行与问题收敛，但当前仍被 `missing FPI` / 页基线问题阻塞**
- 共享的按-block 更早 FPI 回溯：**已实现并已改变 blocker 形态，但尚未彻底消灭 full 模式下的 residual `missing FPI`**

因此当前项目状态应理解为：

- TOAST 历史值重建主链可用
- TOAST pilot 深测已通过
- TOAST full 深测已建立正式入口、参数化规模、报告与复现场景
- TOAST full 是否通过，取决于后续是否继续扩大 shared backtracking 的收敛范围，并补齐 `RM_XLOG_ID` 的页基线记录接入

## 追加人工 `pg_waldump` 核查

在最新一次复现中，实际报错为：

- `missing FPI for block 45907`
- `kind=heap_delete`
- `lsn=9/5CA01320`
- `rel=1663/194304/1478222`
- `toast=false`

进一步核对当前 relation 的 filenode：

- 主表 `fb_toast_scale_src` 的 relfilenode 为 `1478218`
- 其 TOAST relfilenode 为 `1478222`

因此这次 residual blocker 实际仍命中 TOAST relation。

随后使用：

- `pg_waldump -p /isoTest/18waldata -R 1663/194304/1478222 -B 45907 000000010000000900000027 00000001000000090000005C`

得到关键结果：

- `9/3DC889B8 INSERT+INIT blk 45907`
- `9/3DC89208 INSERT blk 45907`
- `9/3DC89A58 INSERT blk 45907`
- `9/3DC89E18 INSERT blk 45907`
- `9/5C9FF778 XLOG FPI_FOR_HINT blk 45907`
- `9/5CA01320 Heap DELETE blk 45907`

同时，范围内最后一个 checkpoint 为：

- `CHECKPOINT_ONLINE redo 9/27A69860`

这说明：

- 当前失败并不和“checkpoint 后无-FPI delete 前应有可恢复页基线”的判断相悖
- 在 `anchor checkpoint -> failing delete` 之间，确实存在可复用基线
- 这条基线来自 `RM_XLOG_ID` 的 `FPI_FOR_HINT`

因此当前 root cause 已收敛为：

- `RecordRef` / shared backtracking 目前没有把 `RM_XLOG_ID` 的 `FPI/FPI_FOR_HINT` 当成目标 relation block 的可复用页基线接入

## 接入 `RM_XLOG_ID` 之后的最新复测

本轮已完成：

- 将 `RM_XLOG_ID` 的 `XLOG_FPI`
- 将 `RM_XLOG_ID` 的 `XLOG_FPI_FOR_HINT`

纳入 `RecordRef` 与 replay 主链。

语义固定为：

- 只建立/推进页基线
- 不生成 `ForwardOp`

复测结果：

- 全量回归：`All 16 tests passed.`
- TOAST full：不再报 `missing FPI`
- 新 first blocker 变为：

```text
WARNING:  will not overwrite a used ItemId
ERROR:  failed to replay heap insert
```

这说明：

- `RM_XLOG_ID` 的 page-image 接入已经生效
- 当前 TOAST full 继续向前推进
- 新暴露的问题已经从“找不到页基线”推进为“insert replay 在已有页状态上重复插入”

## 继续推进后的新增正确性问题

在继续复测 full 之前，先额外补了一类小回归：

- 场景：主表有两列 TOAST，大事务中只更新其中一列，另一列保持不变
- 现象：在 `ForwardOp` finalize 阶段，未变更但仍为 live 的 external datum 可能只被历史 TOAST store 部分覆盖，从而报：
  - `failed to finalize toast-bearing forward row`
  - `missing toast chunk ... in historical toast store`

根因：

- 当前历史 TOAST store 只会纳入 replay 实际触达的 TOAST block
- 对于“未变更、仍 live”的 external datum，时间窗内可能只偶然触达了部分 chunk 所在 block
- 这会导致历史 store 中只有部分 chunk，但 tuple 上的 pointer 仍指向一个当前 live 且完整存在的 TOAST value

修复：

- 保持“历史 store 优先”的语义不变
- 当历史 store 缺 chunk 时，按 tuple pointer 中的：
  - `toastrelid`
  - `valueid`
  显式回退抓取 live TOAST datum

验证：

- 小回归 `fb_toast_flashback` 已扩展到“双 TOAST 列、只改一列”的场景
- `make installcheck REGRESS=fb_toast_flashback`：通过
- 全量回归 `All 16 tests passed.`

## 这轮最新 full blocker

在修掉上述正确性问题后，fresh full 继续推进到 flashback 阶段，但 backend 被系统 OOM killer 杀死。

关键证据：

- SQL 卡点：
  - `SELECT pg_flashback('fb_toast_scale_result', 'public.fb_toast_scale_src', ...);`
- PostgreSQL 日志：
  - `client backend ... was terminated by signal 9: Killed`
- 内核日志：
  - `Out of memory: Killed process ... (postgres)`
  - 被杀 backend `anon-rss` 约 `12GB`

这说明当前 full 的 first blocker 已经从：

- 页基线 / `missing FPI`
- TOAST finalize 缺 chunk

推进为：

- **flashback 阶段未纳入限制的内存增长**

最可疑的当前热点是：

- `ForwardOp / ReverseOp` 持有的大量 inline TOAST row image
- apply 工作集
- TOAST live / retired store

而这些也正对应当前 `TODO.md` 中尚未完成的内存上限覆盖项。

## 当前结论

- TOAST pilot：通过
- TOAST full：
  - 先前出现过 flashback 阶段 backend OOM
  - 但在当前 head 上，`bash tests/deep/bin/bootstrap_env.sh --full && bash tests/deep/bin/run_toast_scale.sh --full` 已连续两次 fresh 复跑通过
  - 两次结果均为：`truth_count = 25000`、`result_count = 25000`、`diff_count = 0`
- 小回归：
  - `fb_toast_flashback` 已补强并通过
- 全量回归：
  - `All 16 tests passed.`

补充观察：

- 两次通过的 full 运行中，flashback 查询 backend RSS 仍可到约 `7.2GB ~ 7.4GB`
- 因此当前应将状态更新为：
  - TOAST full 当前已无 blocker
  - 但 `ForwardOp/ReverseOp/apply` 与 TOAST store 的内存模型仍是明确的 residual risk，需要继续收敛
