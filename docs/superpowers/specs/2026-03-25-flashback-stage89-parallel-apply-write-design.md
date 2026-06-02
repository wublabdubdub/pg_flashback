# Flashback Stage 8/9 Parallel Apply/Write Design

**日期**：2026-03-25

## 目标

在已经完成的“结果直写 `UNLOGGED heap`”基础上，继续把阶段 `8/9` 改成真正的 background worker 并行 apply/write。

首版真并行必须满足：

- 仍保留用户入口 `pg_flashback(text, text, text)`
- keyed / bag 两种 apply 语义都正确
- `psql` 侧 stage `8/9` 继续能看到百分比
- 所有并行行为都可通过显式 GUC 关闭

## 已确认硬约束

### 1. PostgreSQL `ParallelContext` 不能直接用于写结果表

PG18 的 `parallel mode` 明确只支持只读操作。`heap_delete` / `heap_update` 直接禁止 parallel mode 写入，`README.parallel` 也明确写明“all operations must be strictly read-only”。

这意味着：

- 不能把当前需求直接挂到 `CreateParallelContext()` / parallel worker 上
- 即便 `ParallelContext` 自带 DSM / error queue / 事务状态复制，也不能用于 `table_tuple_insert()`

### 2. 普通 dynamic bgworker 看不到 leader 当前事务里新建但未提交的结果表

当前 `pg_flashback()` 在用户事务内创建结果表。如果改为普通 bgworker 并行写：

- worker 会运行在独立事务里
- 它无法访问 leader 当前 SQL 语句中刚创建、但尚未提交的表

因此“leader 当前事务创建结果表，worker 直接并行写入”不可行。

## 设计决策

### 总体模式

新增显式 GUC：

- `pg_flashback.parallel_apply_workers`

语义：

- `0`：默认关闭，继续走当前串行 `UNLOGGED` 直写路径
- `> 0`：开启真并行 bgworker apply/write

这不是纯性能开关，而是“语义隔离开关”。开启后会进入新的结果表生命周期模型。

### 并行路径的结果表生命周期

并行路径不再由 leader backend 在当前事务里创建结果表，而是改为：

1. leader 启动一个独立 helper bgworker
2. helper bgworker 在自己的事务里创建并提交 `UNLOGGED` 结果表
3. leader 再启动若干 apply workers
4. apply workers 并行写这个已提交、所有 worker 都可见的结果表
5. 完成后 `pg_flashback()` 仅返回结果表名

这条路径的结果是：

- worker 真正可以并行写 heap 表
- 但结果表不再受调用方当前事务回滚控制

这是首版真并行的核心用户语义变化，必须在 README / 调试手册 / 维护文档里明确写出。

## 工作分解

### 1. Worker 类型

新增 `fb_parallel` 模块，提供两类 bgworker 入口：

- `fb_parallel_result_create_worker_main`
  - 自主事务创建并提交结果表
  - 返回 `result_relid`
- `fb_parallel_apply_worker_main`
  - 接收 leader 分发的 current tuples / primitive reverse-op work items
  - 在本地构建 keyed/bag 工作集
  - 计算本分区结果行数
  - 等待统一 emit 阶段
  - 直接写共享 `UNLOGGED` 结果表

### 2. 分区模型

按逻辑身份做稳定 hash 分区：

- keyed：按 `key_identity`
- bag：按 `row_identity`

leader 在分发阶段完成分区决策。

### 3. Work item 模型

不把 `REPLACE` 原样发给 worker，而是在 leader 端拆成 primitive items：

- `REMOVE(tuple)`
- `ADD(tuple)`

这样可正确处理主键变化或 bag 行像变化：

- keyed `REPLACE`
  - `REMOVE(new_row)`
  - `ADD(old_row)`
- bag `REPLACE`
  - `REMOVE(new_row)`
  - `ADD(old_row)`

每个 primitive item 都按自身 identity 路由到对应 worker。

### 4. 数据传输

leader 与每个 apply worker 之间使用：

- 单独的 DSM segment
- `shm_mq` 单写单读队列

leader 负责：

- 扫描当前表
- 计算 current tuple 的 identity 与目标分区
- 发送 current tuple message
- 遍历 `ReverseOpStream`
- 拆成 primitive items 后发送 op message

worker 负责：

- 从消息队列读取 message
- 重建 `HeapTuple`
- 在本地 keyed / bag 状态机里应用

## 进度模型

### stage 8

并行路径下 stage `8` 改为三段聚合：

- `0% -> 40%`：leader 扫描当前表并分发 current tuples
- `40% -> 80%`：leader 遍历并分发 reverse-op primitive items
- `80% -> 100%`：等待所有 worker 完成本地 apply 计算

stage `8` detail 增加：

- `parallel workers=<n>`

### stage 9

stage `9` 仍表示“最终结果表物化”。

worker 在进入 emit 前先上报本分区 `local_total_rows`，leader 汇总出：

- `global_total_rows`

随后 worker 并行写表，并原子累加：

- `global_emitted_rows`

leader 在等待 worker 结束期间轮询这两个计数，并继续按 `20%` 桶输出 stage `9` NOTICE。

## 错误与清理

### 成功

- `pg_flashback()` 返回结果表名
- 用户下一条 SQL 可以直接 `SELECT * FROM result_name`

### 失败

- 若 helper 已成功建表但 apply 失败，leader 必须启动 cleanup helper 删除残留结果表
- 若 worker 启动失败、消息队列断开或任一 worker 报错，主流程统一报错

### 调用方事务回滚

并行路径下的结果表由独立 worker 在自主事务内创建和填充：

- `pg_flashback()` 成功返回后，即使调用方随后回滚自己的事务，结果表也不会自动消失

这是一条显式、文档化、可关闭的 speed-first 语义。

## 非目标

- 本轮不改 `parallel_segment_scan`
- 本轮不恢复 `TEMP TABLE`
- 本轮不尝试让 leader 当前事务与 apply workers 共享同一个未提交结果表
- 本轮不做 raw relfilenode 直写

## 测试策略

至少补三类验证：

1. 正向回归
   - `parallel_apply_workers = 2`
   - keyed / bag 都能返回正确结果
   - progress NOTICE 显示 `parallel workers=2`

2. 生命周期验证
   - 并行路径结果表为 `UNLOGGED`
   - 并行路径用户结果表由 helper 创建，主会话后续命令可直接读取

3. 错误清理验证
   - worker 启动失败或 apply 失败时，不留下残留结果表

## 风险

- 自主事务结果表生命周期是显式语义变化
- bgworker + DSM + shm_mq 协议比串行路径复杂得多
- keyed / bag 状态机需要提炼成“worker 本地可复用”的无入口层依赖实现
- 大 tuple / TOAST 行像通过队列传输时，消息体大小与内存占用需要额外约束
