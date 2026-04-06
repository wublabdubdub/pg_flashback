# WAL 解码层

这里的“WAL 解码层”不是逻辑复制意义上的 decode，而是 `pg_flashback` 当前为了历史查询所做的结构化扫描与索引构建。

它的目标不是直接输出 SQL，也不是直接输出最终历史行，而是回答：

1. 目标时间窗需要哪些 WAL record
2. 它们是否属于目标主表或 TOAST relation
3. 它们的事务最终是 committed / aborted / unknown
4. 时间窗里是否命中了当前不支持边界
5. replay 所需的 page image / block data / main data 是否已收集

相关代码：

- `src/fb_wal.c`
- `include/fb_wal.h`
- `src/fb_ckwal.c`

## 一、当前层级边界

WAL 层当前只做三件大事：

### 1. 准备扫描上下文

`fb_wal_prepare_scan_context()`

负责：

- 解析来源目录
- 找到 `target_ts` 前最近 checkpoint
- 得到 `start_lsn / end_lsn`
- 记录 timeline、segment 范围、anchor hint 等信息

### 2. 构建 `RecordRef` 索引

`fb_wal_build_record_index()`

负责：

- 顺序扫描 WAL
- 识别目标 relation 相关 record
- 构造 `FbRecordRef`
- 记录 touched xid 和 xid 最终状态
- 记录 unsafe window

### 3. 为 replay 提供游标式读取

`FbWalRecordCursor`

负责：

- 从 spool log / tail log 中按顺序取回 `FbRecordRef`
- 支持 forward/backward 方向读取
- 对 locator-only / deferred payload 使用通用 WAL record materializer
  做按需回填

## 二、当前不是做什么

当前 WAL 层明确不做：

- 生成 SQL 文本
- 直接输出 undo SQL
- 直接输出用户结果集
- 在扫描主循环里构造完整 heap tuple 结果
- 在扫描阶段访问业务表
- 把中间结果落库到业务表或系统表

这部分是当前实现刻意和 `/root/pduforwm` 拉开的边界。

## 三、核心数据结构

### `FbWalScanContext`

定义在 `include/fb_wal.h`。

它描述“一次扫描”的环境。

重要字段：

- 时间窗：`target_ts`、`query_now_ts`
- segment 范围：`first_segment`、`last_segment`、`total_segments`
- LSN 边界：`start_lsn`、`original_start_lsn`、`end_lsn`
- anchor：`anchor_checkpoint_lsn`、`anchor_redo_lsn`、`anchor_time`
- 来源统计：`pg_wal_segment_count`、`archive_segment_count`、`ckwal_segment_count`
- sidecar / hint：`anchor_hint_found`、`checkpoint_sidecar_entries`
- 并行参数与命中窗口：`parallel_workers`、`segment_prefilter_used`

当前口径补充：

- `pg_flashback.parallel_workers` 是 flashback 主链总 worker 参数
- `0` 表示串行，`> 0` 表示允许并行且参数值即 worker 上限
- WAL 层内允许并行的阶段都受这个统一 worker 参数控制
- 旧的 `pg_flashback.parallel_segment_scan` 已进入删除路径，不再作为长期参数口径

### `FbRecordRef`

它是当前 WAL 层最重要的输出单位。

包含：

- `kind`
- `lsn`
- `end_lsn`
- `xid`
- `info`
- `init_page`
- `committed_before_target`
- `committed_after_target`
- `aborted`
- `commit_ts`
- `commit_lsn`
- `blocks[]`
- `main_data`

换句话说，`FbRecordRef` 不是原始 XLogRecord，而是为 replay 裁剪后的目标子集。

### `FbRecordBlockRef`

描述单条 WAL record 中 block 级引用。

包含：

- locator
- forknum
- blkno
- 是否主表 / TOAST 表
- 是否有 image
- `apply_image`
- 是否有 block data

### `FbWalRecordIndex`

它是整轮时间窗扫描的汇总结果。

除了持有 record log，还保存：

- anchor 信息
- unsafe reason / scope / xid / commit_ts / lsn
- 各类 target DML 计数
- memory limit / tracked bytes
- xid status hash
- tail inline 状态

## 四、当前扫描流程

### 第一步：来源解析

扫描开始前，先解析 WAL 来源。

当前 resolver 会综合：

- `archive_dest`
- 兼容回退 `archive_dir`
- PostgreSQL `archive_command` 自动发现结果
- `pg_wal`
- `recovered_wal`

对应代码：

- `src/fb_guc.c`
- `src/fb_wal.c`
- `src/fb_ckwal.c`

### 第二步：选择连续可扫描 segment

目标不是“把目录里所有文件都扫一遍”，而是拼出：

- timeline 正确
- segno 连续
- 能覆盖 `start_lsn..end_lsn`

的候选段集合。

### 第三步：顺序扫描 record

当前仍以 `XLogReader` 顺序读 record 为主。

当前正式主路径已拆成两段：

- metadata phase
  - 当前方向已改为 summary-first：
    - 优先从 summary relation spans / touched xids / xid outcomes / unsafe facts 收敛 touched / xid / unsafe
    - 仅对 summary 缺失、损坏或覆盖不足的 uncovered window 回退 WAL 扫描
    - 原有 metadata/xact WAL 路径继续保留，承接 meta 未及时生成场景
  - checkpoint / anchor 继续复用独立 checkpoint sidecar
- payload phase
  - 在 anchor 已确定后，再按 payload window 物化 `RecordRef`

这一步会：

- 识别事务边界
- 识别目标 relation 相关 heap/xlog 记录
- 识别 storage change / truncate / rewrite 等 blocker
- 必要时抓 payload

### 第四步：构造 record logs

当前索引层不是简单只保存在内存数组里，而是配合 `fb_spool` 使用 append-only log。

这也是为什么 `FbWalRecordIndex` 中会有：

- `record_log`
- `record_tail_log`

当前补充口径：

- 当 `summary payload locator-first` 完整覆盖 payload 且满足并行条件时，
  `record_log` 可只落 `locator-only payload stub`
- 但 replay / deferred payload 不允许再以“每条 stub 新建一个 WAL reader”
  的方式回填真实记录
- 当前已拍板引入可复用的 WAL record materializer，供：
  - replay discover / warm / final
  - anchor fallback
  - deferred payload materialize
  共用

## 五、当前识别的 record 范围

定义在 `include/fb_wal.h` 的 `FbWalRecordKind`：

- `HEAP_INSERT`
- `HEAP_DELETE`
- `HEAP_UPDATE`
- `HEAP_HOT_UPDATE`
- `HEAP_CONFIRM`
- `HEAP_LOCK`
- `HEAP_INPLACE`
- `HEAP2_PRUNE`
- `HEAP2_VISIBLE`
- `HEAP2_MULTI_INSERT`
- `HEAP2_LOCK_UPDATED`
- `XLOG_FPI`
- `XLOG_FPI_FOR_HINT`

这说明当前 WAL 层已经不再只是识别三种 DML，而是把影响页基线与后续 row image 的关键 record 一并纳入。

## 六、事务状态为什么必须在这层解决

因为 replay 不能只看“这条 record 长什么样”，还必须知道它对历史结果是否有逻辑影响。

当前 `FbRecordRef` 上的三个状态位：

- `committed_before_target`
- `committed_after_target`
- `aborted`

分别对应：

1. 对目标历史结果已经生效
2. 对当前表已生效，但对目标历史结果需要反向撤销
3. 逻辑上不应进入最终历史结果，但物理页形态可能仍要参与 replay

第三点非常重要，当前代码已经明确修复“aborted record 物理上不能直接跳过”的问题。

## 七、unsafe window 检测

当前 WAL 层会在扫描阶段尽早识别不支持边界。

相关分类：

- `FB_WAL_UNSAFE_TRUNCATE`
- `FB_WAL_UNSAFE_REWRITE`
- `FB_WAL_UNSAFE_STORAGE_CHANGE`

作用范围：

- 主表
- TOAST relation

但当前实现对 TOAST `SMGR TRUNCATE` 做了专门放宽：

- 不在 relation 级直接 fatal
- 留到 TOAST 重建阶段再决定是否失败

## 八、为什么要做 prefilter / tail inline / sidecar

这些都不是独立产品功能，而是为真实主路径服务。

### 1. segment prefilter

目的：

- 在正式顺序扫描前，先做保守命中判断
- 避免明显无关段进入主扫描

开关：

- `pg_flashback.parallel_workers`

### 2. recent tail inline

目的：

- 让 `pg_wal` recent tail 进入同一 resolver / scan 流程
- 不再单独装配一条 recent-only 小路径

### 3. sidecar

目的：

- 保存段级/锚点相关辅助信息
- 减少重复扫描和重复找 checkpoint 的代价

当前这些功能都已经进代码，但收益还在继续收敛，不应在文档里写成“已完全闭环”。

## 九、当前与 `/root/pduforwm` 的关系

允许参考：

- 顺序扫描思路
- 事务提交提取思路
- 行像拼接思路

但当前实现明确规避这些慢路径：

1. 在扫描主循环里拼 SQL
2. 扫描阶段落库中间结果
3. 先全量 decode、后按 relation 过滤
4. 热路径里做 TOAST 文件回读
5. 热路径里做大量事务分组与上下文查找

换句话说，`pg_flashback` 的 WAL 层是“为 replay 服务的结构化事实提取层”，不是一个 SQL 文本生成器。

## 十、当前代码现状与早期基线的差异

早期文档常把 WAL 层描述成：

- 单目录扫描
- PG18-only
- 只做最轻量 pass

这些描述现在都不够准确。

当前更准确的说法是：

- 构建目标支持口径已扩展到 `PG10-18`
- 本机实际验证矩阵是 `PG12-18`
- resolver 已经是 `archive_dest + archive_command autodiscovery + pg_wal + recovered_wal`
- 索引层已经持有面向 replay 的 payload / block ref / xid state

## 十一、这一层当前还没完成什么

1. WAL 索引自身的高水位内存仍需继续 bounded spill 化
2. 更多 heap / rmgr record 的 correctness coverage 仍在推进
3. sidecar / prefilter 在真实大 live case 下的收益还需继续量化
4. `fb_export_undo()` 还没有建立正式消费链路

## 十二、最短心智模型

当前 WAL 层的职责可以概括成一句：

```text
把“这一时间窗里和目标 relation 有关、并且足以驱动页级重放”的 WAL 事实提纯出来，交给 replay
```
