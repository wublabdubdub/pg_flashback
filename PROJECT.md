# PROJECT.md

## 项目目标

`pg_flashback` 是一个 PostgreSQL 扩展，提供只读历史查询与反向操作导出能力。

当前代码已安装的用户接口：

- `SELECT pg_flashback('fb1', 'public.t1', '2026-03-22 10:00:00+08');`
- `SELECT * FROM fb1;`

规划中但当前未对外安装的接口：

- `fb_export_undo(regclass, timestamptz)`

## 首版范围

- 支持普通持久化 heap 表
- 支持必要 TOAST
- 支持 `INSERT/DELETE/UPDATE`
- 支持归档 WAL 完整前提下的历史查询
- 支持导出 undo SQL / reverse op
- 严格只读，不改业务表

## 首版不支持

- 时间窗内任何 DDL / rewrite：
  - `ALTER TABLE`
  - `TRUNCATE`
  - `VACUUM FULL`
  - `CLUSTER`
  - relfilenode 重写
  - drop / recreate
- 分区父表
- 临时表
- unlogged 表
- 自动执行 undo SQL
- 不完整归档下的 best-effort 返回

## 结果语义

- 有主键/稳定唯一键的表：按键精确恢复逻辑结果。
- 无主键表：按 `bag/multiset` 语义恢复，保证结果集内容和重复次数正确，不保证同值重复行的物理身份。

## 实现主线

1. 从 WAL 中找到 `target_ts` 之前最近的 checkpoint，取其 `redo` 位置作为页级重放锚点
2. 单次顺序扫描 WAL，建立目标 relation 的 `RecordRef` 索引层：
   - 记录目标 heap/toast 相关的 heap DML record
   - 提取 block/FPI/init-page 元信息
   - 记录相关事务的 commit / abort 边界
   - 检测目标时间窗内的 truncate / rewrite / storage change
3. 基于 `RecordRef` 建立 `BlockReplayStore`
4. 依照 LSN 顺序对目标 block 做前向重放：
   - `INSERT + INIT_PAGE` 可直接初始化页
   - 其他路径依赖 FPI 或更早已重放页状态
   - 若 replay 中出现 `missing FPI`，不按 block 单独回扫；改为收集缺页基线 block 集，并共享回扫更早 `RecordRef` 查找可用 `FPI/INIT_PAGE`
   - 对于 PG18 中可能携带 main-fork block image / block data / main data 的 heap 记录，必须统一进入 `RecordRef` 与 replay 路径，不能只处理 `INSERT/DELETE/UPDATE`
5. 在重放前/后页面中提取逻辑行像，生成 `ForwardOp`
6. 由 `ForwardOp` 派生 `ReverseOp`
7. 返回 `target_ts` 时刻的历史结果，或导出 undo SQL / reverse op

## 当前代码状态（2026-03-25）

- 当前安装脚本对外只暴露：
  - `fb_version()`
  - `fb_check_relation(regclass)`
  - `pg_flashback(text, text, text)`
- 当前主链已落地：
  - `checkpoint -> RecordRef -> BlockReplayStore -> heap redo`
  - 从页级重放提取 `ForwardOp`
  - 从 `ForwardOp` 构建 `ReverseOp`
  - `keyed` 与 `bag` 两种查询执行模型
  - `pg_flashback(text, text, text)` 真实 flashback 用户入口
- 当前运行时已落地：
  - `archive_dest + pg_wal + recovered_wal` 三路来源解析
  - 内嵌 `fb_ckwal`
  - 查询级 `memory_limit_kb`
  - `parallel_segment_scan`
  - opt-in 的 `parallel_apply_workers`
  - 扩展私有目录自动初始化
- 当前 deep full 已切到 baseline 快照恢复模式，并带状态文件续跑

当前未完成：

- `fb_export_undo`（明确放到当前主线最后实现）
- batch B / residual `missing FPI` 收敛
- TOAST store 的内存上限覆盖
- 主键变更与更多正确性覆盖
- `parallel_segment_scan` 在真实 flashback 主路径上的端到端收益仍需继续验证/收敛
- 双来源解析的诊断与边界行为还可继续增强：
  - 内嵌 `fb_ckwal` 的识别/校正/复制能力继续增强
  - 更细的来源决策与调试输出
- 对 PG18 heap WAL 的补齐范围仍在推进：
  - 已补齐：`HEAP_CONFIRM`、`HEAP_INPLACE`、`HEAP2_VISIBLE`、`HEAP2_MULTI_INSERT`、`HEAP2_LOCK_UPDATED`
  - 仍需推进：`HEAP_LOCK` / `HEAP2_PRUNE_*` 从最小 no-op 向“对后续页状态安全”的最小重放升级

当前 TOAST 历史值重建已实现：

- TOAST relation record 已进入同一套 `RecordRef + BlockReplayStore + redo` 路径
- 已建立扩展内部 TOAST chunk store：
  - `live_chunks`
  - `retired_chunks`
- 主表 row image 生成时，external toast pointer 会优先读取历史 TOAST store，而不是 live TOAST relation
- 历史值会直接 inline 到重写 tuple 中，不再依赖 `INDIRECT` 指针
- 如果历史 chunk 缺失，直接报错，不回退到错误数据
- 当前已新增回归：
  - `fb_toast_flashback`
- 当前已完成规模化 TOAST 测试：
  - `tests/deep/sql/80_toast_scale.sql`
  - 结果见 `docs/reports/2026-03-23-toast-scale-report.md`
  - 最近一次复跑结果仍为：`truth_count = 4000`、`result_count = 4000`、`diff_count = 0`
 - TOAST full 深测已建立正式入口并执行：
   - 入口：`tests/deep/bin/run_toast_scale.sh --full`
   - 报告：`docs/reports/2026-03-23-toast-full-report.md`
   - 当前状态：执行已完成，但仍被 `missing FPI` / 页基线问题阻塞
   - 当前实现状态：
     - 共享的按-block 更早 FPI 回溯首版已落地
     - 原始 `toast=true` `heap_delete` 缺页基线已不再是 first blocker
     - full 深测当前 residual blocker 变成主表 `heap_delete` 的 `missing FPI`
   - 已拍板的修复路线：
     - 保留全局 checkpoint 首锚点
     - 对缺页基线 block 采用共享的按-block 更早 FPI 回溯
     - 主表与 TOAST relation 统一走同一套补锚逻辑
     - 若更早 WAL 中仍无可恢复页基线，则继续明确报错

## 当前验证状态

- 当前 `Makefile` 中登记的回归集包含：
  - `fb_smoke`
  - `fb_relation_gate`
  - `fb_relation_unsupported`
  - `fb_runtime_gate`
  - `fb_flashback_keyed`
  - `fb_flashback_bag`
  - `fb_flashback_storage_boundary`
  - `pg_flashback`
  - `fb_user_surface`
  - `fb_memory_limit`
  - `fb_toast_flashback`
  - `fb_progress`
  - `fb_parallel_apply`
- deep 侧当前已具备：
  - TOAST 规模化脚本 `tests/deep/sql/80_toast_scale.sql`
  - TOAST deep 入口 `tests/deep/bin/run_toast_scale.sh`
  - 主表 full baseline 快照恢复与续跑入口 `tests/deep/bin/run_all_deep_tests.sh --full`

## 当前用户接口边界

当前代码已实际收口为：

- 对外只保留真实 flashback 用户入口：
  - `pg_flashback(text, text, text)`
- `fb_export_undo()` 当前尚未对外安装
- `fb_flashback_materialize()` / `fb_internal_flashback()` 当前未对外安装
- 用户不需要手写 `AS t(...)`

## 当前新增的用户接口决策

当前可用入口是：

```sql
SELECT pg_flashback(
  'fb1',
  'fb_live_minute_test',
  '2026-03-23 08:09:30.676307+08'
);

SELECT * FROM fb1;
```

约束如下：

- 第一个参数：结果表名，`text`
- 第二个参数：源表名，`text`
- 第三个参数：目标时间点，`text`
- 用户不再需要手写：
  - `::regclass`
  - `::timestamptz`
- 当前结果对象正在向“速度优先”口径调整：
  - 默认路径已经去掉 `tuplestore -> TEMP TABLE` 二次物化
  - 默认结果直接落为 `UNLOGGED heap` 结果表
  - 显式设置 `pg_flashback.parallel_apply_workers > 0` 后，会切到 bgworker 并行 apply/write
  - 并行路径下结果表由独立 worker 自主事务创建，因此不再跟随调用方事务回滚
- 如果目标表名已存在：直接报错，不自动覆盖

## 运行时前置配置

- `pg_flashback.archive_dest`
  - 当前首选归档来源配置
- `pg_flashback.archive_dir`
  - 当前保留为兼容回退配置
- 当前实现会同时解析：
  - `archive_dest`
  - `pg_wal`
  - `recovered_wal`
- 内嵌缺失 WAL 恢复目录固定为 `DataDir/pg_flashback/recovered_wal/`
- 用户不再配置 `pg_flashback.ckwal_restore_dir` / `pg_flashback.ckwal_command`

- 当前代码会在扩展库加载时自动初始化扩展私有目录：
  - `DataDir/pg_flashback/runtime/`
  - `DataDir/pg_flashback/recovered_wal/`
  - `DataDir/pg_flashback/meta/`
- segment 选择规则当前实现为：
  - `archive_dest` 缺失时才从 `pg_wal` 读取
  - 两端同时存在时一律优先 `archive_dest`
  - `pg_wal` 文件名与头部/pageaddr 不一致时，视为被覆盖或错配，转入 `ckwal`
- 当前内嵌 `fb_ckwal` 已经实现：
  - 可信 segment 的内部复制/复用
  - 错配 `pg_wal` 段按页头识别实际 timeline/segno 后落盘到 `recovered_wal/`
  - 在同一轮 resolver 中回灌转换结果
- 当前明确约束为：
  - `fb_ckwal` 不负责“凭空重建”真正缺失的 segment
  - 当 `archive_dest` / `pg_wal` / `recovered_wal` 都没有可用段时，直接报 `WAL not complete`

- `full_page_writes`
  - 首版物理内核依赖 checkpoint 后首次修改产生 FPI
  - 若无法从 checkpoint 锚点为目标 block 建立安全页基线，则直接报错

- `pg_flashback.memory_limit_kb`
  - 新增查询级内存硬上限
  - 用于限制当前查询内最重的几类结构：
    - `RecordRef` 数组
    - FPI image
    - block data
    - main data
    - `BlockReplayStore` 中的 page state
  - 达到上限时直接报错，不做静默降级
  - 该上限当前是“热路径已跟踪内存”的硬限制，不等价于整个 backend 的总内存
  - 后续若引入 spool / spill，则该 GUC 仍作为“内存层”上限保留
- `pg_flashback.parallel_apply_workers`
  - 默认 `0`
  - `> 0` 时显式开启 bgworker 并行 apply/write
  - 并行路径会把结果表创建和写入移到独立 worker 事务中完成
  - 因此结果表生命周期独立于调用方事务回滚

## 外部参考

- WAL 扫描和解析阶段允许参考 `/root/pduforwm`
- 参考重点是：
  - `docs/replayWAL.md`
  - `src/pg_walgettx.c`
  - `src/decode.c`
- 但 `pg_flashback` 不直接复用该项目的工具接口、目录结构、输出格式或执行模型
- 本项目最终形态始终是 PostgreSQL 扩展，不是外部回放工具

## 当前扫描策略

- 首先做单次 WAL 顺序扫描：
  - 找到 `target_ts` 之前最近的 checkpoint
  - 检查 archive 目录内 WAL 段连续性
  - 建立目标 relation 的 `RecordRef` 索引
  - 提取事务提交边界
  - 识别目标 relation 的 unsafe window
- 后续阶段不再直接依赖 WAL 中是否自带 old tuple
- `DELETE/UPDATE` 的旧行像来源是：
  - checkpoint 锚点之后的页级重放状态
  - 在应用 redo 前从历史页面 offset 上提取 tuple
- 当前缺页基线修复策略已拍板为：
  - 先从 `target_ts` 前最近 checkpoint 启动正常 replay
  - 若某批 block 在此锚点后无法建立页基线，则收集这些 block
  - 对该 block 集共享回扫更早 `RecordRef`
  - 为每个 block 查找最近可恢复 `FPI/INIT_PAGE`
  - 用这些补锚结果预热 `BlockReplayStore` 后再执行正常 replay
  - 不为不同 block 做重复 WAL 回扫
  - 当前已完成首版实现，但 batch B / full 深测下仍需继续扩大收敛范围

## Batch B 当前处理策略

针对 deep pilot 中暴露的 batch B 问题，当前正式策略分成三层：

1. 先补齐 PG18 中所有可能携带 main-fork image / block data / main data 的 heap/heap2 记录进入 `RecordRef` 与 replay，排除“record 集漏接导致漏消费可用 image”的情况。
   - 这一层当前已完成：
     - `HEAP_CONFIRM`
     - `HEAP_INPLACE`
     - `HEAP2_VISIBLE`
     - `HEAP2_MULTI_INSERT`
     - `HEAP2_LOCK_UPDATED`
2. 在此基础上增强 `missing FPI` 诊断，把“当前 block 首条相关 record 无 image/INIT”与“本应被索引但未被索引”的情况区分开。
3. 对 PG18 的 `RM_XLOG_ID` 记录单独补齐页基线来源：
   - 将 `XLOG_FPI`
   - 将 `XLOG_FPI_FOR_HINT`
   纳入目标 relation 的 `RecordRef`
   - 仅用于建立可复用页基线与 shared backtracking
   - 不直接生成 `ForwardOp`
3. 在第 1、2 层基础上，正式进入“共享的按-block 更早 FPI 回溯”实现，而不是逐 block 单独回扫或直接放大到全局更早锚点。

当前状态：

- shared block-level backtracking 首版已落地
- 它已经把 TOAST full 的 first blocker 从 `toast=true` 的 `heap_delete` 推进到了主表 `heap_delete`
- 下一步是继续收敛 full 场景下 residual `missing FPI`

这样做的目的，是显式规避 `/root/pduforwm` 的重路径：

- 不在扫描热路径内拼 SQL
- 不在扫描阶段写库
- 不在扫描阶段做 TOAST 文件回读
- 不在拿到 target relation 之前做深度 decode
- 不做整段 WAL 二次全量扫描
