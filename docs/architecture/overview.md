# 总体架构

本文描述当前仓库主线代码的真实结构，不覆盖已经删除的“结果表物化 / 并行结果表写入”旧模型，也不把尚未完成的设计稿当成现状。

当前用户入口为：

```sql
SELECT pg_flashback_to(
  'public.t1'::regclass,
  '2026-03-28 10:00:00+08'
);
```

```sql
SELECT *
FROM pg_flashback(
  NULL::public.t1,
  '2026-03-28 10:00:00+08'
);
```

对外安装面当前只有：

- `fb_version()`
- `fb_check_relation(regclass)`
- `pg_flashback_to(regclass, text)`
- `pg_flashback(anyelement, text)`

对应安装脚本：

- `sql/pg_flashback--0.1.0.sql`

## 一、先记住主链

当前代码主链可以压成这条线：

```text
pg_flashback(anyelement, text)
  -> relation/runtime gate
  -> WAL source resolve + scan context
  -> RecordRef index
  -> checkpoint + FPI + block redo
  -> ForwardOp
  -> ReverseOpSource
  -> keyed/bag streaming apply
  -> SRF rows
```

`FROM pg_flashback(...)` 的 planner / executor 主链当前已经不是单节点黑盒，而是：

```text
Custom Scan (FbApplyScan)
  -> Custom Scan (FbReverseSourceScan)
    -> Custom Scan (FbReplayFinalScan)
      -> Custom Scan (FbReplayWarmScan)
        -> Custom Scan (FbReplayDiscoverScan)
          -> Custom Scan (FbWalIndexScan)
```

如果要再补一句当前大窗口实现特点，就是：

```text
部分结构已支持 bounded spill / spool，但 WAL 索引与 replay 主链仍在继续收敛
```

## 二、模块划分

### `fb_entry`

文件：

- `src/fb_entry.c`
- `include/fb_entry.h`

职责：

- SQL 入口
- 目标 row type 解析
- 目标时间解析
- 串起 gate、WAL、replay、reverse-op、apply
- 统一清理 SRF 生命周期
- 提供少量回归专用 debug 入口

关键函数：

- `pg_flashback(PG_FUNCTION_ARGS)`
- `fb_validate_flashback_target()`
- `fb_build_flashback_reverse_ops()`

### `fb_catalog`

文件：

- `src/fb_catalog.c`
- `include/fb_catalog.h`

职责：

- 检查 relation 是否支持
- 识别 TOAST relation
- 根据主键或稳定唯一键选择 `keyed` / `bag`

关键函数：

- `fb_catalog_load_relation_info()`

### `fb_guc`

文件：

- `src/fb_guc.c`
- `include/fb_guc.h`

职责：

- 定义 `pg_flashback.*` GUC
- 解析有效 archive 来源
- 自动识别可安全解析的 `archive_command`
- 暴露内存上限、进度开关、`pg_wal` 调试目录等配置

当前 GUC：

- `pg_flashback.archive_dest`
- `pg_flashback.archive_dir`
- `pg_flashback.debug_pg_wal_dir`
- `pg_flashback.memory_limit`
- `pg_flashback.runtime_retention`
- `pg_flashback.recovered_wal_retention`
- `pg_flashback.meta_retention`
- `pg_flashback.spill_mode`
- `pg_flashback.parallel_workers`
- `pg_flashback.export_parallel_workers`
- `pg_flashback.show_progress`

### `fb_runtime`

文件：

- `src/fb_runtime.c`
- `include/fb_runtime.h`

职责：

- 初始化和校验扩展私有目录
- 对 `runtime/recovered_wal/meta` 执行轻量目录保洁
- 为不同目录应用不同的保留/删除策略

当前固定目录：

- `DataDir/pg_flashback/runtime`
- `DataDir/pg_flashback/recovered_wal`
- `DataDir/pg_flashback/meta`

### `fb_progress`

文件：

- `src/fb_progress.c`
- `include/fb_progress.h`

职责：

- 管理查询级进度上下文
- 向客户端输出固定 9 段 `NOTICE`
- 在 `show_progress=on` 时为每条进度输出附带相对上一条 NOTICE 的增量耗时
- 在查询结束时额外输出一次总耗时 NOTICE

当前阶段定义：

1. validate
2. prepare wal
3. build record index
4. replay discover
5. replay warm
6. replay final
7. build reverse ops
8. apply
9. emit residual rows

### `fb_memory`

文件：

- `include/fb_memory.h`
- `src/fb_memory.c` 不存在，当前以内联/辅助函数方式供各模块使用

职责：

- 统一做 query 级 tracked bytes charge/release
- 对 `RecordRef`、replay block、reverse-op、apply hash 等热路径内存做硬上限约束

### `fb_spool`

文件：

- `src/fb_spool.c`
- `include/fb_spool.h`

职责：

- 提供 query 级 spill 目录
- 提供 append-only spool log / cursor / anchor
- 给 `RecordRef` payload、reverse-op runs、sidecar 调试路径提供顺序读写介质
- 给 `pg_flashback_to()` keyed 并行导出提供跨 backend 共享的 reverse-source 顺序读介质

### `fb_wal`

文件：

- `src/fb_wal.c`
- `include/fb_wal.h`

职责：

- 解析 WAL 来源
- 查找 checkpoint / redo 锚点
- 顺序扫描目标时间窗
- 建立 `FbWalRecordIndex`
- 维护 touched xid / xid status / unsafe window
- 支持 segment prefilter、tail inline、sidecar 相关逻辑

主要输出结构：

- `FbWalScanContext`
- `FbWalRecordIndex`
- `FbRecordRef`

### `fb_ckwal`

文件：

- `src/fb_ckwal.c`
- `include/fb_ckwal.h`

职责：

- 当 archive 与 `pg_wal` 都无法直接提供可信 segment 时，尝试从可见目录中复制/纠正 WAL 段
- 将恢复结果统一落到 `recovered_wal`
- 支持并发 worker 共享 `recovered_wal` 缓存，不因同一 segment 的同时恢复而互相撞文件
- 让 resolver 在同一轮中继续消费恢复后的段

### `fb_replay`

文件：

- `src/fb_replay.c`
- `include/fb_replay.h`

职责：

- 基于 `RecordRef` 做页级重放
- 管理 `BlockReplayStore`
- 执行 discover / warm / final 三阶段
- 生成 `ForwardOp`
- 直接把结果写入 `FbReverseOpSource`

这部分是当前正确性主链的核心。

### `fb_reverse_ops`

文件：

- `src/fb_reverse_ops.c`
- `include/fb_reverse_ops.h`

职责：

- 定义 `ForwardOp` / `ReverseOp` 行像结构
- 将 replay 产生的逻辑变化转成逆序可消费的 source
- 当内存超过 run limit 时做外排序 spill
- 提供 reader 供 apply 顺序消费

### `fb_apply`

文件：

- `src/fb_apply.c`
- `include/fb_apply.h`

职责：

- 协调 relation scan 与 reverse-op apply
- 管理 scan / residual 两阶段
- 向 SQL SRF 提供 datum 输出
- 向 `CustomScan` 提供 slot-native 输出
- 对 keyed relation 执行单列 stable key fast path：
  - key eq / key in 的索引点查
  - key order + limit 的有序索引扫描与早停

### `fb_custom_scan`

文件：

- `src/fb_custom_scan.c`
- `include/fb_custom_scan.h`

职责：

- 通过 planner hook 识别 `FROM pg_flashback(...)` 的 `RTE_FUNCTION`
- 输出多层 `CustomPath / CustomScan` 算子树
- 将 `wal index / replay discover / replay warm / replay final / reverse finish / apply` 拆成独立 executor 节点
- 绕开 PostgreSQL 默认 `FunctionScan -> ExecMakeTableFunctionResult() -> tuplestore`
- 根节点 `FbApplyScan` 直接把当前历史结果写入 scan slot，不再走 `Datum -> ExecStoreHeapTupleDatum()`
- 在规划期识别 keyed fast path，并把内部 `FbFastPathSpec` 传到根节点 executor / apply

### `fb_apply_keyed`

文件：

- `src/fb_apply_keyed.c`

职责：

- 对有主键/稳定唯一键的表，按 key 反向应用变化集
- 只跟踪变化 key，不再构造整表 hash
- 为 fast path 提供：
  - 缺失 key 的 replacement tuple 直取
  - residual key 列表与有序 merge 所需的 keyed state 访问接口

### `fb_apply_bag`

文件：

- `src/fb_apply_bag.c`

职责：

- 对无键表按 `bag/multiset` 语义处理
- 用 `row_identity -> delta` 描述差异

### `fb_toast`

文件：

- `src/fb_toast.c`
- `include/fb_toast.h`

职责：

- 管理历史 TOAST chunk store
- 同步 TOAST relation page image / tuple 变更
- 在主表行像构造时把 external datum 重写成 inline 值

当前 store 分两类：

- `live_chunks`
- `retired_chunks`

### `fb_export`

文件：

- `src/fb_export.c`
- `include/fb_export.h`

职责：

- `pg_flashback_to(regclass, text)` 用户入口
- 对 keyed + 单列稳定键 relation，支持不扫当前整表的原表回退：
  - 先汇总变化 key 在目标时间点的终态
  - 再以批量 `UPDATE / INSERT / DELETE` 直接改写原表

说明：

- `fb_export_undo()` 在 `src/fb_entry.c` 中已有入口骨架
- 当前仍直接报 `not implemented`
- `pg_flashback_to()` 已安装到公开 SQL 面
- 旧的 `pg_flashback_rewind()` 与持久表导出公开入口已下线

### `fb_compat`

文件：

- `src/fb_compat.c`
- `include/fb_compat.h`

职责：

- 处理 `PG10-18` 间 locator / xlogreader / catalog 等差异

当前版本口径：

- 源码/构建目标：`PG10-18`
- 本机实际验证矩阵：`PG12-18`

## 三、关键数据结构

### `FbRelationInfo`

定义：

- `include/fb_common.h`

包含：

- `relid`
- `toast_relid`
- `locator`
- `key_index_oid`
- `toast_locator`
- `mode`
- `mode_name`
- `key_natts`
- `key_attnums`

它是入口层与后续所有模块共享的 relation 元信息。

### `FbWalScanContext`

定义：

- `include/fb_wal.h`

作用：

- 保存一次 WAL 时间窗扫描的上下文

重要字段：

- `target_ts`
- `query_now_ts`
- `timeline_id`
- `start_lsn`
- `end_lsn`
- `anchor_checkpoint_lsn`
- `anchor_redo_lsn`
- `resolved_segments`
- `using_archive_dest`
- `ckwal_invoked`
- `parallel_workers`

### `FbRecordRef`

定义：

- `include/fb_wal.h`

作用：

- 描述一条目标 relation 相关 WAL 记录的结构化引用

包含：

- record kind
- lsn / end_lsn
- xid
- committed_before/after_target
- aborted
- block refs
- main_data / block_data

### `FbWalRecordIndex`

定义：

- `include/fb_wal.h`

作用：

- 描述本次 flashback 需要的目标 WAL 子集

除 `record_count` 外，还保存：

- unsafe reason/scope
- 各类 target DML 计数
- xid status hash
- tail inline 状态
- spool log 指针

### `FbReplayResult`

定义：

- `include/fb_replay.h`

作用：

- 汇总 replay 阶段结果与资源使用

### `FbForwardOp` / `FbReverseOp`

定义：

- `include/fb_reverse_ops.h`

作用：

- 把“页级变化”提升成“逻辑行变化”

当前字段比早期设计更收敛，只保留：

- `type`
- `xid`
- `commit_ts`
- `commit_lsn`
- `record_lsn`
- `old_row`
- `new_row`

### `FbReverseOpSource`

定义：

- `src/fb_reverse_ops.c`
- `include/fb_reverse_ops.h`

作用：

- 作为 replay 与 apply 之间的统一边界
- 既能纯内存，也能 spill 为外排序 runs

## 四、一次查询如何跑完

### 1. 入口准备

`pg_flashback()` 在 `src/fb_entry.c` 中完成：

- 解析 `NULL::schema.table`
- 解析 `target_ts`
- 建立 progress context
- 注册 cleanup callback

### 2. gate

入口层做三类校验：

- 目标时间不能在未来
- archive 来源必须可解析
- relation 必须受支持

### 3. WAL 准备与索引

`fb_wal_prepare_scan_context()`：

- 解析来源目录
- 找 checkpoint / redo 锚点
- 确定时间窗范围

`fb_wal_build_record_index()`：

- 顺序扫描 WAL
- 抽取目标 relation 相关记录
- 同步事务状态
- 标记 unsafe window

### 4. replay + ForwardOp

`fb_replay_build_reverse_source()` 会：

- 创建 replay store
- 对目标 block 做 discover / warm / final
- 在 final 阶段从页前后像构造逻辑变化
- 将变化作为 reverse-op append 到 `FbReverseOpSource`

虽然函数名叫 `build_reverse_source`，但内部仍先经过 ForwardOp 语义层。

### 5. apply

`fb_apply_begin()` 打开当前表扫描：

- `keyed` 走 `fb_keyed_apply_begin()`
- `bag` 走 `fb_bag_apply_begin()`

执行顺序：

1. 扫当前表
2. 命中变化集的行做逆向替换/抵消
3. 未命中的当前行直接输出
4. 扫描结束后进入 residual 阶段
5. 补发“历史存在、当前已不存在”的行

### 6. SRF / CustomScan 发射

`pg_flashback()` 当前统一只走 `SFRM_ValuePerCall`。

直接调用 `pg_flashback()` 时，入口仍是 `ValuePerCall` SRF。

`FROM pg_flashback(...)` 场景下，当前会先通过 `pg_flashback` 的 planner support function 提前加载模块，再由 `fb_custom_scan` 将默认 `FunctionScan` 替换为多层 `CustomScan` 算子树。

因此 `pg_flashback()` 的默认语义仍是直接查询结果集，不创建结果表，也不再经过 PostgreSQL 默认 table-function `tuplestore` materialize。

另外，根节点 `FbApplyScan` 当前会直接以 slot-native 方式接收 `fb_apply` 的当前行结果，不再先构造复合 `Datum` 再反解回 slot；这样 `ExecutorState` 不会再随着已输出历史行线性膨胀。

## 五、当前内存边界

### 已经收敛的部分

1. apply 不再按当前整表大小占内存
2. reverse-op 已有 `FbReverseOpSource + fb_spool` spill 边界
3. TOAST retired chunk 可落 spill 文件

### 仍在继续推进的部分

1. `FbWalRecordIndex` 的 payload / `RecordRef`
2. `BlockReplayStore`
3. replay 期间 page image / block data / main data 的高水位

所以当前 `memory_limit` 虽然已经是统一约束，但“大窗口完全 bounded spill”还没有全链条完成。

## 六、当前实现和旧文档/旧模型的边界

以下内容现在已经不是主链：

- 结果表物化后再查询
- 返回结果表名
- `pg_flashback(text, text, text)`
- `fb_parallel`
- `parallel_apply_workers`
- 公开安装面的 `fb_flashback_materialize(...)`

如果文档里还出现这些词，默认应认为是历史内容，不能拿来指导当前代码。

## 七、当前未完成项

从架构角度看，当前仍明显未完成的主项有：

- `fb_export_undo()` 真实实现
- WAL 索引 / replay 主链进一步 bounded spill 化
- batch B / residual `missing FPI` 收敛
- 主键变更等剩余正确性补齐
- 更多 PG18 heap WAL 与宽表/TOAST 极端场景验证

## 八、建议阅读顺序

如果你是第一次接手当前代码，建议按这个顺序：

1. `AGENTS.md`
2. `STATUS.md`
3. `TODO.md`
4. `PROJECT.md`
5. `docs/specs/2026-03-22-pg-flashback-design.md`
6. 本文
7. `docs/architecture/核心入口源码导读.md`
8. `docs/architecture/wal-source-resolution.md`
9. `docs/architecture/reverse-op-stream.md`
10. `docs/architecture/源码级维护手册.md`
11. `docs/architecture/调试与验证手册.md`
