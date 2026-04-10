# 总体架构 / Architecture Overview

[中文](#中文) | [English](#english)

## 中文

本文描述当前仓库主线代码的真实结构，不覆盖已经删除的“结果表物化 / 并行结果表写入”旧模型，也不把尚未完成的设计稿当成现状。

当前用户入口为：

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

对纯 `count(*) FROM pg_flashback(...)`，planner 还会额外在
`UPPERREL_GROUP_AGG` 下推成单节点：

```text
Custom Scan (FbCountAggScan)
  -> COUNT_ONLY wal index build
  -> current row count baseline
  -> direct bigint result
```

这条路径不再走 PostgreSQL 默认 `Aggregate -> FbApplyScan`，也不再发射整批历史行给上层聚合器。

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
- `pg_flashback.spill_mode`
- `pg_flashback.parallel_workers`
- `pg_flashback.export_parallel_workers`
- `pg_flashback.show_progress`

当前默认值补充：

- `pg_flashback.memory_limit = 1GB`
- `pg_flashback.parallel_workers = 8`
- `pg_flashback.show_progress = on`

### `fb_runtime`

文件：

- `src/fb_runtime.c`
- `include/fb_runtime.h`

职责：

- 初始化和校验扩展私有目录
- 提供 `runtime/recovered_wal/meta` 路径给各模块落盘使用
- `recovered_wal/meta` 不再做通用自动删除或保留期淘汰
- 查询结束时对 `runtime/` 触发一次安全 sweep：
  - 仅清理 owner backend 已失活的 `fbspill-*` / `toast-retired-*`
  - 对活跃 owner 或状态不确定的产物保持保留

当前固定目录：

- `DataDir/pg_flashback/runtime`
- `DataDir/pg_flashback/recovered_wal`
- `DataDir/pg_flashback/meta`
- `DataDir/pg_flashback/meta/summary`

### `fb_summary`

文件：

- `src/fb_summary.c`
- `include/fb_summary.h`

职责：

- 定义 segment 通用 summary / segment index 文件格式
- 在 `meta/summary` 中读写 summary
- 提供 locator / relid bloom probe
- 提供 relation spans / xid outcomes / relation-scoped touched xids /
  紧凑 unsafe facts / block anchors / payload locators 的读取接口
- 当前 summary v8 已额外提供：
  - relation-scoped block anchors：
    - 每段内按 relation / block 记录可用 `FPI/INIT_PAGE` 锚点
    - 服务 `replay discover/warm` 的 missing-block anchor 解析
  - relation-scoped payload locators：
    - 每段内按 relation 记录 payload 需要 decode 的精确 record start LSN
    - 服务 query-side payload locator-first 物化，减少 span 内无关 WAL decode
- 当前正在把 payload locator 进一步收敛为：
  - summary build 期写成 relation-scoped stable locator slice
  - query cache 期直接复用 public locator slice
  - payload plan 期按 segment 去重，不再由碎片 span/base windows 重复驱动同一 segment lookup
  - payload visit 期按 case 分流：
    - 高密度 locator case 允许把 locator 归并为 covered segment runs 顺扫 WAL
    - 发射期仍必须按精确 `record_start_lsn` 命中过滤，不能把 run 内其他 relation record 混入 payload
    - 稀疏 locator case 保留 direct read，避免 decode 范围被批量 run 放大
- 提供查询期 backend-local summary section cache，避免同一查询重复读取相同 summary 文件
- 供 query prefilter 和后台预建服务共用
- 当前 `3/9 build record index` 继续收敛方向已固定为：
  - `summary-span`
    - 由 query-side per-segment `palloc + copy` 迁移到 cache 期 stable public slice
    - query 侧只做 segment 去重、裁窗与 merge，不再反复复制 relation spans
  - `xact-status`
    - 保留 summary-first
    - metadata 期额外产出 query-local `xact_summary_log`
    - query 期先消费 `xact_summary_log`，仅对未解出的 xid 再读 summary
    - 在 unresolved xid 上优先做精确补洞
    - 当 relation span 内仍未命中时，允许继续按全局 resolved segments 做
      `summary exact lookup`，避免少量 unresolved xid 直接退回真实 WAL
    - 只有精确补洞仍无法回答时，才允许继续回退到原始 WAL

### `fb_wal` / record materializer

文件：

- `src/fb_wal.c`
- `include/fb_wal.h`

职责补充：

- `fb_wal_build_record_index()` 仍负责顺扫构建 `RecordRef` 索引
- `FbWalRecordCursor` 负责从 spool 中恢复记录并向 replay 发射
- 当前已拍板把 locator-only / deferred payload 的按需回填，
  收敛到 WAL 层内部统一的 reusable record materializer：
  - 复用 `XLogReaderState`
  - 复用当前打开 segment
  - 区分顺扫窗口与稀疏按 LSN 物化的 open hint
  - 服务 `4/9 replay discover`、anchor fallback、warm/final replay、
    deferred payload materialize
- 该层优化属于通用 WAL 物化内核，不依赖执行器 fast path

### `fb_summary_service`

文件：

- `src/fb_summary_service.c`
- `include/fb_summary_service.h`

职责：

- 当前代码仍保留 preload/shared-memory 版 summary 服务与 SQL 视图实现
- 已接受的新主线是将该服务外移为库外 daemon `pg_flashback-summaryd`
- 迁移完成后的扩展内职责收口为：
  - 优先读取 daemon 发布的 `state.json` / `debug.json`
  - daemon 状态缺失或 stale 且 preload 服务存在时，回退读取 shmem 视图数据
  - 提供 `pg_flashback_summary_progress`
  - 提供 `pg_flashback_summary_service_debug`
  - 写入并直接读取查询侧最近一次 summary 使用/降级观测 hint

迁移中的 daemon 目标职责：

- 周期扫描 archive / `pg_wal` / `recovered_wal`
- 承担 hot / cold backlog 调度
- 构建 `meta/summary`
- 对 `meta/summary` 做源 WAL 探活与自动清理
- 发布 `meta/summaryd/state.json` / `debug.json`
- 当前实现中，daemon 已直接读取 `PGDATA` / `archive_dest` / `pg_wal`
  并独立完成 build/cleanup，不再要求数据库连接或 `--conninfo`
- 后续阶段只剩 standalone core 的继续收敛与性能打磨

当前交付面补充：

- daemon 不再依赖 systemd/service 注册作为默认承载形式
- 用户当前通过仓库脚本 `scripts/pg_flashback_summary.sh`
  手动管理 daemon 生命周期：
  - `start`
  - `stop`
  - `status`
  - `run-once`
- `b_pg_flashback.sh` 负责：
  - build / install
  - extension setup / remove
  - 写 summaryd config
  - 调用脚本 runner 启停 daemon

当前进度视图口径补充：

- external daemon state 场景下，`pg_flashback_summary_progress`
  会锚定已发布 snapshot 边界，避免 `stable_newest_segno` /
  `missing_segments` 随实时 tail 候选来回跳动
- preload/shmem 场景继续按 session-local 候选集统计，
  避免后台全局 snapshot 污染本地测试和 gap fixture 语义

该迁移的核心目标是：

- 去除 `shared_preload_libraries` 依赖
- 不要求 PostgreSQL 重启
- 保持完整 summary 用户体验不变

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

### `tests/release_gate`

文件：

- `tests/release_gate/bin/*.sh`
- `tests/release_gate/sql/*.sql`
- `tests/release_gate/config/*.json`
- `tests/release_gate/golden/*.json`
- `tests/release_gate/templates/*`

职责：

- 提供独立于 `installcheck` 与 `tests/deep/` 的发布前阻断式验证框架
- 在空实例口径下重建 `alldb`
- 检查并清理按 PG 主版本分隔的归档目录：
  - `/walstorage/14waldata`
  - `/walstorage/15waldata`
  - `/walstorage/16waldata`
  - `/walstorage/17waldata`
  - `/walstorage/18waldata`
- 驱动 `/root/alldbsimulator` 进行建数、DML 压测与大表扩容
- 通过总入口脚本提供阶段级编排与续跑控制：
  - `--list-stages`
  - `--from <stage>`
  - `--to <stage>`
  - `--only <stage>`
- `1h` DML 压测当前固定为 schema 级 `insert/update/delete` 等权重混合；
  `bulk 10k` / `mixed dml` 由后续定向快照阶段单独构造
- 捕获 truth snapshot，并统一导出标准化 CSV
- 执行 `pg_flashback(...)`、`COPY TO`、`CTAS` 发布前场景
- 读取 `PG14-18` golden baseline，并按双阈值给出性能阻断结论
- 生成单独 Markdown 报告

### `open_source` mirror

文件：

- `open_source/README.md`
- `open_source/manifest.txt`
- `scripts/sync_open_source.sh`
- `open_source/pg_flashback/`

职责：

- 保留仓库内的长期维护开源镜像目录
- 明确根仓库与公开镜像之间的边界：
  - 根仓库是研发权威源
  - `open_source/pg_flashback/` 是对白名单执行同步后的公开镜像
- 通过白名单刷新公开目录，只保留：
  - 插件源码
  - 安装 SQL
  - PGXS 基础回归资产
  - 精简公开文档
- 明确排除内部研发状态文档、实验报告、deep/release-gate 资产、日志与构建产物
- 为后续同步 GitHub 提供固定入口，不依赖手工挑文件

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
- 给查询式 flashback 的 reverse-source / spool / debug 路径提供顺序读写介质

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
- 在 metadata 扫描期顺手产出 query-local `xact_summary_log`
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

- 当 archive 无法直接提供可信 segment 时，尝试从 `pg_wal` 错配段恢复 WAL
- 将恢复结果统一落到 `recovered_wal`
- 支持并发 worker 共享 `recovered_wal` 缓存，不因同一 segment 的同时恢复而互相撞文件
- 让 resolver 在同一轮中继续消费恢复后的段

当前补充约束：

- 若 archive 已经有 mismatch `pg_wal` 文件头所对应的真实 segment，则 resolver 不应提前把该 mismatch 文件 materialize 到 `recovered_wal`
- 这类“错名但已被 archive 覆盖”的 `pg_wal` candidate 会被直接忽略，不再制造额外 `recovered_wal` 垃圾
- archive exact-hit 不再通过 `fb_ckwal` 复制到 `recovered_wal`
- `recovered_wal` 仅承载已恢复缓存与“archive 缺失/未开时，由错配 `pg_wal` 修复出的真实 segment”

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

### `fb_compat`

文件：

- `src/fb_compat.c`
- `include/fb_compat.h`

职责：

- 处理 `PG14-18` 间 locator / xlogreader / catalog 等差异

当前版本口径：

- 源码/构建目标：`PG14-18`
- 本机实际验证矩阵：`PG14-18`

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
- `xact_summary_spool_hits`

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
- query-local `xact_summary_log`
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
- 优先消费 metadata 期写下的 `xact_summary_log`
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

## English

This document describes the current production architecture of
`pg_flashback`.

Key points:

- The public entry is `pg_flashback(anyelement, text)`.
- The main pipeline is `relation gate -> WAL index -> checkpoint/FPI/block
  redo -> ForwardOp -> ReverseOpSource -> keyed/bag apply -> SRF rows`.
- `FROM pg_flashback(...)` is executed through a stack of custom scan nodes
  rather than PostgreSQL's default `FunctionScan`.
- Count-only queries can use a dedicated fast path without materializing the
  full historical row stream.
- The architecture also includes runtime directories, WAL source resolution,
  summary sidecars, replay stages, reverse-op generation, and keyed/bag apply.

In short, `pg_flashback` is a replay-driven streaming query engine rather than
a result-table materialization workflow.
