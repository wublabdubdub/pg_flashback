# TODO.md

## 当前公开接口

- [x] 公开安装面仅提供 `pg_flashback_to(regclass, text)` 与 `pg_flashback(anyelement, text)`
- [x] 查询型调用形态固定为 `NULL::schema.table`
- [x] `pg_flashback()` 不创建结果表
- [x] `pg_flashback()` 不返回结果表名
- [x] `FROM pg_flashback(...)` 不再经过 PostgreSQL `FunctionScan` / `tuplestore`

## 本轮已完成

- [x] 记录开发期 core / gdb 临时文件统一落盘到 `/isoTest/tmp`，不再使用 `/tmp`
- [x] 修复 `FbApplyScan` 直接返回 apply 内部 slot，导致 residual 历史行参与上层表达式投影/排序时崩溃的问题
- [x] 修复 keyed 单列 typed-key fast path 的单 key hash 不一致，恢复 `WHERE 主键 = const` / `IN (...)` 对更新行与已删除历史行的正确命中
- [x] 用 `scenario_oa_12t_50000r.documents` 复核 `id=10`：
  - `2026-03-29 20:00:13` 返回 `1`
  - delete commit 实际发生在 `2026-03-29 22:42:32.744216+08`
  - `2026-03-29 22:45:00` 返回 `0`
- [x] 移除 `pg_flashback()` 内部 materialized SRF，统一收口为 `ValuePerCall`
- [x] 新增本机 nightly 自动提交脚本并安装 `01:00` 用户级 `cron`
- [x] 细化 `docs/architecture/`，并按当前代码现状重写三份中文手册
- [x] 删除旧公开入口 `pg_flashback(text, text, text)`
- [x] 删除旧结果表物化逻辑
- [x] 删除 `fb_parallel` 模块
- [x] 删除 `pg_flashback.parallel_apply_workers`
- [x] 将 `pg_flashback()` 改成直接查询型 SRF
- [x] 将 keyed apply 改为变化 key 驱动
- [x] 将 bag apply 改为 delta 驱动
- [x] 迁移回归到新接口
- [x] 迁移 deep SQL 主线到新接口
- [x] 重写核心维护文档到当前实现口径

## 当前待办

### P3 / P4 补齐

- [ ] 将 `fb_decode_insert_debug` 改成基于 `ForwardOp` 的开发期调试出口
- [ ] 增加 reverse-op / row-image 开发期调试出口

### P5 查询执行

- [x] apply 主链改为变化集驱动的小内存流式执行
- [x] 历史结果集改为直接查询型 SRF 返回
- [x] 修复 `PG18` `same-block HOT_UPDATE + FPW-only` 误报 `heap update record missing new tuple data`
- [x] keyed 主键变化与原表回退场景补齐
- [x] 补齐 TOAST relation truncate / storage_change 的用户案例回归
- [x] 增强 `storage_change` 报错诊断，直接显示 main/toast、create/truncate、xid、commit time
- [x] 将 TOAST `SMGR TRUNCATE` 从直接报 `storage_change` 改为“允许 flashback，最终缺 chunk 时再报错”
- [x] 将 standalone `standby_lock` 从误判的 `storage_change` blocker 中移除
- [x] 将 `pg_flashback.memory_limit` 默认值提升到 `1048576KB`，避免已验证可恢复的 live case 被默认 64MB 预算直接拦截
- [x] 修复 replay discover 对“跨页记录缺锚点导致的新页状态落后”处理，避免后续同页记录误报 `failed to replay heap insert`
- [x] 修复 checkpoint sidecar anchor 起扫范围错误裁到 `checkpoint_lsn`，恢复 `redo_lsn..checkpoint_lsn` 区间的必要 FPI/FPW 扫描
- [x] 补齐当前 `REGRESS` 缺失的 `expected/*.out`，恢复全量 installcheck 可执行状态
- [ ] apply / replay / TOAST 交界处继续补更多宽表与极端场景验证

### P5.5 用户接口与来源模型

- [x] 安装脚本改为创建 `pg_flashback(anyelement, text)`
- [x] 删除旧的结果表名调用形态
- [x] 删除旧的 `pg_flashback(text, text, text)` 公开暴露
- [x] 将 `pg_flashback_to(regclass, text)` 收口为唯一原表闪回入口：
  - keyed + 单列稳定键
  - `AccessExclusiveLock`
  - 外键/用户触发器直接报错
  - 批量 `UPDATE / INSERT / DELETE` 原表
- [x] 删除公开安装面的 `pg_flashback_rewind(regclass, text)`
- [x] 下线“创建 `table_flashback` 新表”的公开入口与对应回归/文档口径
- [x] 迁移回归与 deep 脚本调用点
- [x] 同步 README / STATUS / TODO / PROJECT / 维护文档 / ADR
- [x] 已加载扩展后，未定义的 `pg_flashback.*` GUC 名称直接报错，避免 typo 被静默接受
- [x] 未显式设置 `pg_flashback.archive_dest` 时，从 PostgreSQL `archive_command` 自动发现本地归档目录
- [x] 支持本地 `pg_probackup archive-push -B ... --instance ...` 归档目录自动识别
- [x] `archive_library` 非空或复杂/远程 `archive_command` 时回退为要求显式配置 `pg_flashback.archive_dest`
- [x] `show_progress` 开启时为每条输出追加增量耗时，并在结束时输出总耗时
- [ ] `fb_export_undo` 对外安装与实现决策
- [ ] `pg_flashback_to` 的旧导出路径代码清理与剩余死代码收口
- [x] 给 `DataDir/pg_flashback/{runtime,recovered_wal,meta}` 建立统一删除机制：
  - `runtime` 清理死 backend 遗留 `fbspill-*`
  - `recovered_wal` 按保留期淘汰旧恢复段
  - `meta` 按保留期淘汰旧 sidecar

### P5.6 内存与效率

- [x] apply 不再按当前表大小线性占用内存
- [x] 用 `CustomScan` 接管 `FROM pg_flashback(...)`，消除 `FunctionScan -> ExecMakeTableFunctionResult -> pgsql_tmp`
- [x] 将 `CustomScan` 输出改为 slot-native，消除 `Datum -> ExecStoreHeapTupleDatum()` 导致的 `ExecutorState` 线性膨胀
- [x] 将单个 `FbFlashbackScan` 拆成多节点 `CustomScan` 算子树，让 `EXPLAIN ANALYZE` 可见：
  - `FbWalIndexScan`
  - `FbReplayDiscoverScan`
  - `FbReplayWarmScan`
  - `FbReplayFinalScan`
  - `FbReverseSourceScan`
  - `FbApplyScan`
- [x] 实现 keyed fast path：
  - `WHERE 主键 = const`
  - `WHERE 主键 IN (const, ...)`
  - `ORDER BY 主键/唯一键 ... LIMIT N`
  - 第一阶段只支持单列稳定主键/唯一键，其余场景自动回退
- [x] 从本机会话日志恢复 bounded spill Stage A 代码基线（`fb_spool` / `fb_spill` / `FbReverseOpSource`）
- [x] 继续从本机会话日志恢复 `2026-03-26 21:10` 后半段到 `2026-03-27` 的 spill follow-up / `fb_wal` sidecar / SRF 主链
- [x] 统一内存超限报错增加 `pg_flashback.memory_limit` 调参提示与可读单位
- [x] `pg_flashback.memory_limit` 支持用户直接传 `kB/MB/GB`
- [x] 修复 `pg_flashback.memory_limit='8GB'` 被错误拒绝的问题
- [x] 将用户侧参数名改为 `pg_flashback.memory_limit`，并把最大允许值收紧到 `32GB`
- [x] 新增 `pg_flashback.spill_mode`，并在 replay 前按 `memory_limit` 做 preflight 内存/磁盘选择
- [x] 将 flashback 主链并行控制统一收口为 `pg_flashback.parallel_workers`
  - 删除旧的 `pg_flashback.parallel_segment_scan`
  - 当前不接管 `pg_flashback.export_parallel_workers`
- [x] 将 resolver / sidecar 接到统一 flashback 并行参数
- [x] 将 WAL prefilter 接到统一 flashback 并行参数
- [x] 将 `parallel_workers=0` 改为“关闭并行但保留串行 prefilter”
- [x] 将 WAL payload / materialize 改造成不改变语义的并行阶段
  - 当前正式主路径已包含：
    - 进程级 payload worker + shared segment snapshot
    - raw spool merge + anchor rebuild
    - 连续大 payload window 细分
    - overlap read + logical emit boundary 的 correctness 修复
  - 已补自动化回归 `fb_wal_parallel_payload`
  - live case 已确认 `FbWalIndexScan` 与总时长均有明显收益
- [ ] 将 WAL metadata scan 改造成不改变语义且稳定优于串行基线的并行阶段
  - 当前 metadata 两段式并行 prototype 已做过，并补过 `fb_recordref` safe/unsafe 合同回归
  - 但 live case 上未打赢串行 metadata 基线，当前保持关闭
  - 下一轮目标是减少 worker 启停 / 额外 WAL pass 开销后再重新打开
- [ ] 将 replay / reverse-source 改造成不改变语义的并行阶段
- [ ] 将 apply 改造成对不同 SQL 形态普遍受益的通用并行阶段
  - 当前已验证过一版 keyed query-side apply prototype：
    - shared reverse-source
    - parallel table scan
    - per-worker tuple spool
    - leader merge seen-keys + residual
  - 当前已补：
    - query 路径 shareable reverse-source 接线
    - typed seen-key merge
    - `fb_apply_parallel` correctness 回归
    - worker 数安全收口与失败回退
    - 大表保护阈值，避免未成熟 apply 并行拖慢 live case
    - 串行 apply 热路径的 direct single-typed probe / bloom 风格 negative filter / CustomScan raw slot return
  - 但 live case 仍明显受 `tuple spool + leader 回读` 传输模型拖累，当前保持关闭，后续需要改成更低传输开销的方案
  - 当前第一阶段聚焦：
    - keyed + 单列 typed key
    - 继续把 `documents(id)` 这类 keyed live case 打到稳定优于上一轮串行 apply 基线
    - 优先替换 worker -> leader tuple 文件 spool 回读为基于 `shm_mq` 的流式传输
    - seen-key 先继续保留小 spool，等 tuple 传输打赢后再决定是否继续共享化
- [ ] WAL 索引 / replay 主链继续向 bounded spill 演进
- [ ] 将 live case 当前依赖的高默认内存继续回收回真正的 bounded spill / eviction 路径
- [ ] deep full 的 flashback 并行 `parallel_workers=0/N` 端到端验证继续推进
- [ ] 按恢复后的代码重新补跑 installcheck / deep / 冷缓存场景验证
  - 当前与本轮改动直接相关的 `fb_keyed_fast_path` / `fb_flashback_hot_update_fpw` / `fb_custom_scan` 已复跑
  - 当前剩余的是 `fb_keyed_fast_path.out` 末尾空白尾行格式对齐，不是结果内容差异

## 恢复记录

- [x] 已将当前可编译恢复点同步回当前工作区
- [x] 已记录稳定临时树 `/tmp/pgfb_stageA_clean`
- [x] 已将恢复记录继续同步到设计 / ADR / 架构文档

### P5.7 版本兼容与文档

- [x] 将 `README.md` 改写为正式客户使用手册
- [x] 抽取 `fb_compat` 并去掉当前 `PG18` 专属构建假设
- [x] 收敛到源码/构建目标 `PG10-18`
- [x] 跑通本机 `PG12-18` 编译矩阵
- [x] 在文档中明确 `PG10/11` 为“待补环境复验”

### deep / 正确性

- [ ] 收敛 batch B / residual `missing FPI`
- [ ] 持续补齐 TOAST full 与大时间窗验证

## 完成前检查

- [x] `make PG_CONFIG=/home/18pg/local/bin/pg_config install`
- [x] `su - 18pg -c 'PGPORT=5832 ... make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck'`
- [ ] 与本轮改动相关的 deep 验证按需补跑
