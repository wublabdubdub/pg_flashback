# STATUS.md

## 当前代码口径（2026-03-26）

- 当前版本支持目标已扩展为：
  - 源码/构建目标 `PG10-18`
  - 本机实际验证矩阵 `PG12-18`
  - `PG10/11` 由于当前机器无本地 toolchain，待补环境复验
- 当前安装脚本只对外安装：
  - `fb_version()`
  - `fb_check_relation(regclass)`
  - `pg_flashback(anyelement, text)`
- 当前用户调用形态固定为：
  - `SELECT * FROM pg_flashback(NULL::schema.table, target_ts_text);`
- 当前结果模型固定为：
  - 不创建结果表
  - 不返回结果表名
  - 默认仍是直接查询型 SRF
  - 允许在执行器允许时内部切到 materialized SRF / `tuplestore`
- 当前旧入口与旧中间层已删除：
  - `pg_flashback(text, text, text)`
  - `fb_parallel`
  - `parallel_apply_workers`
  - 公开安装面的 `fb_flashback_materialize(...)`
- 当前主链已经收敛为：
  - `checkpoint + RecordRef + block redo + ForwardOp + ReverseOpSource + streaming apply`
- 当前 apply 已切到小内存口径：
  - keyed 只跟踪变化 key
  - bag 只跟踪变化 row identity
  - 不再按当前整表大小构造 apply 工作集
- 当前 spill / sidecar 主链已进入恢复后的继续收敛状态：
  - `fb_spool`
  - `sql/fb_spill.sql`
  - `sql/fb_wal_sidecar.sql`
  - `fb_wal` recent tail inline / sidecar 诊断口径
  - `pg_flashback()` 内部 materialized SRF 发射路径
- 当前运行时与来源解析已落地：
  - `archive_dest`
  - `archive_dir` 兼容回退
  - `debug_pg_wal_dir`
  - `memory_limit_kb`
  - `parallel_segment_scan`
  - `show_progress`
  - 自动初始化 `DataDir/pg_flashback/{runtime,recovered_wal,meta}`
- 当前 `pg_flashback()` 进度显示固定为 9 段：
  - stage `9` 已改为 residual 历史行发射

## 本轮完成

- 新增仓库维护脚本 `scripts/cron_daily_update.sh`：
  - 仅在仓库存在变更时执行 `git add -A`
  - 固定提交信息 `update`
  - 推送目标固定为 `origin/main`
- 已为当前用户安装本机 `cron`：
  - 每天 `01:00` 在 `/root/pg_flashback` 执行自动提交/推送
  - 日志输出到仓库根目录 `cron_daily_update.log`
- 将 `README.md` 重写为面向客户的使用文档
- 抽取 `fb_compat` 跨版本兼容层
- 去掉 `Makefile` 中默认绑定的 `PG18` `PG_CONFIG`
- 收敛 `PG12-18` 本机编译矩阵
- 同步 `STATUS.md` / `TODO.md` / `PROJECT.md` / `docs/architecture/overview.md` 到当前版本支持口径
- 将公开入口彻底迁移到 `pg_flashback(anyelement, text)`
- 删除结果表物化与并行结果表写入代码
- 删除 `include/fb_parallel.h` / `src/fb_parallel.c`
- 删除 `pg_flashback.parallel_apply_workers` GUC
- 将 apply 主链改为流式执行：
  - 新增 `src/fb_apply.c`
  - 重写 `src/fb_apply_keyed.c`
  - 重写 `src/fb_apply_bag.c`
- 将 `pg_flashback()` 改为 value-per-call SRF
- 修复 SRF cleanup 生命周期问题
- 修复“普通 heap tuple 直接作为复合 Datum 返回”导致的崩溃问题
- 回归 SQL 全量迁移到 `NULL::schema.table` 调用形态
- deep SQL 主线迁移到直接查询口径，不再依赖结果表
- 重写当前维护文档，移除旧的结果表/并行写表描述
- 已从本机会话日志中恢复出一版可编译的 bounded spill Stage A 代码基线：
  - 新增 `fb_spool`
  - 新增 `sql/fb_spill.sql`
  - `fb_wal` / `fb_replay` / `fb_reverse_ops` / `fb_apply` 已切到 `FbReverseOpSource`
  - 该基线来自 `2026-03-26 18:22`、`2026-03-26 20:03`、`2026-03-26 21:10` 三段会话的前半段稳定 patch
- 已继续从本机会话日志恢复出 `2026-03-27` 后续 spill follow-up：
  - `fb_apply` 新增 emit / materialize 双路径
  - `fb_entry` 新增内部 materialized SRF 分支
  - `fb_wal` 新增 recent tail inline / sidecar 相关恢复代码与调试 SQL
  - `sql/fb_recordref.sql` / `sql/fb_toast_flashback.sql` / `sql/fb_wal_sidecar.sql` 已同步回恢复结果
  - 当前稳定继续恢复树为：`/tmp/pgfb_exact_probe`
- 已补回本轮会话内新增的设计 / ADR / 报告文档：
  - `docs/specs/2026-03-26-bounded-spill-design.md`
  - `docs/decisions/ADR-0006-bounded-spill-main-pipeline.md`
  - `docs/decisions/ADR-0007-toast-heavy-materialized-srf.md`
  - 若干 `docs/superpowers/specs` / `plans` 与冷缓存报告
- 已复盘用户 live 案例 `scenario_oa_12t_50000r.notices`：
  - 主表 `relfilenode = 16396047`
  - TOAST `relfilenode = 16396114`
  - 建表事务 `xid = 204237`，提交时间 `2026-03-27 22:40:37.885578+08`
  - 阻塞 flashback 的真实原因不是建表，而是 TOAST relation 在事务 `xid = 294470` 中发生 `SMGR TRUNCATE`
  - 该 TOAST truncate 提交时间为 `2026-03-27 22:51:42.387556+08`
- 已新增回归 `fb_flashback_toast_storage_boundary`，覆盖“target 后 TOAST relation 发生 truncate / storage_change 必须直接报错”的用户案例边界
- 已增强 `storage_change` 诊断信息，当前会直接暴露 relation scope / storage op / xid / commit time

## 当前验证结果

- `scripts/cron_daily_update.sh` 幂等校验：通过
- 当前用户 `crontab` 安装校验：通过
- `PG12-18` 本机编译矩阵：通过
- `make PG_CONFIG=/home/18pg/local/bin/pg_config install`：通过
- `su - 18pg -c 'PGPORT=5832 ... make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck'`：`All 12 tests passed.`
- `2026-03-27` 当前恢复工作区 `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`：通过
- `PGPORT=5832 PGUSER=18pg make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck REGRESS='fb_flashback_toast_storage_boundary'`：`All 1 tests passed.`
- `/tmp/pgfb_exact_probe` 顺序回放成功后 `make PG_CONFIG=/home/18pg/local/bin/pg_config -j4`：通过
- 当前回归覆盖：
  - `fb_smoke`
  - `fb_relation_gate`
  - `fb_relation_unsupported`
  - `fb_runtime_gate`
  - `fb_flashback_keyed`
  - `fb_flashback_bag`
  - `fb_flashback_storage_boundary`
  - `fb_flashback_toast_storage_boundary`
  - `pg_flashback`
  - `fb_user_surface`
  - `fb_memory_limit`
  - `fb_toast_flashback`
  - `fb_progress`

## 仍在进行

- `PG10/11` 环境级正式复验
- batch B / residual `missing FPI` 收敛
- `fb_export_undo`
- WAL 索引 / replay 主链继续向 bounded spill 演进
- 更多 PG18 heap WAL 与主键变化正确性补齐
- install / installcheck / deep 级别验证尚未按本轮恢复后的代码重新补跑
- 当前可继续恢复 / 对照的稳定临时树为：
  - `/tmp/pgfb_stageA_clean`
  - `/tmp/pgfb_exact_probe`

## 下一步

- 观察首次 `2026-03-28 01:00 CST` 自动提交结果，确认日志与推送行为符合预期
- 在具备环境后补齐 `PG10/11` 的正式编译/回归复验
- 继续处理 deep pilot batch B blocker
- 为 `fb_export_undo` 开始独立实现
- 继续压缩 WAL 索引 / replay 路径的高水位内存
- 补齐主键变化与更多 TOAST / 宽表场景验证
- 按当前恢复后的工作区补跑 install / installcheck / deep 验证，确认恢复结果不仅“可编译”
