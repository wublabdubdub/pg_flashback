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
  - 不走 `tuplestore`
  - 直接以 SRF 逐行返回结果集
- 当前旧入口与旧中间层已删除：
  - `pg_flashback(text, text, text)`
  - `fb_parallel`
  - `parallel_apply_workers`
  - 公开安装面的 `fb_flashback_materialize(...)`
- 当前主链已经收敛为：
  - `checkpoint + RecordRef + block redo + ForwardOp + ReverseOp + streaming apply`
- 当前 apply 已切到小内存口径：
  - keyed 只跟踪变化 key
  - bag 只跟踪变化 row identity
  - 不再按当前整表大小构造 apply 工作集
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

## 当前验证结果

- `PG12-18` 本机编译矩阵：通过
- `make PG_CONFIG=/home/18pg/local/bin/pg_config install`：通过
- `su - 18pg -c 'PGPORT=5832 ... make PG_CONFIG=/home/18pg/local/bin/pg_config installcheck'`：`All 12 tests passed.`
- 当前回归覆盖：
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

## 仍在进行

- `PG10/11` 环境级正式复验
- batch B / residual `missing FPI` 收敛
- `fb_export_undo`
- WAL 索引 / replay 主链继续向 bounded spill 演进
- 更多 PG18 heap WAL 与主键变化正确性补齐

## 下一步

- 在具备环境后补齐 `PG10/11` 的正式编译/回归复验
- 继续处理 deep pilot batch B blocker
- 为 `fb_export_undo` 开始独立实现
- 继续压缩 WAL 索引 / replay 路径的高水位内存
- 补齐主键变化与更多 TOAST / 宽表场景验证
