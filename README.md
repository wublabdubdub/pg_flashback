# pg_flashback

`pg_flashback` 是一个 PostgreSQL 扩展项目，目标是提供类似 Oracle Flashback Query 的只读历史查询能力。

当前确定的实现路线：

- 先从 WAL 中找到 `target_ts` 之前最近的 checkpoint，并取其 `redo` 位置作为页级重放锚点。
- 单次顺序扫描 WAL，建立目标 relation 的 `RecordRef` 索引层。
- 对目标 block 做前向重放，`DELETE/UPDATE` 的旧行像从重放前页面提取，而不是依赖 WAL 自带旧 tuple。
- 在页级重放之上提取 `ForwardOp` / `ReverseOp`，再导出 undo SQL 或生成最终历史结果。
- 当前对外只保留真实 flashback 主入口：
  - `pg_flashback(text, text, text)`：创建速度优先的历史结果表

## 参考实现边界

WAL 扫描和解析阶段可以参考 `/root/pduforwm`，主要参考这些文件：

- `/root/pduforwm/docs/replayWAL.md`
- `/root/pduforwm/src/pg_walgettx.c`
- `/root/pduforwm/src/decode.c`

但该项目只作为参考样本：

- 不直接复用其接口设计
- 不直接复用其输出协议
- 不把 `pg_flashback` 做成外部工具式产品
- 本项目最终仍以 PostgreSQL 扩展内部的 `reverse op stream` 为主线

另外，已经明确 `/root/pduforwm` 的若干慢路径不适合直接继承：

- 扫描热路径里直接拼装 SQL 文本
- 扫描时写 `public.replayforwal` / COPY 批量落库
- relation 过滤发生得过晚
- 扫描阶段直接做 TOAST 文件/数据文件回读
- 在扫描循环里做大量 `ctx_lookup`、`toast_check`、SQL cache 和事务分组输出

`pg_flashback` 当前 PG18 扫描骨架明确规避以上路径，只做轻量 record pass。

当前新的内核前提：

- 不要求 `wal_level=logical`
- 依赖 `full_page_writes=on`
- 依赖目标时间点之前存在可用 checkpoint
- 依赖 checkpoint 之后 WAL 完整，足以为目标 block 建立页基线

当前 PG18 基线已经打通到 `P5`：

- `RecordRef + BlockReplayStore + heap redo`
- `ForwardOp / ReverseOp`
- `keyed` 历史查询
- `bag` 历史查询
- `pg_flashback()` 最小可用结果返回

当前用户接口：

```sql
SELECT pg_flashback(
  'fb1',
  'fb_live_minute_test',
  '2026-03-23 08:09:30.676307+08'
);

SELECT * FROM fb1;
```

这个接口的目标是：

- 不需要 `::regclass`
- 不需要 `::timestamptz`
- 不需要 `AS t(...)`
- 默认创建 `UNLOGGED` 结果表
- 默认仍走当前 backend 内的串行直写路径
- 显式设置 `pg_flashback.parallel_apply_workers > 0` 后，切到 bgworker 并行 apply/write 路径
- 如果目标表已存在，直接报错
当前这一接口已经实现，可直接使用。

当前仍未完成的，是代码层而不是产品决策层：

- batch B / residual `missing FPI` 收敛
- `fb_export_undo`
- TOAST store 的内存上限覆盖
- 主键变更等剩余正确性补齐
- 双来源解析的诊断与边界行为继续增强

当前 TOAST 基础主链已经打通：

- TOAST relation 记录已进入同一套页级重放路径
- 主表 external toast pointer 读取已重定向到扩展内部历史 TOAST store
- 历史 TOAST 值已改为 inline datum 形式物化进 row image
- 当前已新增并跑通回归：
  - `fb_toast_flashback`
- 当前已完成一轮规模化 TOAST 深测：
  - 脚本：`tests/deep/sql/80_toast_scale.sql`
  - 基线：`4000` 行、双 TOAST 列
  - 工作负载：`UPDATE + DELETE + INSERT + ROLLBACK`
  - 结果：`truth_count = 4000`、`result_count = 4000`、`diff_count = 0`

详细记录见：

- `docs/reports/2026-03-23-toast-scale-report.md`

当前代码侧可直接确认的验证面：

- `Makefile` 当前登记的回归集包含：
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
- deep 脚本当前包含：
  - `tests/deep/bin/run_all_deep_tests.sh`
  - `tests/deep/bin/run_toast_scale.sh`
  - `tests/deep/bin/test_full_snapshot_resume.sh`

当前 TOAST 深测有一个固定操作约束：

- 每次完成 TOAST 测试后，手动清空归档目录 `/isoTest/18waldata`

当前测试工作已经进入深度生产化阶段：

- `tests/deep/` 已具备 pilot / full 两套入口
- `full` 当前采用 baseline 快照恢复模型，不再重复导 baseline
- baseline 快照会做空间检查
- full 模式失败后会保留状态文件和 baseline 快照，支持续跑

当前 pilot 结果：

- 批次 A `keyed`：通过
- 批次 C `bag`：通过
- 批次 D `archive_dest + fake pg_wal overlap`：通过
- 批次 E `长时间窗/多轮 DML`：通过
- 批次 B `事务边界与回滚`：当前失败

当前 pilot blocker 已被定位到锚点策略：

- 现实现仍固定使用 `target_ts` 前最近 checkpoint 作为页级重放锚点
- 深测已确认存在真实场景：某些 block 在 anchor 之后首条相关记录是 `PRUNE_VACUUM_CLEANUP` / `VISIBLE` 等不带 `FPI/INIT` 的记录
- 随后才出现 `UPDATE/HOT_UPDATE`
- 这会导致当前 replay 报 `missing FPI`

详细记录见：

- `docs/reports/2026-03-23-deep-pilot-report.md`

深测设计与计划文件：

- `docs/specs/2026-03-23-deep-production-test-design.md`
- `docs/superpowers/plans/2026-03-23-deep-production-test-plan.md`

项目当前仍处于基础阶段。请优先阅读：

1. `AGENTS.md`
2. `PROJECT.md`
3. `STATUS.md`
4. `TODO.md`
5. `docs/specs/2026-03-22-pg-flashback-design.md`

如果你要从“能跑”切换到“能维护、能自己调试”，再继续读：

6. `docs/architecture/源码级维护手册.md`
7. `docs/architecture/核心入口源码导读.md`
8. `docs/architecture/调试与验证手册.md`

## 本机构建说明

当前工作区的 PG18 构建路径是：

- `PG_CONFIG=/home/18pg/local/bin/pg_config`

本机 PG18 的本地登录方式：

```bash
su - 18pg
psql postgres
```

常用命令：

```bash
make
make install
make clean && make install
su - 18pg -c 'PGPORT=5832 psql postgres -Atqc "drop database if exists contrib_regression;" && cd /walstorage/pg_flashback && rm -rf results regression.out regression.diffs && make installcheck'
```

注意：

- 如果修改了 `include/*.h` 中的结构体或函数声明，优先使用 `make clean && make install`
- 当前 PGXS 增量构建对头文件依赖不完全可靠，只跑 `make install` 可能留下旧对象

当前已接入的最小运行时配置：

- `pg_flashback.archive_dest`
  - 当前首选的归档来源配置
  - 会与当前实例的 `pg_wal` 一起参与 segment 解析
- `pg_flashback.archive_dir`
  - 当前保留为兼容回退配置
  - 未设置 `archive_dest` 和 `archive_dir` 时，历史查询入口 / `fb_export_undo()` 会先报错
- 内嵌缺失 WAL 恢复目录固定为：
  - `DataDir/pg_flashback/recovered_wal/`
  - 用户不再配置任何 `ckwal` 相关参数
- 扩展运行时私有目录当前会自动初始化为：
  - `DataDir/pg_flashback/runtime/`
  - `DataDir/pg_flashback/recovered_wal/`
  - `DataDir/pg_flashback/meta/`
  - 当前代码是在扩展库加载时完成初始化与校验
- `pg_flashback.debug_pg_wal_dir`
  - 开发期专用 `pg_wal` 覆盖目录
  - 主要用于回归测试和 source resolver 调试
- `pg_flashback.memory_limit_kb`
  - 查询级内存硬上限
  - 当前已覆盖 `RecordRef` / FPI / block data / main data / `BlockReplayStore`
  - 超限时直接报错，不做静默降级
- `pg_flashback.show_progress`
  - 默认 `on`
  - `pg_flashback()` 会通过 `NOTICE` 向 `psql` 输出阶段进度
  - 如果你只想静默执行，可先 `SET pg_flashback.show_progress = off`
- `pg_flashback.parallel_apply_workers`
  - 默认 `0`
  - `> 0` 时显式开启 bgworker 并行 apply/write
  - 并行路径下结果表会由独立 worker 自主事务创建并提交
  - 这意味着：`pg_flashback()` 成功后，即使调用方随后回滚自己的事务，结果表也仍然存在

当前 `pg_flashback()` 的 progress 固定为 `9` 段：

1. 前置校验
2. 准备 WAL 扫描上下文
3. 扫描 WAL 并建立 `RecordRef`
4. replay `DISCOVER`
5. replay `WARM`
6. replay `FINAL` 并生成 `ForwardOp`
7. 构建 `ReverseOp`
8. 应用 `ReverseOp`
9. 物化结果表

其中阶段 `3/4/5/6/7/8/9` 输出百分比；阶段 `1/2` 只在进入时输出一条 `NOTICE`。

百分比默认只在 `0/20/40/60/80/100` 这些整 `20%` 边界输出；其中第 `9` 段按“已写入结果表的行数 / 最终结果总行数”推进。

如果开启 `pg_flashback.parallel_apply_workers > 0`：

- 第 `8` 段 detail 会显示 `parallel workers=<n>`
- 第 `8` 段不再是单 backend apply，而是 leader 分发 current tuples / reverse-op primitive items，再等待 worker 本地 apply 完成
- 第 `9` 段按所有 worker 聚合后的写表进度推进

注意：`pg_flashback.archive_dir = '/isoTest/18pgdata/pg_wal'` 现在只应作为兼容回退配置；当前代码已经是 `archive_dest + pg_wal + recovered_wal` 解析模型。当前规则是：

- 两端同时存在时一律优先 `archive_dest`
- `pg_wal` 文件名与内容错配时，不可信并转入 `ckwal`
- 扩展内恢复目录已内建，用户不再配置 `ckwal_restore_dir` / `ckwal_command`
- 当前内嵌 `fb_ckwal` 已完成：
  - 运行时私有目录管理
  - 可信 segment 的内部复制与复用
  - `pg_wal` 错配段按页头读取实际 timeline/segno 后转换为标准 `recovered_wal/<actual-segname>`
  - 转换结果在同一轮 resolver 中立即回灌为 `ckwal` 候选
- 当前内嵌 `fb_ckwal` 的能力边界已经明确：
  - 只负责识别、校正、复制、回灌已有的可用 WAL 段
  - 不负责“凭空重建”真正缺失的 segment
  - 因此当 `archive_dest` / `pg_wal` / `recovered_wal` 都拿不到可用段时，直接报 `WAL not complete`

注意：

- 本机默认 `pg_config` 指向 PG12，不能直接代表本项目目标环境。
- 当前 `Makefile` 默认使用本机 PG18 路径，但后续仍可通过 `PG_CONFIG=...` 覆盖。
- 当前回归测试以 `18pg` 本地用户跑通。

## 目录约定

- `src/`：扩展源码
- `include/`：头文件
- `sql/`：扩展安装脚本与回归测试 SQL
- `expected/`：回归测试期望输出
- `docs/architecture/`：技术设计拆分文档
- `docs/decisions/`：已拍板架构决策
- `docs/roadmap/`：长期路线图
- `docs/specs/`：正式设计规格
- `docs/superpowers/plans/`：可执行实现计划
