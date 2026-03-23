# pg_flashback

`pg_flashback` 是一个 PostgreSQL 扩展项目，目标是提供类似 Oracle Flashback Query 的只读历史查询能力。

当前确定的实现路线：

- 先从 WAL 中找到 `target_ts` 之前最近的 checkpoint，并取其 `redo` 位置作为页级重放锚点。
- 单次顺序扫描 WAL，建立目标 relation 的 `RecordRef` 索引层。
- 对目标 block 做前向重放，`DELETE/UPDATE` 的旧行像从重放前页面提取，而不是依赖 WAL 自带旧 tuple。
- 在页级重放之上提取 `ForwardOp` / `ReverseOp`，再导出 undo SQL 或生成最终历史结果。
- 同时支持两类输出：
  - `pg_flashback(regclass, timestamptz)`：返回历史结果集
  - `fb_export_undo(regclass, timestamptz)`：导出 undo SQL / reverse op 日志

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
- `pg_flashback(regclass, timestamptz)` 最小可用结果返回

当前用户接口补充：

- `pg_flashback(regclass, timestamptz)` 仍然是底层查询函数
- 由于 PostgreSQL 对 `SETOF record` 的解析约束，直接 `SELECT * FROM pg_flashback(...)` 仍无法在运行时自动推断列定义
- 当前新增的实用接口是：
  - `fb_flashback_materialize(regclass, timestamptz, text default 'fb_flashback_result')`
  - 它会从数据字典自动生成列定义，并创建临时表 `fb_flashback_result`
  - 用户随后可直接：

```sql
SELECT fb_flashback_materialize('public.t1'::regclass, '2026-03-23 08:09:30+08');
TABLE fb_flashback_result;
```

当前已拍板的下一层用户接口是：

```sql
SELECT fb_create_flashback_table(
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
- 默认创建 `TEMP TABLE`
- 如果目标表已存在，直接报错
- 创建成功后会输出一条 WAL 诊断信息，用来判断当前时间点对应的 LSN 以及本次扫描窗口的起止 WAL 段

当前这一接口已经实现，可直接使用。

当前最终用户入口分层已经拍板：

- 主用户入口：`fb_create_flashback_table(text, text, text)`
- 中间层 helper：`fb_flashback_materialize(regclass, timestamptz, text)`
- 底层能力：`pg_flashback(regclass, timestamptz)`

也就是说：

- 普通用户默认应使用 `fb_create_flashback_table()`
- `pg_flashback()` 继续保留，但主要用于内部测试、调试和高级场景

当前仍未完成的，是代码层而不是产品决策层：

- 基于 `archive_dest + pg_wal` 的 WAL 来源增强
  - 内嵌 `fb_ckwal` 的真实恢复逻辑
  - 更细的来源决策调试信息
- 稳定的 `fb_decode_insert_debug`
- TOAST 场景
- `fb_export_undo`（已明确放到当前主线最后实现）

当前测试工作已经进入下一阶段：

- 第一阶段：功能链与基础回归，基本完成
- 第二阶段：深度生产化测试，已完成设计与计划，并已开始 pilot

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

当前全量回归已通过：

- `fb_smoke`
- `fb_relation_gate`
- `fb_relation_unsupported`
- `fb_runtime_gate`
- `fb_wal_scan`
- `fb_wal_sources`
- `fb_wal_source_policy`
- `fb_recordref`
- `fb_replay`
- `fb_flashback_keyed`
- `fb_flashback_bag`
- `fb_flashback_materialize`
- `fb_create_flashback_table`
- `fb_memory_limit`

当前已接入的最小运行时配置：

- `pg_flashback.archive_dest`
  - 当前首选的归档来源配置
  - 会与当前实例的 `pg_wal` 一起参与 segment 解析
- `pg_flashback.archive_dir`
  - 当前保留为兼容回退配置
  - 未设置 `archive_dest` 和 `archive_dir` 时，`pg_flashback()` / `fb_export_undo()` 会先报错
  - 当前已接入最小真实 WAL 扫描框架
  - 开发期可用 `fb_scan_wal_debug(regclass, timestamptz)` 查看扫描摘要
  - 后续开发期调试会优先围绕 `RecordRef` 和 `BlockReplayStore`
- 内嵌缺失 WAL 恢复目录固定为：
  - `DataDir/pg_flashback/recovered_wal/`
  - 用户不再配置任何 `ckwal` 相关参数
- `pg_flashback.debug_pg_wal_dir`
  - 开发期专用 `pg_wal` 覆盖目录
  - 主要用于回归测试和 source resolver 调试
- `pg_flashback.memory_limit_kb`
  - 查询级内存硬上限
  - 当前已覆盖 `RecordRef` / FPI / block data / main data / `BlockReplayStore`
  - 超限时直接报错，不做静默降级

注意：`pg_flashback.archive_dir = '/isoTest/18pgdata/pg_wal'` 现在只应作为兼容回退配置；当前产品方向是 `archive_dest + pg_wal` 双来源解析。

已确认的后续方向是：

- 最终需要引入 `archive_dest` 语义，而不是把用户配置绑死在 `pg_wal`
- WAL 来源解析要同时考虑：
  - 当前仍在 `pg_wal` 中的 recent segments
  - 已归档到 `archive_dest` 的历史 segments
- `pg_wal` 已被覆盖、但可由扩展内嵌 `fb_ckwal` 自动恢复的 segments
- 对最后一种情况，`/root/xman` 中的 `ckwal` 只作为实现参考，不作为运行时依赖
- segment 选择规则已经定为：
  - `archive_dest` 缺失时才从 `pg_wal` 读取
  - 两端同时存在时一律优先 `archive_dest`
  - `pg_wal` 文件名与内容错配时，不可信并转入 `ckwal`
- `CREATE EXTENSION pg_flashback` 时会自动初始化：
  - `DataDir/pg_flashback/runtime/`
  - `DataDir/pg_flashback/recovered_wal/`
  - `DataDir/pg_flashback/meta/`
- 扩展内恢复目录已内建，用户不再配置 `ckwal_restore_dir` / `ckwal_command`
- 当前开发期可用：
  - `fb_wal_source_debug()`
  - 观察 resolver 最终使用的 `archive / pg_wal / ckwal` 计数

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
