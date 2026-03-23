# PROJECT.md

## 项目目标

`pg_flashback` 是一个 PostgreSQL 扩展，提供只读历史查询与反向操作导出能力。

目标接口：

- `SELECT * FROM pg_flashback('public.t1'::regclass, '2026-03-22 10:00:00+08'::timestamptz);`
- `SELECT * FROM fb_export_undo('public.t1'::regclass, '2026-03-22 10:00:00+08'::timestamptz);`
- `SELECT fb_flashback_materialize('public.t1'::regclass, '2026-03-22 10:00:00+08'::timestamptz);`
- `TABLE fb_flashback_result;`
- `SELECT fb_create_flashback_table('fb1', 'public.t1', '2026-03-22 10:00:00+08');`
- `SELECT * FROM fb1;`

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
5. 在重放前/后页面中提取逻辑行像，生成 `ForwardOp`
6. 由 `ForwardOp` 派生 `ReverseOp`
7. 返回 `target_ts` 时刻的历史结果，或导出 undo SQL / reverse op

## 当前已实现到 P5

- `checkpoint -> RecordRef -> BlockReplayStore -> heap redo`
- 从页级重放提取 `ForwardOp`
- 从 `ForwardOp` 构建 `ReverseOp`
- `keyed` 与 `bag` 两种查询执行模型
- `pg_flashback(regclass, timestamptz)` 返回最小可用历史结果集
- `fb_flashback_materialize(regclass, timestamptz, text)` 自动从数据字典提取列定义并创建临时结果表

当前仍未完成：

- `fb_export_undo`
- 稳定的 `fb_decode_insert_debug`
- TOAST 历史值重建
- `multi_insert`
- `archive_dest + pg_wal` 的 WAL 来源自动解析

## 关于“无需 AS t(...)”的当前结论

已确认：

- `pg_flashback(regclass, timestamptz)` 当前底层仍返回 `SETOF record`
- PostgreSQL 解析器无法根据运行时的 `regclass` 参数，在 `SELECT * FROM pg_flashback(...)` 解析阶段自动推断结果列定义
- 因此“完全保持当前函数名和调用形式，同时彻底去掉 `AS t(...)`”不是单纯改 C 代码即可解决的问题

当前实际方案是：

- 保留 `pg_flashback(regclass, timestamptz)` 作为底层 SRF
- 新增 `fb_flashback_materialize(regclass, timestamptz, text)`：
  - 从 `pg_attribute` / `format_type()` 自动获取列定义
  - 动态创建临时表并填充结果
  - 用户后续直接 `TABLE fb_flashback_result` 即可，无需手写 `AS t(...)`

如果未来一定要做到：

```sql
SELECT * FROM pg_flashback('public.t1'::regclass, ts);
```

完全不带 `AS t(...)` 仍能直接展开列，那就需要重新设计用户接口，而不是继续沿用当前 `SETOF record` 形态。

## 当前新增的用户接口决策

已拍板的新入口是：

```sql
SELECT fb_create_flashback_table(
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
  - `AS t(...)`
- 结果对象为 `TEMP TABLE`
- 如果目标表名已存在：直接报错，不自动覆盖

## 运行时前置配置

- `pg_flashback.archive_dir`
  - 当前仅作为 PG18 基线阶段的开发配置
  - 当前实现把它视为单一 WAL 来源目录
  - PG18 基线实现需要：
    - `full_page_writes=on`
    - 目标时间点之前存在可用 checkpoint
    - 指定目录中的 WAL 完整

- 后续要升级为 `archive_dest + source resolver` 模型
  - 不应把用户配置永久绑定到 `pg_wal`
  - 正确产品语义应当是：
    - 主配置指向归档目标 `archive_dest`
    - 运行时同时判断需要的 WAL 当前位于：
      - `pg_wal`
      - `archive_dest`
      - 或两者都不完整，需要额外恢复
  - 当 `pg_wal` 中需要的 segment 已被覆盖时，需要补上“缺失 WAL 恢复”这一层
  - 这一层后续将参考 `/root/xman` 中的 `ckwal` 思路，但保持为参考，不直接耦合外部工具

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

这样做的目的，是显式规避 `/root/pduforwm` 的重路径：

- 不在扫描热路径内拼 SQL
- 不在扫描阶段写库
- 不在扫描阶段做 TOAST 文件回读
- 不在拿到 target relation 之前做深度 decode
- 不做整段 WAL 二次全量扫描
