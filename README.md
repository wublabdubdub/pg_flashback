# pg_flashback

`pg_flashback` 是一个 PostgreSQL 只读历史查询扩展，用于直接查询业务表在过去某个时间点的结果集。

它面向的典型场景是：

- 排查误更新、误删除后的历史状态
- 对账、审计、取证
- 在不恢复整库、不改业务表的前提下查看历史结果

当前安装包对外提供的能力是“历史结果集查询”。扩展保持严格只读，不会自动执行 undo SQL，也不会改写业务表。

## 版本支持

截至 `2026-03-26`，当前版本支持口径如下：

| PostgreSQL | 状态 | 说明 |
| --- | --- | --- |
| `10-11` | 源码兼容目标 | 当前机器无对应本地 toolchain，待补环境复验 |
| `12-18` | 已本机编译验证 | 已完成本地编译矩阵验证 |

说明：

- 本轮验证口径是“有什么测什么”。
- 当前仓库已经去掉 `PG18` 专属构建假设，默认跟随你提供的 `PG_CONFIG`。

## 当前安装接口

安装扩展后，当前可用接口为：

- `fb_version()`
- `fb_check_relation(regclass)`
- `pg_flashback(anyelement, text)`

其中真正的客户入口是：

```sql
SELECT *
FROM pg_flashback(
  NULL::public.orders,
  '2026-03-26 10:00:00+08'
);
```

参数说明：

- 第一个参数：目标表的复合类型锚点，固定写法 `NULL::schema.table`
- 第二个参数：目标时间点，`text`

这个接口的目标是：

- 不要求手写 `AS t(...)`
- 不要求手写 `::regclass`
- 不要求手写 `::timestamptz`
- 直接返回结果集，不创建结果表

## 安装

### 1. 选择目标 PostgreSQL

使用与你目标实例一致的 `pg_config`：

```bash
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

例如：

```bash
make PG_CONFIG=/home/18pg/local/bin/pg_config
make PG_CONFIG=/home/18pg/local/bin/pg_config install
```

### 2. 在数据库中创建扩展

```sql
CREATE EXTENSION pg_flashback;
```

### 3. 为实例准备 WAL 来源

至少需要提供一个可读取的归档目录：

```sql
SET pg_flashback.archive_dest = '/path/to/archive';
```

当前 WAL 来源策略：

- 首选 `pg_flashback.archive_dest`
- `pg_flashback.archive_dir` 仅作为兼容回退配置保留
- `pg_wal` 只补 archive 尚未覆盖的 recent tail
- 若 `pg_wal` 文件名与内容错配，扩展会尝试恢复到内部 `recovered_wal/`

## 运行前提

使用 `pg_flashback` 前，请确认以下条件成立：

- `full_page_writes = on`
- 目标时间点到当前时间之间的 WAL 完整可用
- 归档保留窗口覆盖你要查询的历史时间窗
- 目标对象是普通持久化 heap 表

当前不支持的对象或行为包括：

- 临时表
- unlogged 表
- 系统 catalog
- 物化视图
- 分区父表
- 自动执行 undo SQL

## 快速开始

### 1. 校验目标表是否适合 flashback

```sql
SELECT fb_check_relation('public.orders'::regclass);
```

### 2. 查询过去时间点的结果集

```sql
SELECT *
FROM pg_flashback(
  NULL::public.orders,
  '2026-03-26 10:00:00+08'
);
```

### 3. 如需静默执行，可关闭进度输出

```sql
SET pg_flashback.show_progress = off;
```

默认情况下，`pg_flashback()` 会通过 `NOTICE` 输出阶段进度，适合 `psql` 和人工排障。

## 关键配置项

### `pg_flashback.archive_dest`

主归档目录配置。生产环境建议显式设置。

### `pg_flashback.archive_dir`

旧配置的兼容回退项。新部署优先使用 `archive_dest`。

### `pg_flashback.memory_limit_kb`

单次查询的热路径内存上限。达到上限时直接报错，不做静默降级。

### `pg_flashback.parallel_segment_scan`

控制 WAL segment 级预筛选是否并行执行。属于高级调优项，默认是否开启请结合你的实例负载评估。

### `pg_flashback.show_progress`

是否输出 `NOTICE` 进度。默认 `on`。

## 结果语义

### 有主键或稳定唯一键的表

按键精确恢复历史逻辑结果。

### 无主键表

按 `bag / multiset` 语义恢复结果集：

- 保证结果内容和重复次数正确
- 不保证同值重复行的物理身份

## 当前能力边界

当前实现主线是：

- 从 `target_ts` 前最近 checkpoint 建立锚点
- 扫描 WAL 建立目标 relation 的 `RecordRef`
- 基于 `checkpoint + FPI + block redo` 重放历史页状态
- 从页级重放提取 `ForwardOp / ReverseOp`
- 以流式方式返回历史结果集

当前安装包还没有对外安装 `fb_export_undo()`；如果你当前需要的是“历史查询”，可以直接使用本 README 的调用方式。

## 会直接报错的场景

以下情况当前设计为直接失败，不返回部分结果：

- WAL 不完整
- 目标时间点在未来
- 时间窗内检测到 DDL / rewrite / truncate / relfilenode 变化
- TOAST 历史值无法重建
- relation 类型不支持
- 超出 `pg_flashback.memory_limit_kb`

## 常见排障

### 报错 `WAL not complete`

说明目标时间窗内缺少必须的 WAL。优先检查：

- `pg_flashback.archive_dest` 是否配置正确
- 归档目录是否真的覆盖了目标时间窗
- 目标实例的 `pg_wal` recent tail 是否仍然存在

### 报错 relation unsupported

先用 `fb_check_relation(regclass)` 确认目标对象是否是普通持久化 heap 表。

### 查询很安静，但你想看进度

确认没有关闭：

```sql
SET pg_flashback.show_progress = on;
```

### 查询输出很多 `NOTICE`，影响程序日志

对程序化调用关闭进度输出：

```sql
SET pg_flashback.show_progress = off;
```

## 生产使用建议

- 先在目标表上执行一次 `fb_check_relation()`
- 为归档目录设置明确的保留策略，保证可查询时间窗
- 对宽表、TOAST 重表和长时间窗查询，先在预发环境验证内存上限和耗时
- 将 `show_progress` 用于人工排障；对应用侧请求，建议显式关闭

## 变更与验证说明

当前仓库的开发文档使用中文，代码与标识符使用英文。

当前版本已完成：

- 客户使用 README 重写
- `PG10-18` 兼容层抽取
- 本机 `PG12-18` 编译矩阵验证

当前仍待补的是：

- `PG10/11` 的环境级正式复验
