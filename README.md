# pg_flashback

[中文](#中文) | [English](#english)

## 中文

### 1. 这个插件能做什么，为什么 PostgreSQL 自带做不到

`pg_flashback` 用来直接查询一张普通 heap 表在过去某个时间点的结果集。

它解决的是这类问题：

- 一条或一批误更新、误删除已经提交，想直接看回某个时间点表里到底是什么数据
- 想按表粒度做历史查询，而不是恢复整个实例
- 想把历史结果直接当普通 `SELECT` 结果集来用

当前 PostgreSQL 自带能力做不到这件事，原因是：

- PostgreSQL 没有类似 Oracle Flashback Query 的内建“按时间查询历史结果集”能力
- `PITR` / 备份恢复只能把整个实例恢复到某个时间点，不能直接在当前库里对单表执行历史查询
- 逻辑复制 / WAL 查看工具可以帮助解析变更，但不能把“某张表在某个时间点的完整结果集”直接作为一条 SQL 返回

`pg_flashback` 的目标，就是把这件事收口成：

```sql
SELECT *
FROM pg_flashback(
  NULL::public.orders,
  '2026-03-26 10:00:00+08'
);
```

### 2. 这个东西怎么用

先构建并安装扩展：

```bash
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

当前顶层构建也会同时安装独立可执行文件 `pg_flashback-summaryd`
到 PostgreSQL `bindir`，作为库外 summary daemon 形态的统一交付入口。

然后在数据库里创建或升级扩展：

```sql
CREATE EXTENSION pg_flashback;
```

```sql
ALTER EXTENSION pg_flashback UPDATE TO '0.2.0';
```

如果你要在“不重启 PostgreSQL”的前提下获得完整 summary 体验，
当前可用方式是启动独立 daemon。

当前实现注意点：

- `pg_flashback-summaryd` 目前仍通过 libpq 连接扩展内部 debug helper
  触发 build / cleanup
- 因此当前启动时应显式提供 `--conninfo`
- 如果不写 `--conninfo`，daemon 会退回 libpq 默认连接参数，
  很容易因为默认 `dbname/user` 不匹配而启动失败

推荐直接按完整参数启动：

```bash
pg_flashback-summaryd \
  --pgdata /path/to/pgdata \
  --archive-dest /path/to/archive \
  --conninfo "host=/tmp port=5432 dbname=postgres user=postgres connect_timeout=2" \
  --foreground
```

也可以改成配置文件方式，但 sample config 中同样应写入 `conninfo=...`。

WAL 来源建议：

- 若本机 `archive_command` 属于当前支持的本地模式，可依赖 autodiscovery
- 若归档路径复杂、远程或想避免歧义，显式设置 `pg_flashback.archive_dest`

例如：

```sql
SET pg_flashback.archive_dest = '/path/to/archive';
```

启动后先确认 summary 服务状态面已经可见：

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

建议先检查目标表是否受支持：

```sql
SELECT fb_check_relation('public.orders'::regclass);
```

查询历史结果集：

```sql
SELECT *
FROM pg_flashback(
  NULL::public.orders,
  '2026-03-26 10:00:00+08'
);
```

参数说明：

- 第一个参数是目标表的复合类型锚点，固定写法 `NULL::schema.table`
- 第二个参数是目标时间点，类型是 `text`

如果不想看到进度 `NOTICE`：

```sql
SET pg_flashback.show_progress = off;
```

常用参数：

- `pg_flashback.archive_dest`
  - 作用：指定 flashback 查询要读取的 WAL 归档目录
  - 用法：

```sql
SET pg_flashback.archive_dest = '/path/to/archive';
```

- `pg_flashback.show_progress`
  - 作用：控制是否输出进度 `NOTICE`
  - 用法：

```sql
SET pg_flashback.show_progress = off;
```

- `pg_flashback.memory_limit`
  - 作用：限制单次 flashback 查询的热路径内存上限
  - 用法：

```sql
SET pg_flashback.memory_limit = '4GB';
```

- `pg_flashback.parallel_workers`
  - 作用：控制 flashback 主链允许使用的并行 worker 上限
  - 用法：

```sql
SET pg_flashback.parallel_workers = 4;
```

### 3. 这个东西的前提条件是什么

使用前需要满足这些条件：

- PostgreSQL `14-18`
- `full_page_writes = on`
- 目标时间窗所需 WAL 必须完整可读
- 归档目录必须覆盖目标时间窗
- 目标对象必须是普通持久化 heap 表
- 除此以外无其他任何附加条件

如果使用库外 summary daemon，当前额外需要：

- `pg_flashback-summaryd` 进程能访问 `PGDATA`
- 显式提供可用的 `--conninfo`
- 可读的 archive 路径，通常与 `pg_flashback.archive_dest` 一致

当前这些场景不在支持范围内：

- 临时表
- unlogged 表
- 系统 catalog
- 物化视图
- 分区父表
- 自动执行 undo SQL
- 时间窗内发生 DDL、rewrite、truncate 或 relfilenode 变化

## English

### 1. What This Extension Does, And Why PostgreSQL Cannot Do It Natively

`pg_flashback` lets you query the result set of a regular heap table at a past
point in time.

It is meant for cases like:

- committed accidental updates or deletes where you need to see the table as it
  was at a specific time
- table-level historical queries without restoring the whole instance
- using historical results as a normal `SELECT` result set

PostgreSQL cannot do this natively today because:

- PostgreSQL has no built-in equivalent of Oracle Flashback Query
- `PITR` and backup restore can recover an entire instance to a point in time,
  but they cannot directly run a historical query for one table inside the
  current database
- logical decoding and WAL inspection can help analyze changes, but they do not
  directly return the full result set of one table at one timestamp as a single
  SQL query

`pg_flashback` reduces that workflow to:

```sql
SELECT *
FROM pg_flashback(
  NULL::public.orders,
  '2026-03-26 10:00:00+08'
);
```

### 2. How To Use It

Build and install the extension:

```bash
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

The top-level build also installs a standalone executable,
`pg_flashback-summaryd`, into PostgreSQL's `bindir` as the delivery entry point
for the external summary-daemon mode.

Create or upgrade the extension in the database:

```sql
CREATE EXTENSION pg_flashback;
```

```sql
ALTER EXTENSION pg_flashback UPDATE TO '0.2.0';
```

To get the full summary experience without restarting PostgreSQL, use the
standalone daemon.

Current implementation notes:

- `pg_flashback-summaryd` still connects through libpq and reuses
  extension-exposed debug helpers for build / cleanup
- because of that, you should currently pass `--conninfo` explicitly
- if `--conninfo` is omitted, libpq defaults may choose the wrong
  `dbname/user`, which often makes startup fail

Recommended full command:

```bash
pg_flashback-summaryd \
  --pgdata /path/to/pgdata \
  --archive-dest /path/to/archive \
  --conninfo "host=/tmp port=5432 dbname=postgres user=postgres connect_timeout=2" \
  --foreground
```

You can also use `--config`, but the config file should still include
`conninfo=...` in the current implementation.

WAL source guidance:

- if your local `archive_command` matches one of the supported local patterns,
  autodiscovery can be enough
- if the archive path is complex, remote, or you want deterministic behavior,
  set `pg_flashback.archive_dest` explicitly

For example:

```sql
SET pg_flashback.archive_dest = '/path/to/archive';
```

After startup, verify that the summary status surface is visible:

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

It is recommended to check that the target table is supported:

```sql
SELECT fb_check_relation('public.orders'::regclass);
```

Query the historical result set:

```sql
SELECT *
FROM pg_flashback(
  NULL::public.orders,
  '2026-03-26 10:00:00+08'
);
```

Arguments:

- the first argument is the relation composite type anchor, always
  `NULL::schema.table`
- the second argument is the target timestamp as `text`

If you do not want progress `NOTICE`s:

```sql
SET pg_flashback.show_progress = off;
```

Common parameters:

- `pg_flashback.archive_dest`
  - purpose: sets the WAL archive directory used by flashback queries
  - usage:

```sql
SET pg_flashback.archive_dest = '/path/to/archive';
```

- `pg_flashback.show_progress`
  - purpose: controls whether progress `NOTICE`s are emitted
  - usage:

```sql
SET pg_flashback.show_progress = off;
```

- `pg_flashback.memory_limit`
  - purpose: limits the hot-path memory budget of a flashback query
  - usage:

```sql
SET pg_flashback.memory_limit = '4GB';
```

- `pg_flashback.parallel_workers`
  - purpose: controls the maximum number of parallel workers allowed in the
    flashback main pipeline
  - usage:

```sql
SET pg_flashback.parallel_workers = 4;
```

### 3. Prerequisites

Before using it, make sure the following conditions are true:

- PostgreSQL `14-18`
- `full_page_writes = on`
- the WAL required by the target time window is complete and readable
- the archive directory covers the target time window
- the target object is a regular persistent heap table
- there are no additional prerequisites beyond the items above

If you use the external summary daemon, the current implementation also needs:

- a `pg_flashback-summaryd` process that can access `PGDATA`
- an explicit working `--conninfo`
- a readable archive path, usually the same path used by
  `pg_flashback.archive_dest`

The following are currently unsupported:

- temporary tables
- unlogged tables
- system catalogs
- materialized views
- partitioned parent tables
- automatic undo execution
- DDL, rewrite, truncate, or relfilenode changes inside the target window
