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

#### 一键安装

在仓库根目录直接执行：

```bash
scripts/b_pg_flashback.sh
```

脚本当前强制要求以非 root OS 用户执行；root 运行会直接报错退出，且当前 OS 用户必须就是 `PGDATA` owner。

脚本会交互式提示输入：

- `pg_config` 路径，默认取 `which pg_config`
- `PGDATA` 路径，默认取环境变量 `PGDATA`
- 数据库名，默认取环境变量 `PGDATABASE`，否则 `postgres`
- 数据库用户名，默认取环境变量 `PGUSER`
- 数据库密码，默认取环境变量 `PGPASSWORD`（提示中隐藏）
- 数据库端口，默认取环境变量 `PGPORT`，否则 `5432`

交互顺序固定为：

- `pg_config`
- `PGDATA`
- `dbname`
- `dbuser`
- `db-password`
- `db-port`

setup 完成后，脚本会自动完成：

- 若已存在 `PGDATA/pg_flashback`，先只做受限安全清理：
  - 清理 `runtime/`
  - 清理 `meta/summaryd` 下的 stale `state/debug/lock`
  - 清理 `meta/summary` 下的临时 `.tmp.*`
  - 保留 `recovered_wal/` 与正式 summary/meta 文件
- build / install
- 若检测到旧环境仍把 `pg_flashback` 放在
  `shared_preload_libraries`：
  - 自动把它移出 `shared_preload_libraries`
  - 自动重启一次 PostgreSQL，卸载旧预加载库映像
- `CREATE EXTENSION` 或 `ALTER EXTENSION UPDATE`
- 写入 `pg_flashback-summaryd` 的 config
- 通过 `scripts/pg_flashback_summary.sh` 启动 shell watchdog，由其保活 `pg_flashback-summaryd`
- 安装当前数据库 OS 用户的 cron 保活项，持续执行 `scripts/pg_flashback_summary.sh start`
- 若未显式传 `--archive-dest`，
  默认优先自动识别 PostgreSQL `archive_command`
  的本地归档目录；
  识别不了时才回退到 `PGDATA/pg_wal`
- 对目标数据库执行
  `ALTER DATABASE ... SET pg_flashback.archive_dest = ...`

安装后，summary runner 的手工入口固定为：

```bash
scripts/pg_flashback_summary.sh start
scripts/pg_flashback_summary.sh stop
scripts/pg_flashback_summary.sh status
```

注意：

- shell runner 和 cron 都直接依赖当前仓库里的
  `scripts/pg_flashback_summary.sh`，安装后不要删除这份 checkout
- runner 固定读取 `~/.config/pg_flashback/pg_flashback-summaryd.conf`
  并固定管理：
  - `~/.config/pg_flashback/pg_flashback-summaryd.watchdog.pid`
  - `~/.config/pg_flashback/pg_flashback-summaryd.pid`
  - `~/.config/pg_flashback/pg_flashback-summaryd.log`
- cron 只负责保证 watchdog 存在；`1s` 级 child 自恢复由 watchdog 承担
- 终端输出当前会按阶段分隔显示，方便区分：
  `Preflight` / `Safe Cleanup` / `Build / Install` /
  `Database Changes` / `Runner Setup` / `Cron Keepalive`
- 交互输入会在可判断范围内立即校验：
  `pg_config` 会校验“存在且可执行”，`PGDATA` 会校验“目录存在”
- 如果目标环境已经完成同一套初始化，重复执行 setup
  会返回 `already_initialized` 并直接退出，不会重复改动
- `--remove` 不会自动删除 `PGDATA/pg_flashback`
  这个数据目录；脚本会在结束时明确打印保留路径，
  由用户手工决定是否执行删除
- `--remove` 当前会额外清理同一 `PGDATA`
  下的 legacy `pg_flashback-summaryd*.service/.conf`
  与已安装的 `pg_flashback-summaryd` 二进制

如需一键移除，可执行：

```bash
scripts/b_pg_flashback.sh --remove
```

#### 用户自定义手动安装

这一段就是把一键安装拆出来让用户自己执行。下面命令默认都在仓库根目录执行，并沿用当前 `summaryd` 的固定路径 watchdog + cron 模型。

先准备变量：

```bash
export REPO_ROOT="$(pwd)"
export PG_CONFIG_BIN=/path/to/pg_config
export PGDATA=/path/to/pgdata
export DB_NAME=postgres
export DB_USER=postgres
export DB_PASSWORD='your_password'
export DB_PORT=5432
export ARCHIVE_DEST=/path/to/archive

export PSQL_BIN="$("${PG_CONFIG_BIN}" --bindir)/psql"
export SUMMARYD_BIN="$("${PG_CONFIG_BIN}" --bindir)/pg_flashback-summaryd"
export CONFIG_PATH="${HOME}/.config/pg_flashback/pg_flashback-summaryd.conf"
export RUNNER_SCRIPT="${REPO_ROOT}/scripts/pg_flashback_summary.sh"
```

其中：

- `ARCHIVE_DEST` 请手工填写能覆盖目标时间窗的 WAL 归档目录
- 如果你就是要按 bootstrap 的兜底口径手工做，可以把它设成 `${PGDATA}/pg_wal`

如果 `PGDATA/pg_flashback` 已存在，先按 bootstrap 的同一口径做受限安全清理，不要直接删整个目录：

```bash
mkdir -p \
  "${PGDATA}/pg_flashback/runtime" \
  "${PGDATA}/pg_flashback/meta/summary" \
  "${PGDATA}/pg_flashback/meta/summaryd"

find "${PGDATA}/pg_flashback/runtime" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
rm -f \
  "${PGDATA}/pg_flashback/meta/summaryd/state.json" \
  "${PGDATA}/pg_flashback/meta/summaryd/debug.json" \
  "${PGDATA}/pg_flashback/meta/summaryd/"*.lock \
  "${PGDATA}/pg_flashback/meta/summary/.tmp."*
```

然后手工执行 build / install：

```bash
make PG_CONFIG="${PG_CONFIG_BIN}" clean
make PG_CONFIG="${PG_CONFIG_BIN}"
make PG_CONFIG="${PG_CONFIG_BIN}" install
```

如果旧环境里仍有：

```sql
shared_preload_libraries = 'pg_flashback'
```

手工安装前还需要把它移除并重启 PostgreSQL。当前 `summaryd`
已不依赖 preload，这一步只用于卸载旧预加载库映像，避免
`CREATE EXTENSION pg_flashback` 命中旧版 `pg_flashback.so`。

手工创建或升级扩展：

```bash
VERSION="$(cat VERSION)"

PGPASSWORD="${DB_PASSWORD}" "${PSQL_BIN}" \
  -v ON_ERROR_STOP=1 \
  -U "${DB_USER}" \
  -p "${DB_PORT}" \
  -d "${DB_NAME}" <<SQL
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
    CREATE EXTENSION pg_flashback;
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'pg_flashback'
      AND extversion <> '${VERSION}'
  ) THEN
    EXECUTE format('ALTER EXTENSION pg_flashback UPDATE TO %L', '${VERSION}');
  END IF;
END;
$$;
SQL
```

手工写数据库默认 `archive_dest`：

```bash
PGPASSWORD="${DB_PASSWORD}" "${PSQL_BIN}" \
  -v ON_ERROR_STOP=1 \
  -U "${DB_USER}" \
  -p "${DB_PORT}" \
  -d "${DB_NAME}" <<SQL
ALTER DATABASE ${DB_NAME} SET pg_flashback.archive_dest = '${ARCHIVE_DEST}';
SQL
```

手工写固定路径的 `summaryd` 配置文件：

```bash
mkdir -p "$(dirname "${CONFIG_PATH}")"
cat > "${CONFIG_PATH}" <<EOF
pgdata=${PGDATA}
archive_dest=${ARCHIVE_DEST}
interval_ms=1000
EOF
```

手工启动 watchdog：

```bash
"${RUNNER_SCRIPT}" start
```

如果你希望得到与一键安装相同的保活效果，再手工安装当前 OS 用户的 `cron`：

```bash
CRON_BLOCK_BEGIN="# BEGIN pg_flashback-summaryd keepalive"
CRON_BLOCK_END="# END pg_flashback-summaryd keepalive"
CRON_LINE="* * * * * ${RUNNER_SCRIPT} start >/dev/null 2>&1"

{
  crontab -l 2>/dev/null | awk -v begin="${CRON_BLOCK_BEGIN}" -v end="${CRON_BLOCK_END}" '
    $0 == begin { skip = 1; next }
    $0 == end { skip = 0; next }
    skip == 0 { print }
  '
  echo "${CRON_BLOCK_BEGIN}"
  echo "${CRON_LINE}"
  echo "${CRON_BLOCK_END}"
} | crontab -
```

手工安装完成后，后续运维入口仍然是：

```bash
scripts/pg_flashback_summary.sh start
scripts/pg_flashback_summary.sh stop
scripts/pg_flashback_summary.sh status
```

#### 安装后验证

先确认 summary 服务状态面已经可见：

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

外部 daemon 观测建议重点看这几列：

- `state_source`: 当前状态来自 `external` / `shmem` / `none`
- `daemon_state_present`: 是否已经发现 external daemon 发布的状态文件
- `daemon_state_stale`: external 状态文件是否已经过期
- `daemon_state_published_at`: external daemon 最近一次发布时间

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

#### One-Command Install

From the repository root, run:

```bash
scripts/b_pg_flashback.sh
```

The script must be run as a non-root OS user. Root execution fails immediately,
and the current OS user must own `PGDATA`.

The script will prompt interactively for:

- the `pg_config` path, defaulting to `which pg_config`
- the `PGDATA` path, defaulting to environment `PGDATA`
- the database name, defaulting to environment `PGDATABASE` or `postgres`
- the database user, defaulting to environment `PGUSER`
- the database password, defaulting to environment `PGPASSWORD` (hidden in the prompt)
- the database port, defaulting to environment `PGPORT` or `5432`

The prompt order is fixed as:

- `pg_config`
- `PGDATA`
- `dbname`
- `dbuser`
- `db-password`
- `db-port`

After setup, it will automatically:

- if `PGDATA/pg_flashback` already exists, run bounded safe cleanup first:
  - clear `runtime/`
  - clear stale `state/debug/lock` files under `meta/summaryd`
  - clear temporary `.tmp.*` files under `meta/summary`
  - preserve `recovered_wal/` and committed summary/meta files
- build and install the extension
- run `CREATE EXTENSION` or `ALTER EXTENSION UPDATE`
- write the `pg_flashback-summaryd` config
- start a shell watchdog through `scripts/pg_flashback_summary.sh`,
  which keeps `pg_flashback-summaryd` alive
- install a per-user cron keepalive that repeatedly runs
  `scripts/pg_flashback_summary.sh start`
- run
  `ALTER DATABASE ... SET pg_flashback.archive_dest = ...`
  for the target database

After installation, the manual runner entrypoints are fixed as:

```bash
scripts/pg_flashback_summary.sh start
scripts/pg_flashback_summary.sh stop
scripts/pg_flashback_summary.sh status
```

Notes:

- the shell runner and cron keepalive both use
  `scripts/pg_flashback_summary.sh` from this checkout, so do not remove the
  repository after installation
- the runner always uses `~/.config/pg_flashback/pg_flashback-summaryd.conf`
  and fixed paths under the same directory:
  - `~/.config/pg_flashback/pg_flashback-summaryd.watchdog.pid`
  - `~/.config/pg_flashback/pg_flashback-summaryd.pid`
  - `~/.config/pg_flashback/pg_flashback-summaryd.log`
- cron only keeps the watchdog present; the watchdog handles 1-second child restarts
- terminal output is now grouped into explicit stages so setup/remove progress
  is easier to read
- interactive inputs are validated immediately when possible:
  `pg_config` must exist and be executable, and `PGDATA` must be an existing directory
- if the target environment is already initialized with the same config,
  rerunning setup returns `already_initialized` and exits without reapplying
- `--remove` does not automatically delete the `PGDATA/pg_flashback`
  data directory; the script prints the retained path and leaves final deletion
  to the operator
- `--remove` also cleans legacy `pg_flashback-summaryd*.service/.conf`
  entries for the same `PGDATA` and removes the installed
  `pg_flashback-summaryd` binary

If you want one-command removal later, run:

```bash
scripts/b_pg_flashback.sh --remove
```

#### Manual Custom Install

This section is the one-command installer split into the exact manual steps.
Run the commands below from the repository root and keep the current fixed-path
watchdog + cron model for `summaryd`.

Prepare the variables first:

```bash
export REPO_ROOT="$(pwd)"
export PG_CONFIG_BIN=/path/to/pg_config
export PGDATA=/path/to/pgdata
export DB_NAME=postgres
export DB_USER=postgres
export DB_PASSWORD='your_password'
export DB_PORT=5432
export ARCHIVE_DEST=/path/to/archive

export PSQL_BIN="$("${PG_CONFIG_BIN}" --bindir)/psql"
export SUMMARYD_BIN="$("${PG_CONFIG_BIN}" --bindir)/pg_flashback-summaryd"
export CONFIG_PATH="${HOME}/.config/pg_flashback/pg_flashback-summaryd.conf"
export RUNNER_SCRIPT="${REPO_ROOT}/scripts/pg_flashback_summary.sh"
```

Notes for the variables:

- set `ARCHIVE_DEST` to the WAL archive directory that covers the target window
- if you want the same fallback semantics as the bootstrap script, you can set
  it to `${PGDATA}/pg_wal`

If `PGDATA/pg_flashback` already exists, do the same bounded safe cleanup first.
Do not delete the whole directory:

```bash
mkdir -p \
  "${PGDATA}/pg_flashback/runtime" \
  "${PGDATA}/pg_flashback/meta/summary" \
  "${PGDATA}/pg_flashback/meta/summaryd"

find "${PGDATA}/pg_flashback/runtime" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
rm -f \
  "${PGDATA}/pg_flashback/meta/summaryd/state.json" \
  "${PGDATA}/pg_flashback/meta/summaryd/debug.json" \
  "${PGDATA}/pg_flashback/meta/summaryd/"*.lock \
  "${PGDATA}/pg_flashback/meta/summary/.tmp."*
```

Build and install manually:

```bash
make PG_CONFIG="${PG_CONFIG_BIN}"
make PG_CONFIG="${PG_CONFIG_BIN}" install
```

Create or upgrade the extension manually:

```bash
VERSION="$(cat VERSION)"

PGPASSWORD="${DB_PASSWORD}" "${PSQL_BIN}" \
  -v ON_ERROR_STOP=1 \
  -U "${DB_USER}" \
  -p "${DB_PORT}" \
  -d "${DB_NAME}" <<SQL
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_flashback') THEN
    CREATE EXTENSION pg_flashback;
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'pg_flashback'
      AND extversion <> '${VERSION}'
  ) THEN
    EXECUTE format('ALTER EXTENSION pg_flashback UPDATE TO %L', '${VERSION}');
  END IF;
END;
$$;
SQL
```

Write the database default `archive_dest` manually:

```bash
PGPASSWORD="${DB_PASSWORD}" "${PSQL_BIN}" \
  -v ON_ERROR_STOP=1 \
  -U "${DB_USER}" \
  -p "${DB_PORT}" \
  -d "${DB_NAME}" <<SQL
ALTER DATABASE ${DB_NAME} SET pg_flashback.archive_dest = '${ARCHIVE_DEST}';
SQL
```

Write the fixed `summaryd` config file:

```bash
mkdir -p "$(dirname "${CONFIG_PATH}")"
cat > "${CONFIG_PATH}" <<EOF
pgdata=${PGDATA}
archive_dest=${ARCHIVE_DEST}
interval_ms=1000
EOF
```

Start the watchdog manually:

```bash
"${RUNNER_SCRIPT}" start
```

If you want the same keepalive semantics as the one-command installer, add the
per-user cron entry manually:

```bash
CRON_BLOCK_BEGIN="# BEGIN pg_flashback-summaryd keepalive"
CRON_BLOCK_END="# END pg_flashback-summaryd keepalive"
CRON_LINE="* * * * * ${RUNNER_SCRIPT} start >/dev/null 2>&1"

{
  crontab -l 2>/dev/null | awk -v begin="${CRON_BLOCK_BEGIN}" -v end="${CRON_BLOCK_END}" '
    $0 == begin { skip = 1; next }
    $0 == end { skip = 0; next }
    skip == 0 { print }
  '
  echo "${CRON_BLOCK_BEGIN}"
  echo "${CRON_LINE}"
  echo "${CRON_BLOCK_END}"
} | crontab -
```

After manual installation, the operational entrypoints are still:

```bash
scripts/pg_flashback_summary.sh start
scripts/pg_flashback_summary.sh stop
scripts/pg_flashback_summary.sh status
```

#### Post-Install Verification

Verify that the summary status surface is visible:

```sql
SELECT *
FROM pg_flashback_summary_progress;
```

When checking the external daemon, look at these columns first:

- `state_source`
- `daemon_state_present`
- `daemon_state_stale`
- `daemon_state_published_at`

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
