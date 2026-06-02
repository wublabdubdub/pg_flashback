# summary daemon 无 preload 化设计

## 背景

当前 `pg_flashback` 的 summary 预建服务依赖：

- `shared_preload_libraries = 'pg_flashback'`
- preload 阶段 addin shmem / named LWLock tranche
- PostgreSQL bgworker launcher / worker

这与“生产库不重启，仅靠扩展安装和独立进程启动即可享受完整 summary 体验”的目标冲突。

同时，当前 summary 的核心价值并不在“它跑在 PostgreSQL backend 里”，而在：

- 后台提前预建 `meta/summary`
- 查询侧继续走 summary-first
- 提供用户可见的进度 / 调试视图
- 对 `meta/summary` 做源 WAL 探活与自动清理

因此本次设计目标不是删掉 summary，而是把“服务进程形态”从 preload/bgwoker 改成库外 daemon。

## 目标

本次设计的最终目标固定为：

- 不依赖 `shared_preload_libraries`
- 不要求重启 PostgreSQL
- 仍保留完整 summary 体验：
  - 后台预热
  - hot/cold backlog 调度
  - `pg_flashback_summary_progress`
  - `pg_flashback_summary_service_debug`
  - `meta/summary` 自动清理
- 保持 `pg_flashback()` 的 query-side summary-first / fallback 语义不变
- 保持项目首版“严格只读”产品边界，不把服务状态落库

## 非目标

- 不把 summary 服务改成 SQL 表驱动
- 不引入第二套外部语言运行时或复杂构建系统
- 不改变现有 summary 文件格式与查询期 reader 语义，除非实现拆分确有必要

补充说明：

- 最终目标仍是 daemon 不依赖数据库连接完成 summary build
- 但本轮落地允许先通过 libpq 复用扩展内 debug helper 完成 build/cleanup，
  以先交付“不重启 PostgreSQL 即可启用完整 summary 体验”的用户能力

## 总体方案

### 1. 模块拆分

将当前 summary 体系拆成三层：

1. `fb_summary_core`
   - 负责 summary 文件格式、WAL segment 扫描、builder、cleanup、状态快照数据结构
   - 不依赖 PostgreSQL preload / bgworker 生命周期
2. `fb_summary_backend`
   - 负责扩展内 query-side summary 读取与 SQL 视图适配
   - 保留 `pg_flashback()` 查询时的 summary-first / fallback 行为
3. `pg_flashback-summaryd`
   - 独立常驻 daemon
   - 承担 launcher / worker / cleanup / state publish

### 2. 运行时边界

当前落地版本中，daemon 已直接读取文件系统和 WAL，
不再要求数据库连接，也不再保留 `--conninfo`。

daemon 的必需输入改为显式配置：

- `--pgdata`
- `--archive-dest`

daemon 仅直接读取：

- `PGDATA/pg_wal`
- `PGDATA/pg_flashback/recovered_wal`
- `PGDATA/pg_flashback/meta/summary`
- archive 目录
- `pg_control` / WAL 文件头中可直接解析的信息

### 3. 状态总线

不再使用 preload shared memory 保存服务状态，改为文件状态总线：

- daemon 写：
  - `DataDir/pg_flashback/meta/summaryd/state.json`
  - `DataDir/pg_flashback/meta/summaryd/debug.json`
- 查询端写：
  - `DataDir/pg_flashback/runtime/summary-hints/*.json`

发布方式固定为：

- 临时文件写满
- `fsync`
- 原子 `rename`

避免 SQL 视图读到半写入状态。

### 4. SQL 可观测面

保留现有对象名：

- `pg_flashback_summary_progress`
- `pg_flashback_summary_service_debug`

但其内部来源从 shared memory 改为状态文件。

语义调整：

- `service_enabled`
  - 从“preload 服务已注册”改为“daemon 心跳在有效期内”
- 其余进度/调试字段尽量维持现有列名与含义
- 无状态文件时，视图不报错，返回“服务未就绪/无快照”

### 5. 查询期回灌

`pg_flashback()` 查询结束时继续上报最近一次 summary 使用情况，但不再写 shared memory，改为写 hint 文件：

- 最近查询时间
- summary ready
- span fallback segments
- metadata fallback segments
- 查询涉及的 timeline / segno 范围

当前实现中，这份 hint 由扩展视图直接读取；后续如果 daemon 需要做聚合，
再追加归并逻辑。

### 6. 调度与清理

调度逻辑沿用现有语义：

- `1` 个 hot worker 跟 recent frontier
- 其余 worker 优先补 cold backlog
- hot / cold queue 分离

清理语义也保持不变：

- 仅清理 `meta/summary`
- 仍按 archive / `pg_wal` / `recovered_wal` 的当前可见集合探活
- 不触碰 `runtime/` / `recovered_wal/` / 其它 query-owned 产物

## 启动与交付

### 1. 构建目标

开源用户 `git clone` 后，统一通过顶层 `Makefile` 构建：

```bash
make PG_CONFIG=/path/to/pg_config
make PG_CONFIG=/path/to/pg_config install
```

默认同时产出：

- `pg_flashback.so`
- SQL/control 安装文件
- `pg_flashback-summaryd`

### 2. 安装位置

- daemon 可执行文件安装到 `$(bindir)`
- service 模板与 sample 配置安装到 `$(sharedir)/pg_flashback/`

### 3. 启动方式

推荐启动方式是 `systemd`，但保留直接命令行启动：

```bash
pg_flashback-summaryd \
  --pgdata /path/to/pgdata \
  --archive-dest /path/to/archive
```

也支持：

```bash
pg_flashback-summaryd \
  --config /path/to/pg_flashback-summaryd.conf \
  --foreground
```

## 兼容迁移

迁移分两阶段：

### Phase A

- 新增 daemon
- 扩展内补状态文件 reader
- query-side summary 继续复用原 reader
- preload 服务保留但逐步降级
- daemon 当前通过 libpq + extension debug helper 驱动 build/cleanup
- `pg_flashback_summary_progress` 的“snapshot 边界锚定”稳定口径
  当前只对 external state 路径启用；shmem/preload 路径仍沿用本地候选集统计

### Phase B

- 去除 summary 服务对 `shared_preload_libraries` 的功能依赖
- `fb_summary_service.c` 收口为 SQL 状态适配层
- 背景 summary 完整能力由 daemon 承担
- 将 daemon 的 build/cleanup 从 libpq helper 继续抽离为纯 frontend-safe core

## 风险与约束

### 1. 共享代码边界

如果 summary builder 仍深度绑定 backend 内存管理和内部 helper，daemon 化将很难维护。

因此必须优先抽出“纯 builder core”。

### 2. 视图一致性

从 shared memory 改为状态文件后，视图变为“最终一致”而非瞬时一致。

这是可接受代价，但必须在文档中明确。

### 3. 实例归属

同一台机器上可存在多个 PostgreSQL 实例，因此 daemon 必须以 `PGDATA` 为实例唯一锚点，并在
`DataDir/pg_flashback/meta/summaryd/lock` 上持有单实例锁。

## 验收标准

满足以下条件才视为本设计完成：

- 不修改 `shared_preload_libraries` 也可启用完整 summary 体验
- PostgreSQL 无需重启即可安装扩展并启动 daemon
- `pg_flashback_summary_progress` / `pg_flashback_summary_service_debug` 仍可用
- daemon 构建与安装并入顶层 `make` / `make install`
- 开源 README 能直接指导用户从源码构建并启动 daemon
