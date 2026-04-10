# ADR-0035: summary 服务外移为库外 daemon，去除 preload 依赖

## 状态

Accepted

## 背景

当前 summary 服务依赖：

- `shared_preload_libraries`
- preload 阶段 addin shmem
- PostgreSQL bgworker launcher / worker

这与“不重启数据库即可获得完整 summary 体验”的上线目标冲突。

同时，目标中的 summary build 语义已经固定为：

- 理想终态下 worker 不连接数据库
- 不访问 catalog
- 不把任务状态落库

因此，真正需要保留的是 summary 的后台服务语义，而不是 PostgreSQL 内置 bgworker 这一具体承载形式。

## 决策

将 summary 服务从 preload/bgwoker 模式迁移到库外常驻 daemon。

当前落地分两层：

- 新增独立进程 `pg_flashback-summaryd`
- daemon 的 build/cleanup 已下沉为真正不依赖数据库连接的
  frontend-safe / standalone core
- `--conninfo` 不再作为接口保留
- daemon 负责：
  - archive / `pg_wal` / `recovered_wal` 扫描
  - hot / cold 调度
  - summary build
  - `meta/summary` 自动清理
  - 状态快照发布
- daemon 生命周期当前固定由脚本管理，而不是 systemd/service 注册：
  - 推荐入口收口为 `scripts/pg_flashback_summary.sh`：
    - `start`
    - `stop`
    - `status`
    - `run-once`
  - `run-once` 语义固定为：
    - 扫描一次 `archive_dest + pg_wal + recovered_wal`
    - 完成一轮 summary build / state publish / cleanup
    - 然后退出
- 扩展内保留：
  - query-side summary reader
  - `pg_flashback()` 的 summary-first / fallback
  - SQL 视图
- 服务状态不再放 shared memory，改为状态文件：
  - `meta/summaryd/state.json`
  - `meta/summaryd/debug.json`
- 查询侧最近一次 summary 命中/降级观测改写到 runtime hint 文件，
  当前由扩展视图直接读取，不要求 daemon 二次汇总

## 后果

优点：

- 不再依赖 `shared_preload_libraries`
- 不要求 PostgreSQL 重启
- 不占用 PostgreSQL bgworker 配额
- 仍保留完整 summary 用户体验
- 当前已经满足“不落库”与“默认不连接数据库”

代价：

- 服务状态从 shared memory 改为文件快照，进度视图变为最终一致
- 需要维护脚本 runner、daemon 锁文件与状态文件兼容性
- 需要将现有 summary builder 继续抽离出 backend 专属依赖

## 语义要求

- daemon 最终必须只依赖：
  - `PGDATA`
  - `archive_dest`
  - `pg_wal`
  - `recovered_wal`
  - `meta/summary`
- README 与开源 README 必须明确写清：
  - 如何编译
  - 编译产物落点
  - `PGDATA/pg_flashback/` 目录和文件何时生成
  - daemon 的脚本启动方式
- `PGDATA` 是实例唯一锚点
- 从源码构建时，daemon 必须由顶层 `Makefile` 一并生成与安装
- `pg_flashback_summary_progress` / `pg_flashback_summary_service_debug` 对外对象名保持不变
- `meta/summary` 自动清理语义保持不变，仅迁移执行主体
- `pg_flashback_summary_progress` 的 snapshot 边界锚定稳定口径，
  当前只对 external state 路径生效；shmem/preload 路径继续按本地候选集统计
