# pg_flashback Summary 预建服务设计

## 目标

为 `pg_flashback` 增加一套常驻的 summary 预建服务，尽量在用户发起 flashback 查询前完成 WAL segment 摘要构建，并把 metadata 文件尺寸控制在可持续范围内。

本次设计固定目标：

- 不侵入 PostgreSQL WAL 写入/提交热路径
- 允许影响查询侧 prefilter 路径
- 预建服务优先使用扩展自己的 shared memory 与 `DataDir/pg_flashback/meta` 文件
- summary 首版先服务于 WAL segment prefilter
- `meta` 在 summary 持续积累后需要自动清理

## 非目标

- 不把 summary 构建挂到 `XLogInsert`、walwriter、checkpointer、archiver 热路径
- 不把任务状态或 summary 元数据落到用户表或 shared buffers
- 不要求 worker 连接数据库或访问 catalog
- 不在首版实现 chunk 级 summary
- 不在首版实现 query usage 统计驱动的清理优先级

## 总体方案

新增一套“旁路预建服务”：

1. `_PG_init()` 在 `shared_preload_libraries` 模式下注册一个常驻 launcher，并初始化扩展私有 shared memory。
2. launcher 周期扫描 `archive_dest`、`pg_wal` 与 `recovered_wal`，把“缺 summary 或 summary 失效”的 WAL segment 投递到扩展私有共享队列。
3. 多个无数据库连接的 bgworker 从共享队列取任务，读取 WAL 文件，在进程私有内存中构建极小 summary，再原子写入 `DataDir/pg_flashback/meta/summary`。
4. 查询侧 prefilter 优先读取 segment summary；缺失时回退现有 `mmap + memmem` 路径，不让预建服务成为 flashback 查询的可用性前提。
5. launcher 每轮扫描后检查 summary 目录占用；超出高水位后，只删除低价值、可再生的旧 summary，清到低水位停止。

## 为什么这样做

当前冷态 prefilter 的成本主要来自“按查询扫描原始 WAL 正文”。这类成本在第一次查询前无法凭空消失，因此本次设计不试图让“查询现场临时建 summary”本身显著更快，而是把工作前移为：

- postmaster 启动后尽早后台介入
- 新 WAL 到达后尽早构建
- 查询现场尽量只读小 summary 文件

这样既满足“尽量早介入、尽量快完成”的目标，也把对 PG 内核写入路径的影响降到最低。

## 模块划分

### `fb_summary`

新增文件：

- `src/fb_summary.c`
- `include/fb_summary.h`

职责：

- 定义 summary 文件格式
- 读写 `meta/summary/*.meta`
- 用 WAL record 轻量扫描构建 summary
- 为查询 prefilter 提供 summary probe API

### `fb_summary_service`

新增文件：

- `src/fb_summary_service.c`
- `include/fb_summary_service.h`

职责：

- 定义共享队列与共享状态
- 注册 launcher / worker
- 目录扫描、任务投递、任务去重
- 周期唤醒与清理调度

### 现有模块改动

- `src/fb_guc.c`
  - 新增 summary 服务相关 GUC
  - 在 `shared_preload_libraries` 场景注册 shared memory / launcher
- `src/fb_runtime.c`
  - 新增 `meta/summary` 与相关服务目录
- `src/fb_wal.c`
  - prefilter 改为优先使用 segment summary
  - 旧 `prefilter-*.meta` relation-pattern sidecar 停用
- `Makefile`
  - 新增对象文件

## Summary 文件格式

首版 summary 使用“极小、通用、只服务 prefilter”的格式：

- header
  - magic
  - version
  - timeline id
  - segno
  - source kind
  - file identity hash
  - wal_seg_size
  - build timestamp
- flags
  - 是否见到 checkpoint
  - 是否见到 heap / heap2 / xlog_fpi / smgr / standby / xact
- `locator_bloom`
  - 记录 block tag / smgr create / smgr truncate 等基于 `RelFileLocator` 的 relation 痕迹
- `relid_bloom`
  - 记录 `dbOid + relOid` 形式的 relation 痕迹，用于 `HEAP_TRUNCATE` / `HEAP2_REWRITE` / `STANDBY_LOCK`

为什么需要两个 bloom：

- 仅靠 `locator` 会漏掉只带 `relid` 的 unsafe record
- 查询侧当前既有 `info->locator/toast_locator`，也有 `info->relid/toast_relid`
- 允许 false positive，不能 false negative

首版不做：

- exact locator set
- chunk 级 summary
- 事务时间窗索引
- payload 或 block image 摘要

## 查询侧 prefilter 语义

查询侧 `fb_prepare_segment_prefilter()` 改成：

1. 先尝试读取 segment summary
2. 若 summary 有效，则：
   - 用 `main locator` / `toast locator` probe `locator_bloom`
   - 用 `{dbOid, relid}` / `{dbOid, toast_relid}` probe `relid_bloom`
   - 任一命中即视为“可能相关”
3. 若 summary 缺失或无效：
   - 回退旧的 `mmap + memmem` 路径
   - 不再生成 relation-pattern 级 `prefilter-*.meta`

这样可以：

- 让预建 summary 成为主缓存
- 避免旧 `prefilter-*.meta` 文件继续爆炸增长
- 保留查询可用性兜底

## 共享队列与 worker pool

共享队列首版采用固定大小任务槽数组，不做复杂通用 job system。

每个任务槽包含：

- `timeline_id`
- `segno`
- `source_kind`
- `path`
- `file_identity_hash`
- `priority`
- `state`
  - `EMPTY`
  - `PENDING`
  - `RUNNING`
  - `DONE`
  - `FAILED`
- `attempt_count`
- `lease_deadline`
- 最近错误码/消息摘要

worker 只读取 WAL 文件并写 summary，不连接数据库。队列和 worker 的设计要求：

- 固定 shared memory 上限
- 不在 shared memory 中保存大对象
- worker 崩溃后任务可回收
- 重启后任务丢失也无妨，launcher 下一轮重扫可恢复

## 扫描规则

launcher 每轮采用“双前沿”策略：

### 热前沿

优先处理：

- `pg_wal`
- archive 的 recent tail
- `recovered_wal`

目标是让最近产生的 WAL 尽快拥有 summary，减少用户查 recent window 时的冷 miss。

### 冷回填

每轮在剩余预算内回填较老 archive 段，逐步追平历史 backlog。

### 选择规则

同一 `timeline + segno` 若 archive 与 `pg_wal` 同时存在，服务与查询 resolver 保持同口径：

- archive 优先
- `pg_wal` 只补 recent tail

`pg_wal` mismatch/recycled 段不作为首版服务主路径输入；查询侧若通过 `fb_ckwal` 恢复出 `recovered_wal`，后续扫描轮可为恢复段补建 summary。

## 自动清理

当前 ADR-0018 已取消对整个 `meta` 的通用自动 cleanup。本次设计只恢复“summary 专属清理”，不恢复旧 runtime 清理口径。

清理策略：

- 新增 summary 专属容量上限 GUC
- 使用 `high watermark / low watermark`
- 仅当 `meta/summary` 超过高水位时清理
- 删除顺序优先：
  - 最旧
  - 非 recent tail
  - 对应 WAL 文件已不在 `archive_dest` / `pg_wal` / `recovered_wal`
  - 当前不在队列运行中
- 清到低水位即停止

明确不删：

- checkpoint sidecar
- query runtime/spool
- `recovered_wal`
- 当前活跃 worker 正在生成的 summary

## GUC 规划

新增：

- `pg_flashback.summary_service`
  - `on/off`
  - 控制 launcher/worker 是否工作
- `pg_flashback.summary_workers`
  - 默认 `4`
  - 上限建议 `8`
- `pg_flashback.summary_scan_interval`
  - 控制周期扫描间隔
- `pg_flashback.summary_meta_limit`
  - summary 目录总容量上限
- `pg_flashback.summary_meta_low_watermark_percent`
  - 高水位触发后的清理停点

当前保留：

- `pg_flashback.parallel_workers`
  - 不直接复用为 summary worker 数，避免与 flashback 查询并行开关耦合

## 对 PostgreSQL 内核写入路径的影响控制

本设计固定遵守：

- 不在事务提交或 WAL insert 路径同步做 summary
- 不写 shared buffers
- 不写 catalog/user tables
- worker 不开事务、不拿 snapshot、不访问 relcache/catalog
- 所有 summary 状态落扩展私有 shared memory 与 `meta/summary` 文件

这样即使预建服务失效：

- 只会让查询回退到旧 prefilter 路径
- 不会影响正常写入、WAL 生成或事务提交语义

## 测试策略

### 单元/回归

- summary 文件读写
- summary probe 对 main/toast locator 的命中
- `HEAP_TRUNCATE` / `HEAP2_REWRITE` / `STANDBY_LOCK` 的 `relid_bloom` 命中
- query prefilter 优先走 summary、缺失时回退旧路径
- `prefilter-*.meta` 不再持续生成
- summary 清理水位行为

### 集成

- 配置 `shared_preload_libraries`
- 启动 launcher + worker
- 构造 archive / `pg_wal` backlog
- 等待 summary 预建完成
- 确认 flashback 查询 cold start 不再依赖 relation-pattern sidecar

## 风险

- 静态 bgworker + AddinShmem 引入新的 postmaster 生命周期复杂度
- summary 若漏记 unsafe-only record，会造成错误跳过 segment，因此 bloom 填充逻辑必须覆盖所有当前 relation gate 关心的 record 类型
- 自动清理若删到 recent tail 或活跃任务文件，会引入抖动；需要队列状态与路径保护

## 实施顺序

1. 文档与 ADR
2. summary 文件格式与 query probe
3. 查询 prefilter 切换到 summary
4. 共享内存、launcher、worker
5. 周期扫描与任务投递
6. summary 自动清理
7. 回归与手工集成验证
