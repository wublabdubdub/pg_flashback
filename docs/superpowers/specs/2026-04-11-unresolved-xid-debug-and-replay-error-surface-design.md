# unresolved xid 调试接口与 replay 报错面设计

## 背景

当前 `3/9 build record index` 在少量 `unresolved xid` 场景下仍可能明显放大回退耗时。
仓库内已有未公开安装的：

- `fb_summary_xid_resolution_debug(regclass, timestamptz)`
- `fb_summary_xid_probe_debug(regclass, timestamptz, bigint)`

它们能帮助研发现场分析，但当前对最终用户有两个缺口：

1. 缺少一个公开、结构化、稳定的内置函数，直接列出“哪些 xid 导致本次 flashback 仍需 fallback / unresolved”
2. replay 失败报错虽然已有局部页级细节，但仍缺少统一、稳定、便于离线排查的诊断键值

本轮目标是让用户只靠：

- 用户传回的 `pg_flashback_debug_unresolv_xid(...)` 结果
- PostgreSQL 错误日志/客户端报错
- 对应 WAL 文件

即可在不连接原数据库实例的前提下做第一轮问题排查。

## 用户接口

新增公开安装函数：

```sql
SELECT *
FROM pg_flashback_debug_unresolv_xid(
  'public.t1'::regclass,
  '2026-04-11 10:00:00+08'
);
```

函数签名固定为：

- `pg_flashback_debug_unresolv_xid(regclass, timestamptz)`

命名沿用用户指定的 `unresolv` 拼写，不在首版中额外引入别名。

## 返回模型

函数返回结构化 `TABLE`，每行对应一个本轮需要重点排查的 xid。

建议列固定为：

- `xid bigint`
- `xid_role text`
  - `touched`
  - `unsafe`
- `resolved_by text`
  - `summary`
  - `exact_window`
  - `exact_all`
  - `snapshot_clog`
  - `raw_wal`
  - `unresolved`
- `fallback_reason text`
  - `summary_missing_outcome`
  - `summary_missing_assignment`
  - `summary_missing_top_outcome`
  - `summary_missing_segment`
  - `raw_xact_fallback`
  - `unsafe_xid`
- `top_xid bigint`
- `commit_ts timestamptz`
- `summary_missing_segments integer`
- `fallback_windows integer`
- `diag text`

其中：

- `diag` 必须是稳定的 `key=value` 风格摘要，便于日志贴回分析
- 首版不承诺枚举所有内部状态，但必须能支撑“为什么这个 xid 仍然拖慢或阻塞 flashback”这个问题

## 实现路线

### 1. 复用现有 xid probe 基础

`fb_wal.c` 已有：

- 元数据扫描
- summary-first xid outcome 填充
- exact window / exact all 补洞
- unresolved xid fallback 集构建
- 单 xid probe 能力

本轮不再新造一条扫描主链，而是在现有基础上新增一个“逐 unresolved xid 发射结果行”的 SQL 入口。

### 2. 结构化而不是 text 拼接

现有 `fb_summary_xid_resolution_debug(...)` 返回 `text` 摘要，只适合快速 smoke。

新公开函数改为 `RETURNS TABLE`，原因：

- 用户截图/复制结果时不易漂义
- 后续新增字段时兼容性更可控
- 便于直接按 `xid` 逐条追 probe

### 3. replay 报错补统一诊断上下文

当前 `heap insert/update/multi insert` 等报错已带部分页状态细节，但字段分布不完全一致。

本轮补一个统一的 replay error detail 追加层，至少保证失败报错中稳定出现：

- `phase`
- `recidx`
- `record_kind`
- `xid`
- `lsn`
- `end_lsn`
- `rel`
- `fork`
- `blk`
- `off`
- `toast`
- `has_image`
- `apply_image`
- `has_data`
- `init_page`
- `page_lsn`

具体页内碎片信息如 `maxoff/lower/upper/exact_free/heap_free/lp...` 继续按各 record 类型保留。

### 4. 对外版本化

由于新增公开 SQL 接口，本轮必须：

- 前滚扩展版本
- 新增安装脚本
- 新增升级脚本
- 更新 README/用户界面相关回归

## 错误处理

### debug 函数

- 目标时间点在未来：继续报错
- relation 不支持：继续走现有 relation gate
- 若本轮没有 unresolved/fallback xid：返回零行，不伪造占位结果

### replay 报错

- 不改变 correctness
- 不改变错误等级
- 仅补稳定诊断字段，避免“看起来信息更多，但仍无法复盘 record 身份”

## 回归

至少覆盖：

1. 新函数已对外安装
2. 新函数在存在 unresolved xid 的 fixture 上能返回结构化行
3. 新函数在无 unresolved xid 场景返回空集
4. 升级链后函数可见
5. replay 错误面新增稳定字段：
   - `record_kind=...`
   - `xid=...`
   - `end_lsn=...`

## 非目标

- 不在本轮引入自动落盘的 query debug artifact
- 不改变 unresolved xid 的正确性判定
- 不把 debug 函数扩展成“直接自动分析 root cause”
