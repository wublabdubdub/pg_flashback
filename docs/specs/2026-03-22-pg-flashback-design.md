# pg_flashback 设计规格

## 产品目标

`pg_flashback` 提供类似 Oracle Flashback Query 的历史查询与原表闪回能力：

- 输入当前表的复合类型锚点与目标时间点
- 从 WAL 重建 `target_ts` 时刻的逻辑结果
- 直接返回结果集
- 或直接将 keyed 原表回退到目标时间点

当前用户入口固定为：

```sql
SELECT pg_flashback_to('public.t1'::regclass, '2026-03-22 10:00:00+08');
```

```sql
SELECT *
FROM pg_flashback(
  NULL::public.t1,
  '2026-03-22 10:00:00+08'
);
```

## 已确认约束

- 同时支持历史结果查询与 undo SQL / reverse op 导出
- 支持 `pg_flashback_to(regclass, text)` 直接修改 keyed 原表
- 无主键表按 `bag/multiset` 语义处理
- `target_ts -> query_now_ts` 期间 WAL 必须完整
- 时间窗内若检测到 DDL / rewrite / truncate / relfilenode 变化，直接报错
- TOAST 首版支持，但重建失败直接报错
- 开发文档用中文，代码标识符用英文
- 代码与内部标识符统一前缀 `fb`
- 公开 SQL 主入口保留为 `pg_flashback(...)`

## 当前接口面

当前安装 SQL 只对外安装：

- `fb_version()`
- `fb_check_relation(regclass)`
- `pg_flashback_to(regclass, text)`
- `pg_flashback(anyelement, text)`

当前未对外安装：

- `fb_export_undo(regclass, timestamptz)`
- `fb_internal_flashback(...)`
- `fb_flashback_materialize(...)`
- 旧的 scan / replay / decode 调试 SQL 入口

## 为什么采用 `anyelement`

PostgreSQL 无法对通用 `SETOF record` 自动推断“任意表”的返回列定义；如果直接公开 `SETOF record`，调用侧通常必须写：

```sql
SELECT *
FROM pg_flashback(...)
AS t(col1 type1, col2 type2, ...);
```

这在大宽表上不可接受。

因此当前接口固定为：

```sql
SELECT *
FROM pg_flashback(NULL::schema.table, target_ts_text);
```

这里的 `NULL::schema.table` 是“表的复合类型锚点”，用于让 PostgreSQL 在执行前就确定返回行类型。

## 总体架构

主链固定为：

1. relation gate
2. WAL 时间窗扫描与 `RecordRef` 索引
3. `checkpoint + FPI + block redo`
4. 从页级重放提取 `ForwardOp`
5. `ForwardOp -> ReverseOp`
6. keyed / bag 流式 apply
7. 逐行返回结果集

实现上仍以 `reverse op stream` 作为查询/导出逻辑层，但它的来源已经固定为页级重放内核，而不是“直接从 WAL 行像拼 old row”。

## 查询执行流程

一次 `pg_flashback()` 查询的当前实现流程为：

1. 从第一个参数解析目标 relation row type
2. 解析第二个参数 `target_ts`
3. 进行 relation / runtime / archive gate
4. 建立 WAL 扫描上下文
5. 扫描 WAL 并构建目标 relation 的 `RecordRef` 索引
6. 根据 `checkpoint + FPI + block redo` 重建相关历史页状态
7. 生成 `ForwardOp`
8. 构建按时间逆序的 `ReverseOp`
9. 按 relation 模式执行流式 apply，并逐行返回

## 输出模型

当前输出模型已经固定为两种：

- `pg_flashback(anyelement, text)`：
  - 不创建结果表
  - 不走 `tuplestore`
  - 查询结束后即结束，不保留最终结果副本
- `pg_flashback_to(regclass, text)`：
  - 直接修改 keyed 原表
  - 执行时拿 `AccessExclusiveLock`
  - 当前只支持单列稳定键
  - 检测到外键与用户触发器直接报错

## 内存模型

当前内存设计分两层理解：

### apply 层

- keyed：只维护“变化 key 集”
- bag：只维护“变化 row identity 集”
- 不再因当前表大小而构造整表工作集

### 整体查询链路

- 仍存在 `RecordRef` / FPI / block data / replay state 等 query 级内存消耗
- 这些结构受 `pg_flashback.memory_limit_kb` 约束
- 当前尚未把 WAL 索引 / replay 主链全部改为 spill 模型
- 因此当前“小内存”首先保证的是 apply 不按整表大小线性膨胀，而不是“整条链路任何阶段都只占极小内存”

## keyed / bag 语义

### keyed

- 适用于存在主键或稳定唯一键的表
- 只跟踪时间窗内发生变化的 key
- 扫描当前表时：
  - 未命中变化 key 的行直接返回
  - 命中变化 key 的行按反向动作链计算历史行
- 扫描结束后补发“当前已不存在、但历史上存在”的 residual 行

### bag

- 适用于无主键表
- 使用 `row_identity -> delta_count` 模型
- 扫描当前表时吞掉需要抵消的当前行
- 扫描结束后按剩余正 delta 补发行

## 运行时前提

- 首选 WAL 来源为 `pg_flashback.archive_dest`
- `pg_flashback.archive_dir` 仅保留为兼容回退配置
- `pg_wal` 只补 archive 尚未覆盖的 recent tail
- 发生 `pg_wal` 错配时，转入扩展内部 `recovered_wal/`
- 扩展自动维护 `DataDir/pg_flashback/{runtime,recovered_wal,meta}`
- 扩展会按目录类型执行保洁：
  - `runtime/` 清理死 backend 遗留 `fbspill-*` 目录，并对超保留期残留做兜底淘汰
  - `recovered_wal/` 按 `recovered_wal_retention` 淘汰旧恢复段
  - `meta/` 按 `meta_retention` 淘汰旧 sidecar

## 不支持场景

以下情况直接报错，不返回部分结果：

- WAL 不完整
- 时间窗内检测到 DDL / rewrite / truncate / storage change
- relation 类型不支持
- TOAST 历史值无法重建
- 超出查询级 `memory_limit_kb`

## 当前仍在推进

- `fb_export_undo`
- batch B / residual `missing FPI` 收敛
- 更多 PG18 heap WAL 正确性补齐
- WAL 索引 / replay 主链继续向 bounded spill 演进
